import json
import math
from typing import Iterable, Dict, Any, Optional, Set, List
import pandas as pd
import numpy as np

# --------- helpers ---------

def _iter_kv(obj: Any, prefix: str = "") -> Iterable[tuple]:
    """Yield (path, value) pairs for any JSON-like object (dict/list/scalars)."""
    if obj is None:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{prefix}.{k}" if prefix else str(k)
            yield from _iter_kv(v, p)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            p = f"{prefix}[{i}]" if prefix else f"[{i}]"
            yield from _iter_kv(v, p)
    else:
        yield prefix, obj

def _split_value(v: Any):
    """Return (value_num, value_bool, value_text). One of the three is set, others None."""
    # Booleans must be checked before int (since bool is a subclass of int)
    if isinstance(v, bool):
        return (None, bool(v), None)
    # Numeric
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        # Normalize NaN/inf to None
        if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
            return (None, None, None)
        return (float(v), None, None)
    # Timestamps can sneak in as pandas Timestamp; stringify them
    return (None, None, str(v))

def _should_keep(path: str, whitelist: Optional[Set[str]]) -> bool:
    """
    Keep if no whitelist is provided, or if the path starts with any whitelist key.
    Example: whitelist={'code', 'operation'} keeps 'code.cause.old', 'operation', etc.
    """
    if not whitelist:
        return True
    for w in whitelist:
        if path == w or path.startswith(w + ".") or path.startswith(w + "["):
            return True
    return False

# --------- main exploder ---------

def explode_event_meta_long(
    df: pd.DataFrame,
    incident_col: str = "INCIDENT_ID",
    time_col: str = "FOLLOWUP_DATETIME",
    user_col: str = "SYSTEM_OPID",
    phase_col: str = "_phase",
    meta_col: str = "event_meta",
    whitelist: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    Return a long table: one row per (event × meta key).
    Columns: incident_id, event_ts, user, _phase, key, value_num, value_bool, value_text
    """
    keep = [c for c in [incident_col, time_col, user_col, phase_col, meta_col] if c in df.columns]
    if meta_col not in keep:
        raise ValueError(f"{meta_col} not found in DataFrame")

    w = set(whitelist) if whitelist is not None else None

    records: List[Dict[str, Any]] = []
    # Use itertuples for speed and lower memory overhead
    for row in df[keep].itertuples(index=False, name=None):
        # Unpack in the same order we built `keep`
        mapping = dict(zip(keep, row))
        meta = mapping.get(meta_col, None)

        # Parse meta: accept dict/list; if string, try json; else skip
        parsed = None
        if isinstance(meta, (dict, list)):
            parsed = meta
        elif isinstance(meta, str):
            meta_str = meta.strip()
            if meta_str and meta_str != "None":
                try:
                    parsed = json.loads(meta_str)
                except Exception:
                    # skip unparseable payloads
                    parsed = None

        if parsed is None:
            continue

        for path, val in _iter_kv(parsed):
            if not path:  # safety
                continue
            if not _should_keep(path, w):
                continue

            v_num, v_bool, v_text = _split_value(val)
            rec = {
                "incident_id": mapping.get(incident_col),
                "event_ts": mapping.get(time_col),
                "user_id": mapping.get(user_col),
                "_phase": mapping.get(phase_col),
                "key": path,
                "value_num": v_num,
                "value_bool": v_bool,
                "value_text": v_text,
            }
            records.append(rec)

    out = pd.DataFrame.from_records(records)
    # Dtypes for Parquet friendliness
    if not out.empty:
        out["event_ts"] = pd.to_datetime(out["event_ts"], errors="coerce")
        out["incident_id"] = pd.to_numeric(out["incident_id"], errors="coerce").astype("Int64")
        out["value_num"] = pd.to_numeric(out["value_num"], errors="coerce")
        out["value_bool"] = out["value_bool"].astype("boolean")
        out["value_text"] = out["value_text"].astype("string")
        out["user_id"] = out["user_id"].astype("string")
        out["_phase"] = out["_phase"].astype("string")
        out["key"] = out["key"].astype("string")
    return out
