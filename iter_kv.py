import json, math, re
from typing import Any, Iterable, Dict, List, Optional, Set
import pandas as pd
import numpy as np

def _iter_kv(obj: Any, prefix: str = "") -> Iterable[tuple[str, Any]]:
    """Yield (path, value) pairs from dict/list/scalar, with dot paths."""
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

def _norm_value(v: Any) -> tuple[str, str]:
    """
    Return (value_text, value_type).
    - Always produce a string for value_text (so visuals are easy).
    - value_type is 'num'|'bool'|'text' for optional analytics.
    """
    if isinstance(v, bool):
        return ("true" if v else "false", "bool")
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return ("", "text")
        # Keep canonical string, but mark as numeric
        return (str(v), "num")
    # timestamps, None, objects -> stringify safely
    return (str(v) if v is not None else "", "text")

def _should_keep(path: str, whitelist: Optional[Set[str]]) -> bool:
    if not whitelist:
        return True
    for w in whitelist:
        if path == w or path.startswith(w + ".") or path.startswith(w + "["):
            return True
    return False

def explode_event_meta_long_simple(
    df: pd.DataFrame,
    incident_col: str = "INCIDENT_ID",
    time_col: str = "FOLLOWUP_DATETIME",
    user_col: str = "SYSTEM_OPID",
    phase_col: str = "_phase",
    meta_col: str = "event_meta",
    tag_col: Optional[str] = "tag",                 # <- your existing tag column if you have one
    whitelist: Optional[Iterable[str]] = None,      # keep None to take everything
) -> pd.DataFrame:
    """
    Build a long, simple KV table with Tag retained and a single value column:
      [incident_id, event_ts, user_id, _phase, tag, key, value_text, value_type]
    """
    cols = [c for c in [incident_col, time_col, user_col, phase_col, meta_col, tag_col] if c and c in df.columns]
    out_rows: List[Dict[str, Any]] = []
    wl = set(whitelist) if whitelist is not None else None

    for row in df[cols].itertuples(index=False, name=None):
        mapping = dict(zip(cols, row))
        meta = mapping.get(meta_col, None)

        # Determine tag: prefer explicit tag column; else try from meta (cat/kind); else ""
        tag = ""
        if tag_col and tag_col in mapping and mapping[tag_col] is not None:
            tag = str(mapping[tag_col])
        # Parse meta
        parsed = None
        if isinstance(meta, (dict, list)):
            parsed = meta
        elif isinstance(meta, str):
            s = meta.strip()
            if s and s.lower() != "none":
                try:
                    parsed = json.loads(s)
                except Exception:
                    parsed = None

        if parsed is None:
            # Still emit a row for tag itself if you want—usually we skip
            continue

        # If no tag yet, try derive from meta root keys
        if not tag:
            if isinstance(parsed, dict):
                tag = str(parsed.get("tag") or parsed.get("cat") or parsed.get("kind") or "")
            else:
                tag = ""

        for path, val in _iter_kv(parsed):
            if not path or not _should_keep(path, wl):
                continue
            value_text, value_type = _norm_value(val)
            out_rows.append({
                "incident_id": mapping.get(incident_col),
                "event_ts": mapping.get(time_col),
                "user_id": mapping.get(user_col),
                "_phase": mapping.get(phase_col),
                "tag": tag,
                "key": path,
                "value_text": value_text,
                "value_type": value_type,
            })

    kv = pd.DataFrame.from_records(out_rows)
    if kv.empty:
        return kv

    # Friendly dtypes for Parquet/Power BI
    kv["event_ts"] = pd.to_datetime(kv["event_ts"], errors="coerce")
    kv["incident_id"] = pd.to_numeric(kv["incident_id"], errors="coerce").astype("Int64")
    kv["_phase"] = kv["_phase"].astype("string")
    kv["tag"] = kv["tag"].astype("string")
    kv["key"] = kv["key"].astype("string")
    kv["value_text"] = kv["value_text"].astype("string")
    kv["value_type"] = kv["value_type"].astype("string")
    return kv
