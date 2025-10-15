import re, json
import pandas as pd
from dataclasses import dataclass
from typing import Callable, Dict, Optional, Pattern, Any, List

DESC_COL = "FOLLOWUP_DESC"

# ---------- Shared ----------
DT = r'(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}(?::\d{2})?)'
def coerce_dt(s: Optional[str]):
    return pd.to_datetime(s, errors="coerce") if s else None

@dataclass(frozen=True)
class Rule:
    name: str
    priority: int
    detect: Pattern
    extract: Optional[Pattern] = None
    handler: Optional[Callable[[re.Match], Dict[str, Any]]] = None  # returns event_meta dict

def apply_rules(text: str, rules: List[Rule]) -> Dict[str, Any]:
    s = (text or "").strip()
    for rule in sorted(rules, key=lambda r: r.priority):
        if rule.detect.search(s):
            flags = None
            event_meta: Dict[str, Any] = {}
            if rule.extract:
                m = rule.extract.search(s)
                if not m:
                    return {"Tag": rule.name, "Flags": "PARSE_FAIL", "event_meta": {}}
                if rule.handler:
                    event_meta = rule.handler(m)
                    # Handler can set its own flags inside meta; bubble up if present
                    flags = event_meta.pop("_flags", None)
            return {"Tag": rule.name, "Flags": flags, "event_meta": event_meta}
    return {"Tag": None, "Flags": None, "event_meta": {}}

# ---------- ETR rules ----------
SYSTEM_DETECT = re.compile(r'(?i)^\s*SYSTEM\s+ETR\b')
SYSTEM_EXTRACT = re.compile(
    rf'''(?ix)
    ^\s*SYSTEM\s+ETR-?\s*Set\s+ETR\s+for\s+@\s*(?P<loc>.+?)\s+
    To\s+(?:(?P<to_type>SYS)\s+ETR|ETR\s+(?P<to_type_alt>SYS))\s*[:\-]?\s*
    (?P<to_dt>{DT})\s*$
    '''
)
def sys_handler(m: re.Match) -> Dict[str, Any]:
    to_type = (m.group("to_type") or m.group("to_type_alt") or "SYS").upper()
    return {
        "cat": "ETR",
        "kind": "SYSTEM",
        "loc": m.group("loc"),
        "etr_from_type": None,
        "etr_from_ts": None,
        "etr_to_type": to_type,
        "etr_to_ts": coerce_dt(m.group("to_dt")),
    }

MAN_DETECT = re.compile(r'(?i)^\s*MANUAL\s+ETR\b')
MAN_EXTRACT = re.compile(
    rf'''(?ix)
    ^\s*MANUAL\s+ETR-?\s*Set\s+ETR\s+for\s+@\s*(?P<loc>.+?)\s+
    From\s+ETR\s+(?P<from_type>MAN|SYS)\s*[:\-]?\s*(?P<from_dt>{DT})\s+
    To\s+(?:(?P<to_type>MAN)\s+ETR|ETR\s+(?P<to_type_alt>MAN))\s*[:\-]?\s*(?P<to_dt>{DT})\s*$
    '''
)
def man_handler(m: re.Match) -> Dict[str, Any]:
    from_type = (m.group("from_type") or "").upper()
    to_type = (m.group("to_type") or m.group("to_type_alt") or "MAN").upper()
    from_dt = coerce_dt(m.group("from_dt"))
    to_dt = coerce_dt(m.group("to_dt"))
    flags = None
    if pd.notna(from_dt) and pd.notna(to_dt) and to_dt < from_dt:
        flags = "TO_BEFORE_FROM"
    return {
        "cat": "ETR",
        "kind": "MANUAL",
        "loc": m.group("loc"),
        "etr_from_type": from_type,
        "etr_from_ts": from_dt,
        "etr_to_type": to_type,
        "etr_to_ts": to_dt,
        "_flags": flags,   # bubble up to Flags
    }

rules: List[Rule] = [
    Rule("SYSTEM ETR", 10, SYSTEM_DETECT, SYSTEM_EXTRACT, sys_handler),
    Rule("MANUAL ETR", 20, MAN_DETECT, MAN_EXTRACT, man_handler),
    # Add more below (Incident Status, Calls, Crew, GO Created/Updated, etc.)
]

# ---------- Tag a dataframe -> narrow schema ----------
def tag_dataframe_narrow(df: pd.DataFrame, text_col: str = DESC_COL) -> pd.DataFrame:
    out = df.copy()
    results = out[text_col].astype(str).apply(lambda s: apply_rules(s, rules))
    # results is a series of dicts with keys Tag/Flags/event_meta
    out["Tag"] = results.map(lambda d: d["Tag"])
    out["Flags"] = results.map(lambda d: d["Flags"])
    out["event_meta"] = results.map(lambda d: d["event_meta"])
    # (Optional) JSON mirror for Parquet/BI tools that dislike Python dicts:
    out["event_meta_json"] = out["event_meta"].map(lambda d: json.dumps(d, default=str))
    return out

# ---------- Examples of working with the dict column ----------
# Filter all ETR rows:
# etr = df_tagged[df_tagged["Tag"].isin(["SYSTEM ETR", "MANUAL ETR"])]

# Pull values from event_meta on the fly:
# etr["etr_ts"] = etr["event_meta"].map(lambda d: d.get("etr_to_ts"))
# etr["loc"] = etr["event_meta"].map(lambda d: d.get("loc"))

# Later normalize a subset (wide form) if needed:
# wide = pd.json_normalize(etr["event_meta"]).add_prefix("meta_")
# etr_wide = pd.concat([etr.reset_index(drop=True), wide], axis=1)
