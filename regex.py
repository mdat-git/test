# -------- Call Remark (changed) --------
import re
from typing import Dict, Any

CALL_REMARK_DETECT = re.compile(
    r'(?i)^\s*Call\s+remark\s+has\s+been\s+changed\s+to\b'
)

CALL_REMARK_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*Call\s+remark\s+has\s+been\s+changed\s+to
    \s*\[\s*(?P<remark>[^\]]+)\s*\]\s*$
    '''
)

def call_remark_handler(m: re.Match) -> Dict[str, Any]:
    return {
        "cat": "CALL",
        "kind": "REMARK_CHANGED",
        "remark": m.group("remark").strip(),
    }

# Priority: before generic call-reported, no overlap with crew
rules.append(
    Rule("Call Remark Changed", 40, CALL_REMARK_DETECT, CALL_REMARK_EXTRACT, call_remark_handler)
)


# -------- Call Reported --------

CALL_REPORTED_DETECT = re.compile(
    r'(?i)^\s*(?:SCADA|ADMS)?\s*Call\s+reported\b'
)

# YYYY/MM/DD HH:MM:SS
DTY = r'\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}'

CALL_REPORTED_EXTRACT = re.compile(
    rf'''(?ix)
    ^\s*
    (?:(?P<src>SCADA|ADMS)\s+)?          # optional source prefix
    Call\s+reported\s+at\s+
    (?:
        # (A) Bracket form: [ID] LABEL with TEXT
        \[\s*(?P<br_id>[^\]]+)\s*\]\s*(?P<br_label>\S+)\s+with\s+(?P<br_with>.+)
      |
        # (B) Timestamped transformer form
        (?P<ts>{DTY})\s+for\s+(?:this\s+)?Transformer\s*
        \[\s*(?P<x_id>[\w\-]+)\s*\]\s*(?P<x_label>\S+)
        (?:\s+with\s+(?P<x_with>.+))?
    )
    \s*$
    '''
)

def call_reported_handler(m: re.Match) -> Dict[str, Any]:
    src = (m.group("src") or "").upper() or None
    # Decide which branch matched (A vs B)
    if m.group("br_id"):
        # Bracket variant (SCADA/ADMS)
        with_text = (m.group("br_with") or "").strip()
        ami = bool(re.search(r'(?i)\bAMI\b', with_text))
        return {
            "cat": "CALL",
            "kind": "REPORTED",
            "source": src,                        # SCADA / ADMS / None
            "form": "BRACKET",
            "entity_id": m.group("br_id").strip(),    # e.g., 08270 / 1655681E
            "entity_label": m.group("br_label").strip(),  # e.g., 08270 / P5399616-B
            "channel": "AMI" if ami else None,
            "with_text": with_text,              # raw tail, e.g., "OPEN OPEN SW/CREATE INC"
            "reported_ts": None,                 # no timestamp in this shape
        }
    else:
        # Timestamped transformer variant
        with_text = (m.group("x_with") or "").strip()
        ami = bool(re.search(r'(?i)\bAMI\b', with_text))
        return {
            "cat": "CALL",
            "kind": "REPORTED",
            "source": src,                        # usually None for these lines
            "form": "TRANSFORMER",
            "reported_ts": pd.to_datetime(m.group("ts"), errors="coerce"),
            "asset_type": "TRANSFORMER",
            "asset_id": m.group("x_id").strip(),     # e.g., 5399616
            "asset_label": m.group("x_label").strip(),  # e.g., P5399616
            "channel": "AMI" if ami else None,        # AMI vs non-AMI
            "with_text": with_text,                   # e.g., "AMI ESC METER"
        }

# Priority: put around 60–65 (after Incident/Crew/Crew Remark, before GO if you prefer)
rules.append(
    Rule("Call Reported", 60, CALL_REPORTED_DETECT, CALL_REPORTED_EXTRACT, call_reported_handler)
)



# -------- Crew Remark (from CAD) --------
import re
from typing import Dict, Any

CREW_REMARK_DETECT = re.compile(
    r'(?i)^\s*Crew\s*\[\s*[^\]]+\s*\]\s*new\s+remark\s+recorded\b'
)

# Common shapes:
#   Crew [9216] new remark recorded [ ... ] from CAD
#   Crew [9216] new remark recorded [ ... ]
#   Crew [9216] new remark recorded ... from CAD   (rare no bracket)
CREW_REMARK_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*Crew\s*\[\s*(?P<crew>[^\]]+)\s*\]\s*
    new\s+remark\s+recorded
    (?:\s*\[\s*(?P<remark_bracket>[^\]]+)\s*\] | \s+(?P<remark_free>.+?))      # bracketed or free text
    (?:\s+from\s+(?P<src>CAD))?
    \s*$
    '''
)

def crew_remark_handler(m: re.Match) -> Dict[str, Any]:
    remark = (m.group("remark_bracket") or m.group("remark_free") or "").strip()
    return {
        "cat": "CREW",
        "kind": "REMARK",
        "crew_ref": (m.group("crew") or "").strip(),
        "remark": remark,
        "source": "CAD" if m.group("src") else None,
    }

# Priority BEFORE Crew Status
rules.append(
    Rule("Crew Remark (CAD)", 45, CREW_REMARK_DETECT, CREW_REMARK_EXTRACT, crew_remark_handler)
)








# -------- GO / Job lifecycle --------
import re
from ..engine import Rule  # if you’re inside rules/*.py; else adjust import

GO_DETECT = re.compile(
    r'(?i)^\s*(?:Complex\s+)?Job\s*\[\s*GO\b'   # cheap gate: Job[...] or Complex Job[...] with GO
)

# Supports:
#  - Job [GO 092825-00231] created for Location [2049692667]
#  - Job [GO 092825-00231] updated
#  - Complex Job [GO 092825-00259] created for Incident
GO_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*
    (?P<prefix>Complex\s+)?Job
    \s*\[
        \s*(?P<go_raw>GO\s*\d{4,}\s*-\s*\d{3,})\s*
    \]\s*
    (?:
        # created for Location [123]
        created\s+for\s+(?P<tgt_type1>Location)\s*\[\s*(?P<loc_id>\d+)\s*\]
      |
        # created for Incident (no id)
        created\s+for\s+(?P<tgt_type2>Incident)
      |
        # updated (no target)
        (?P<updated>updated)
    )
    \s*$
    '''
)

def _norm_go_id(go_raw: str) -> str:
    # "GO 092825-00231" -> "GO092825-00231"
    return re.sub(r'\s+', '', go_raw)

def job_handler(m: re.Match) -> dict:
    go_raw = m.group("go_raw")
    go_id = go_raw.strip()
    go_norm = _norm_go_id(go_id)

    if m.group("updated"):
        kind = "UPDATED"
        tgt_type = None
        tgt_id = None
    elif m.group("tgt_type1"):  # Location
        kind = "CREATED"
        tgt_type = "LOCATION"
        tgt_id = m.group("loc_id")
    elif m.group("tgt_type2"):  # Incident
        kind = "CREATED"
        tgt_type = "INCIDENT"
        tgt_id = None
    else:
        kind = "UNKNOWN"
        tgt_type = None
        tgt_id = None

    return {
        "cat": "JOB",
        "kind": kind,                   # CREATED / UPDATED
        "go_id": go_id,                 # "GO 092825-00231"
        "go_id_norm": go_norm,          # "GO092825-00231" (join-friendly)
        "target_type": tgt_type,        # LOCATION / INCIDENT / None
        "target_id": (int(tgt_id) if tgt_id else None),
        "is_complex": bool(m.group("prefix")),  # Complex Job vs Job
    }

# Register (after Crew/Incident; before very generic rules)
rules.append(
    Rule("GO Job Lifecycle", 70, GO_DETECT, GO_EXTRACT, job_handler)
)



# -------- Crew Status (from CAD) --------
import re

CREW_STATUS_DETECT = re.compile(
    r'(?i)^\s*Crew\s*\[',  # any Crew[...] line; cheap gate
)

# One extractor handling all three shapes:
#  (A) "status changed to [State]"
#  (B) "status [Assigned] assigned"
#  (C) "unassigned"
CREW_STATUS_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*Crew
    \s*\[\s*(?P<crew>[^\]]+)\s*\]\s*
    (?:
        # (A) status changed to [State]
        status\s+changed\s+to\s*\[\s*(?P<state_changed>[^\]]+)\s*\]
      |
        # (B) status [State] assigned
        status\s*\[\s*(?P<state_bracket>[^\]]+)\s*\]\s+assigned
      |
        # (C) unassigned (no explicit bracketed state)
        (?P<unassigned>unassigned)
    )
    (?:\s+from\s+(?P<src>CAD))?
    \s*$
    '''
)

CREW_STATE_MAP = {
    "ASSIGNED": "ASSIGNED",
    "UNASSIGNED": "UNASSIGNED",
    "DISPATCHED": "DISPATCHED",
    "WORKING": "WORKING",
    "COMPLETED": "COMPLETED",
}

def _canon_crew_state(s: str | None) -> tuple[str | None, str | None]:
    """Normalize a state token; return (canonical, flag)."""
    if not s:
        return None, None
    up = re.sub(r'\s+', ' ', s.strip()).upper()
    canon = CREW_STATE_MAP.get(up)
    if canon:
        return canon, None
    # Unknown—surface but don't fail
    return up.replace(' ', '_'), "UNKNOWN_CREW_STATE"

def crew_status_handler(m: re.Match) -> dict:
    crew = (m.group("crew") or "").strip()
    # Decide which variant matched and pick a raw state
    if m.group("state_changed"):
        state_raw = m.group("state_changed")
    elif m.group("state_bracket"):
        state_raw = m.group("state_bracket")
    elif m.group("unassigned"):
        state_raw = "UNASSIGNED"
    else:
        state_raw = None

    state, flag = _canon_crew_state(state_raw)
    meta = {
        "cat": "CREW",
        "kind": "STATUS",
        "crew_ref": crew,               # e.g., 9216 / 4740 / LC / SAP
        "state_raw": state_raw,         # original (e.g., "Working")
        "state": state,                 # canonical (e.g., "WORKING")
        "source": "CAD" if m.group("src") else None,
    }
    if flag:
        meta["_flags"] = flag
    return meta

# Register with a sensible priority (after Incident Status)
rules.append(
    Rule("Crew Status (CAD)", 50, CREW_STATUS_DETECT, CREW_STATUS_EXTRACT, crew_status_handler)
)



# New extractor for ETR and Hanler 

SYSTEM_DETECT = re.compile(r'(?i)^\s*SYSTEM\s+ETR\b')

# Datetime: MM/DD/YYYY HH:MM[:SS]
DT = r'(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}(?::\d{2})?)'

SYSTEM_EXTRACT = re.compile(
    rf'''(?ix)
    ^\s*SYSTEM\s+ETR-?\s*
    (?:
        # --- (A) Set ETR for @ <Loc> To SYS ETR <to_dt> ---
        Set\s+ETR\s+for\s+@\s*(?P<loc1>.+?)\s+
        To\s+(?:(?P<to_type1>SYS)\s+ETR|ETR\s+(?P<to_type1_alt>SYS))\s*[:\-]?\s*
        (?P<to_dt1>{DT})\s*$
      |
        # --- (B) Change for @ <Loc> From [SYS-]<from_dt> To SYS ETR <to_dt> ---
        Change\s+for\s+@\s*(?P<loc2>.+?)\s+
        From\s*(?:(?P<from_type2>SYS)\s*)?[:\-]?\s*
        (?P<from_dt2>{DT})\s+
        To\s+(?:(?P<to_type2>SYS)\s+ETR|ETR\s+(?P<to_type2_alt>SYS))\s*[:\-]?\s*
        (?P<to_dt2>{DT})\s*$
    )
    '''
)

from typing import Dict, Any

def sys_handler(m: re.Match) -> Dict[str, Any]:
    # unify fields across both alternatives
    loc = m.group("loc1") or m.group("loc2")
    to_type = (m.group("to_type1") or m.group("to_type1_alt") or
               m.group("to_type2") or m.group("to_type2_alt") or "SYS").upper()

    to_dt_text = m.group("to_dt1") or m.group("to_dt2")
    from_type = (m.group("from_type2") or None)
    from_dt_text = m.group("from_dt2") or None

    meta = {
        "cat": "ETR",
        "kind": "SYSTEM",
        "loc": loc,
        "etr_from_type": (from_type.upper() if from_type else None),
        "etr_from_ts": coerce_dt(from_dt_text) if from_dt_text else None,
        "etr_to_type": to_type,   # always SYS in practice, but normalized anyway
        "etr_to_ts": coerce_dt(to_dt_text),
    }

    # Optional sanity check: if both from/to present and to < from, flag it.
    if meta["etr_from_ts"] is not None and meta["etr_to_ts"] is not None:
        try:
            if meta["etr_to_ts"] < meta["etr_from_ts"]:
                meta["_flags"] = "TO_BEFORE_FROM"
        except Exception:
            meta["_flags"] = "DT_PARSE_ERR"

    return meta




# --- Incident Status Change rule ---

INC_STATUS_DETECT = re.compile(
    r'(?i)\bIncident\s*\[\s*\d+\s*\]\s*change\s*status\s*to\b'
)

INC_STATUS_EXTRACT = re.compile(
    r'''(?ix)
    \bIncident\s*\[\s*(?P<incident_id>\d+)\s*\]\s*
    change\s*status\s*to\s*[:\-]?\s*
    (?P<state>[A-Za-z][A-Za-z ]*[A-Za-z])\s*$
    '''
)

KNOWN_STATES = {
    "ASSIGNED": "ASSIGNED",
    "UNASSIGNED": "UNASSIGNED",
    "DISPATCHED": "DISPATCHED",
    "WORKING": "WORKING",
    "PARTIALLY COMPLETED": "PARTIALLY_COMPLETED",
    "COMPLETED": "COMPLETED",
    "ENERGIZED": "ENERGIZED",
}

def canonicalize_state(s: str) -> tuple[str, str | None]:
    raw = (s or "").strip()
    up = re.sub(r'\s+', ' ', raw).upper()
    canon = KNOWN_STATES.get(up)
    flag = None if canon else "UNKNOWN_STATE"
    # fall back to a safe normalized token even if unknown
    canon = canon or up.replace(' ', '_')
    return canon, flag

def inc_status_handler(m: re.Match) -> dict:
    inc_id = m.group("incident_id")
    state_raw = m.group("state")
    state_canon, flag = canonicalize_state(state_raw)
    meta = {
        "cat": "INCIDENT",
        "kind": "STATUS",
        "incident_id": int(inc_id),
        "state_raw": state_raw.strip(),
        "state": state_canon,   # canonical
    }
    if flag:
        meta["_flags"] = flag
    return meta

# Register rule (choose a priority that runs after ETR but before very generic rules)
rules.append(
    Rule("Incident Status Change", 30, INC_STATUS_DETECT, INC_STATUS_EXTRACT, inc_status_handler)
)




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
