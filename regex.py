# -------- Incident Crew Remark applied to all locations --------
INC_CREW_REMARK_ALL_DETECT = re.compile(
    r'(?i)^\s*Incident\s*\[\s*\d+\s*\]\s*Crew\s+Remark\b'
)

INC_CREW_REMARK_ALL_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*Incident\s*\[\s*(?P<incident_id>\d+)\s*\]\s*
    Crew\s+Remark\s*
    \[\s*(?P<remark>.*?)\s*\]\s*
    has\s+been\s+appl(?:y|ied)\s+to\s+all\s+locations
    \s*$
    '''
)

def inc_crew_remark_all_handler(m):
    return {
        "cat": "CREW",
        "kind": "REMARK_APPLIED_ALL_LOCATIONS",
        "scope": "ALL_LOCATIONS",
        "incident_id": int(m.group("incident_id")),
        "remark": (m.group("remark") or "").strip() or None
    }

rules.append(
    Rule("Incident Crew Remark (All Locations)", 43, INC_CREW_REMARK_ALL_DETECT, INC_CREW_REMARK_ALL_EXTRACT, inc_crew_remark_all_handler)
)



# Detect: Crew[...] (new remark recorded | remark changed) ...
CREW_REMARK_DETECT = re.compile(
    r'(?i)^\s*Crew\s*\[\s*[^\]]+\s*\]\s*(?:new\s+remark\s+recorded|remark\s+changed)\b'
)

# Extract:
#  - bracketed remark can contain internal [ ... ] ; we capture up to the FINAL ']' before (from CAD)? end
CREW_REMARK_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*Crew\s*\[\s*(?P<crew>[^\]]+)\s*\]\s*
    (?:new\s+remark\s+recorded|remark\s+changed)
    (?:
        \s*\[\s*(?P<remark_bracket>.*?)(?=\]\s*(?:\s+from\s+[A-Z]+)?\s*$)\]\s*   # bracketed, allow inner [...]
      | \s+(?P<remark_free>.+?)                                                 # or free text
    )
    (?:\s+from\s+(?P<src>[A-Z]+))?
    \s*$
    '''
)


# Detect unchanged
LOC_CREW_REMARK_DETECT = re.compile(r'(?i)^\s*Location\s*\[\s*\d+\s*\]\s*Crew\s*:')

# Extract with look-ahead for the set-to value
LOC_CREW_REMARK_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*Location\s*\[\s*(?P<loc_id>\d+)\s*\]\s*
    Crew\s*:\s*\[\s*(?P<crew_ref>[^\]]+)\s*\]\s*
    Remark\s+is\s+
    (?:
        set\s+to\s*\[\s*(?P<remark>.*?)(?=\]\s*$)\]\s*   # remark may contain inner [...]
      | null
    )
    \s*$
    '''
)
def crew_remark_handler(m):
    remark = (m.group("remark_bracket") or m.group("remark_free") or "").strip()
    return {
        "cat": "CREW",
        "kind": "REMARK",
        "crew_ref": (m.group("crew") or "").strip(),
        "remark": remark,
        "source": (m.group("src") or "").upper() or None,
    }

def loc_crew_remark_handler(m):
    return {
        "cat": "CREW",
        "kind": "REMARK_SET" if m.group("remark") is not None else "REMARK_NULL",
        "scope": "LOCATION",
        "location_id": int(m.group("loc_id")),
        "crew_ref": m.group("crew_ref").strip(),
        "remark": (m.group("remark") or "").strip() or None
    }
rules.append(
    Rule("Location Crew Remark", 44, LOC_CREW_REMARK_DETECT, LOC_CREW_REMARK_EXTRACT, loc_crew_remark_handler)
)


# -------- Location Date Field (Energized / Initial / Estimated Restore) --------
import re
import pandas as pd
from typing import Dict, Any

# High priority: field change > status chatter
LOC_DATE_DETECT = re.compile(
    r'(?i)^\s*(?:His\s+)?Location\s*\[\s*\d+\s*\]\s*'
    r'(?:Energized|Initial|Estimated\s+Restore)\s+Date\s+has\s+been\b'
)

LOC_DATE_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*
    (?:His\s+)?Location
    \s*\[\s*(?P<loc_id>\d+)\s*\]\s*
    (?P<field>Energized|Initial|Estimated\s+Restore)\s+Date\s+has\s+been\s+
    (?:
        # (A) set to [...]
        set\s+to\s*\[\s*(?P<set_val>.*?)\s*\]
      |
        # (B) Change/Changed from [...] to [...]
        (?:Change|Changed)\s+from\s*\[\s*(?P<from_val>.*?)\s*\]\s*to\s*\[\s*(?P<to_val>.*?)\s*\]
      |
        # (C) removed
        removed
    )
    \s*$
    '''
)

def _coerce_dt_maybe(s: str | None):
    if s is None: return None
    s2 = s.strip()
    if s2 == "": return None
    return pd.to_datetime(s2, errors="coerce")

def _delta_flag(from_ts, to_ts):
    if from_ts is None or to_ts is None: return None, None
    if pd.isna(from_ts) or pd.isna(to_ts): return None, "DT_PARSE_ERR"
    dmin = (to_ts - from_ts).total_seconds() / 60.0
    if dmin < 0:   return dmin, "TO_BEFORE_FROM"
    if dmin > 0:   return dmin, "TO_AFTER_FROM"
    return 0.0, "TO_EQUAL_FROM"

FIELD_MAP = {
    "ENERGIZED": "ENERGIZED",
    "INITIAL": "INITIAL",
    "ESTIMATED RESTORE": "ESTIMATED_RESTORE",
}

def _canon_field(s: str) -> str:
    up = re.sub(r'\s+', ' ', s.strip()).upper()
    return FIELD_MAP.get(up, up.replace(' ', '_'))

def loc_date_handler(m: re.Match) -> Dict[str, Any]:
    loc_id = int(m.group("loc_id"))
    field  = _canon_field(m.group("field"))

    # (A) SET
    if m.group("set_val") is not None:
        new_ts = _coerce_dt_maybe(m.group("set_val"))
        return {
            "cat": "LOCATION_DATE",
            "action": "SET",
            "field": field,                 # ENERGIZED / INITIAL / ESTIMATED_RESTORE
            "location_id": loc_id,
            "new_ts": new_ts,
        }

    # (B) CHANGE
    if m.group("from_val") is not None or m.group("to_val") is not None:
        old_ts = _coerce_dt_maybe(m.group("from_val"))
        new_ts = _coerce_dt_maybe(m.group("to_val"))
        delta, flag = _delta_flag(old_ts, new_ts)
        meta = {
            "cat": "LOCATION_DATE",
            "action": "CHANGED",
            "field": field,
            "location_id": loc_id,
            "old_ts": old_ts,
            "new_ts": new_ts,
            "delta_min": delta,            # + later, - earlier
        }
        if flag: meta["_flags"] = flag
        return meta

    # (C) REMOVED
    return {
        "cat": "LOCATION_DATE",
        "action": "REMOVED",
        "field": field,
        "location_id": loc_id,
    }

# Register BEFORE Location Status (e.g., 75 < 80)
rules.append(
    Rule("Location Date Field", 75, LOC_DATE_DETECT, LOC_DATE_EXTRACT, loc_date_handler)
)




# -------- Cause / Occurrence recorded from CAD (global) --------
import re
from typing import Dict, Any

CAD_CODE_DETECT = re.compile(
    r'(?i)^\s*(?:New\s+(?:cause\s+code|occurrence)\s+recorded|Cause\s+code\s+removed)\b'
)

CAD_CODE_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*
    (?:
        # New occurrence recorded [CODE][DESC] from CAD
        New\s+occurrence\s+recorded
        \s*\[\s*(?P<occ_code>[^\]]+)\s*\]
        (?:\s*\[\s*(?P<occ_desc>[^\]]+)\s*\])?
        (?:\s+from\s+(?P<src1>CAD))?
      |
        # New cause code recorded [CODE][DESC] from CAD
        New\s+cause\s+code\s+recorded
        \s*\[\s*(?P<cause_code>[^\]]+)\s*\]
        (?:\s*\[\s*(?P<cause_desc>[^\]]+)\s*\])?
        (?:\s+from\s+(?P<src2>CAD))?
      |
        # Cause code removed [CODE]
        Cause\s+code\s+removed
        \s*\[\s*(?P<cause_removed>[^\]]+)\s*\]
    )
    \s*$
    '''
)

def cad_code_handler(m: re.Match) -> Dict[str, Any]:
    if m.group("occ_code"):
        return {
            "cat": "CODE",
            "kind": "OCCURRENCE_RECORDED",
            "source": "CAD" if m.group("src1") else None,
            "code": m.group("occ_code").strip(),   # e.g., STRCLR
            "desc": (m.group("occ_desc") or "").strip() or None,  # e.g., CLEAR\STRUCTURE
        }
    if m.group("cause_code"):
        return {
            "cat": "CODE",
            "kind": "CAUSE_RECORDED",
            "source": "CAD" if m.group("src2") else None,
            "code": m.group("cause_code").strip(), # e.g., OPESOPOPR
            "desc": (m.group("cause_desc") or "").strip() or None, # e.g., SOP\OPERATOR/CREW\OPEN FOR REPAIRS
        }
    # removed
    return {
        "cat": "CODE",
        "kind": "CAUSE_REMOVED",
        "code": m.group("cause_removed").strip(),
    }

# Priority: put near other signal/call-level events (e.g., 60–70)
rules.append(
    Rule("CAD Code Recorded/Removed", 66, CAD_CODE_DETECT, CAD_CODE_EXTRACT, cad_code_handler)
)




# -------- Location-level Cause / Occurrence set/removed --------
LOC_CODE_DETECT = re.compile(
    r'(?i)^\s*His\s+Location\s*\[\s*\d+\s*\]\s*(?:Cause|Occurn)\s+has\s+been\s+(?:set\s+to|removed)\b'
)

LOC_CODE_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*His\s+Location
    \s*\[\s*(?P<loc_id>\d+)\s*\]\s*
    (?P<which>Cause|Occurn)\s+has\s+been\s+
    (?:
        set\s+to\s*\[\s*(?P<val>.*?)\s*\]     # capture whole value; may contain slashes/semicolons
      | removed
    )
    \s*$
    '''
)

def loc_code_handler(m: re.Match) -> Dict[str, Any]:
    which = m.group("which").upper()          # CAUSE or OCCURN
    val   = m.group("val")
    if val is not None:
        return {
            "cat": "LOCATION_CODE",
            "kind": f"{which}_SET",           # CAUSE_SET / OCCURN_SET
            "location_id": int(m.group("loc_id")),
            "value": val.strip(),             # keep entire text, e.g., "CLEAR\STRUCTURE;"
        }
    else:
        return {
            "cat": "LOCATION_CODE",
            "kind": f"{which}_REMOVED",       # CAUSE_REMOVED / OCCURN_REMOVED
            "location_id": int(m.group("loc_id")),
        }

# Priority: these are field changes; put high (e.g., 76), just below Energized Date (75)
rules.append(
    Rule("Location Code Set/Removed", 76, LOC_CODE_DETECT, LOC_CODE_EXTRACT, loc_code_handler)
)






# --- Incident Details Accessed ---
import re
from typing import Dict, Any

INC_DETAILS_DETECT = re.compile(r'(?i)^\s*Incident\s+details\s+accessed\b')

INC_DETAILS_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*Incident\s+details\s+accessed
    (?:\s+for\s+the\s+first\s+time)?      # optional "for the first time"
    \s*$
    '''
)

def inc_details_handler(m: re.Match) -> Dict[str, Any]:
    first = bool(re.search(r'(?i)for\s+the\s+first\s+time', m.group(0)))
    return {
        "cat": "INCIDENT",
        "kind": "DETAILS_ACCESSED_FIRST" if first else "DETAILS_ACCESSED",
        "first_time": first,
    }

rules.append(
    Rule("Incident Details Accessed", 31, INC_DETAILS_DETECT, INC_DETAILS_EXTRACT, inc_details_handler)
)



# --- Incident Analyzed (optional device set) ---
INC_ANALYZED_DETECT = re.compile(r'(?i)^\s*Incident\s+Analyzed\b')

INC_ANALYZED_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*Incident\s+Analyzed\.?                # "Incident Analyzed" with optional period
    (?:\s*                                     # optional trailing device assignment clause
        Incident\s+Device\s+set\s+to\s*
        \[\s*(?P<dev_id>[^\]]+)\s*\]\s*
        (?P<dev_label>[A-Za-z0-9_\-]+)
    )?
    \s*$
    '''
)

def inc_analyzed_handler(m: re.Match) -> Dict[str, Any]:
    dev_id = (m.group("dev_id") or "").strip() or None
    dev_label = (m.group("dev_label") or "").strip() or None
    return {
        "cat": "INCIDENT",
        "kind": "ANALYZED",
        "device_id": dev_id,
        "device_label": dev_label,
    }

rules.append(
    Rule("Incident Analyzed", 32, INC_ANALYZED_DETECT, INC_ANALYZED_EXTRACT, inc_analyzed_handler)
)


# --- Incident Device main call changed ---
INC_MAINCALL_DETECT = re.compile(
    r'(?i)^\s*Change\s+Incident\s+Device\s+main\s+call\s+to\b'
)

INC_MAINCALL_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*Change\s+Incident\s+Device\s+main\s+call\s+to\s*
    \[\s*(?P<dev_id>[^\]]+)\s*\]
    \s*$
    '''
)

def inc_maincall_handler(m: re.Match) -> Dict[str, Any]:
    return {
        "cat": "INCIDENT",
        "kind": "DEVICE_MAIN_CALL_CHANGED",
        "device_id": (m.group("dev_id") or "").strip(),
    }

rules.append(
    Rule("Incident Device Main Call Changed", 33, INC_MAINCALL_DETECT, INC_MAINCALL_EXTRACT, inc_maincall_handler)
)




# -------- Location Energized Date (set / change / remove) --------
import re
import pandas as pd
from typing import Dict, Any

# High priority: actual field change
LOC_ENERGIZED_DETECT = re.compile(
    r'(?i)^\s*(?:His\s+)?Location\s*\[\s*\d+\s*\]\s*Energized\s+Date\s+has\s+been\b'
)

# We support:
#  A) "... set to [ <any text, possibly empty> ]"
#  B) "... Change from [ <any> ] to [ <any> ]"
#  C) "... removed"
LOC_ENERGIZED_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*
    (?:His\s+)?Location
    \s*\[\s*(?P<loc_id>\d+)\s*\]\s*
    Energized\s+Date\s+has\s+been\s+
    (?:
        # (A) set to [...]
        set\s+to\s*\[\s*(?P<set_val>.*?)\s*\]
      |
        # (B) Change from [...] to [...]
        Change\s+from\s*\[\s*(?P<from_val>.*?)\s*\]\s*to\s*\[\s*(?P<to_val>.*?)\s*\]
      |
        # (C) removed
        removed
    )
    \s*$
    '''
)

def _coerce_dt_maybe(s: str | None):
    """'' or None -> None; else best-effort to pandas timestamp."""
    if s is None: 
        return None
    s2 = s.strip()
    if s2 == "":
        return None
    return pd.to_datetime(s2, errors="coerce")

def _etr_delta_and_flag(from_ts, to_ts):
    if from_ts is None or to_ts is None:
        return None, None
    if pd.isna(from_ts) or pd.isna(to_ts):
        return None, "DT_PARSE_ERR"
    delta = (to_ts - from_ts).total_seconds() / 60.0
    if delta < 0:
        return delta, "TO_BEFORE_FROM"
    elif delta > 0:
        return delta, "TO_AFTER_FROM"
    else:
        return 0.0, "TO_EQUAL_FROM"

def loc_energized_handler(m: re.Match) -> Dict[str, Any]:
    loc_id = int(m.group("loc_id"))
    set_val  = m.group("set_val")
    from_val = m.group("from_val")
    to_val   = m.group("to_val")

    if set_val is not None:
        # (A) SET
        new_ts = _coerce_dt_maybe(set_val)
        return {
            "cat": "LOCATION",
            "kind": "ENERGIZED_DATE_SET",
            "location_id": loc_id,
            "new_ts": new_ts,
        }

    if from_val is not None or to_val is not None:
        # (B) CHANGE
        from_ts = _coerce_dt_maybe(from_val)
        to_ts   = _coerce_dt_maybe(to_val)
        delta, flag = _etr_delta_and_flag(from_ts, to_ts)
        meta = {
            "cat": "LOCATION",
            "kind": "ENERGIZED_DATE_CHANGED",
            "location_id": loc_id,
            "old_ts": from_ts,
            "new_ts": to_ts,
            "delta_min": delta,      # + => moved later, - => earlier
        }
        if flag:
            meta["_flags"] = flag
        return meta

    # (C) REMOVED
    return {
        "cat": "LOCATION",
        "kind": "ENERGIZED_DATE_REMOVED",
        "location_id": loc_id,
    }

# Register BEFORE Location Status (e.g., 75 vs 80)
rules.append(
    Rule("Location Energized Date", 75, LOC_ENERGIZED_DETECT, LOC_ENERGIZED_EXTRACT, loc_energized_handler)
)









# -------- Memo Operations (add/change/delete) --------
import re
from typing import Dict, Any
# from ..engine import Rule   # if using package layout

MEMO_DETECT = re.compile(
    r'(?i)^\s*(?:\[MultiEdit\]\s*)?(?:Added|Changed|Deleted)\b.*\bmemo',  # cheap, robust gate
)

MEMO_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*
    (?P<multi>\[MultiEdit\]\s*)?                     # optional "[MultiEdit]" prefix
    (?:
        # ------------------ ADDED ------------------
        Added\s+(?:new\s+)?memo\s+with\s+id\s+(?P<add_id>\d+)
      |
        # ------------------ DELETED ----------------
        Deleted\s+memo\s+number\s+(?P<del_id>\d+)
      |
        Deleted\s+memos\s+having\s+specific\s+incident\s+ids
        (?P<bulk_del_rest>.*)?                       # no explicit IDs in your examples

      |
        # ------------------ CHANGED (by incident id) ---------------
        Changed\s+memo\s+with\s+incident\s+id\s+no\s+(?P<chg_inc>\d+)
        (?:\s*,\s*outage\s+code\s+updated)?          # optional trailer
      |
        # outage datetime of memo with incident id
        Changed\s+outage\s+datetime\s+of\s+memo\s+with\s+incident\s+id\s+no\s+(?P<chg_out_inc>\d+)
      |
        # each memos having incident id
        Changed\s+each\s+memos?\s+having\s+incident\s+id\s+no\s+(?P<chg_each_inc>\d+)
      |
        # ------------------ CHANGED (by memo number) ---------------
        Changed\s+incident\s+id\s+of\s+memo\s+number\s+(?P<chg_inc_memo>\d+)
      |
        Changed\s+outage\s+and\s+upd\s+datetime\s+of\s+memo\s+number\s+(?P<chg_both_memo>\d+)
      |
        Changed\s+end\s+datetime\s+of\s+memo\s+number\s+(?P<chg_end_memo>\d+)
    )
    \s*$
    '''
)

def _fields_from_match(m: re.Match) -> list[str]:
    """Infer which fields changed from the matched branch/phrase."""
    if m.group("chg_out_inc"):
        return ["OUTAGE_DATETIME"]
    if m.group("chg_both_memo"):
        return ["OUTAGE_DATETIME", "UPD_DATETIME"]
    if m.group("chg_end_memo"):
        return ["END_DATETIME"]
    if m.group("chg_inc_memo"):
        return ["INCIDENT_ID"]
    # Generic 'Changed memo with incident id no ###'
    if m.group("chg_inc") or m.group("chg_each_inc"):
        # may also have 'outage code updated' trailer; capture as field
        text = m.group(0)
        fields = []
        if re.search(r'(?i)\boutage\s+code\s+updated\b', text):
            fields.append("OUTAGE_CODE")
        # If none detected, mark generic change
        return fields or ["GENERIC"]
    return ["GENERIC"]

def memo_handler(m: re.Match) -> Dict[str, Any]:
    # Which branch fired?
    meta: Dict[str, Any] = {"cat": "MEMO"}
    meta["multi_edit"] = bool(m.group("multi"))

    if m.group("add_id"):
        meta.update({
            "kind": "ADDED",
            "memo_id": int(m.group("add_id")),
        })
        return meta

    if m.group("del_id"):
        meta.update({
            "kind": "DELETED",
            "memo_id": int(m.group("del_id")),
        })
        return meta

    if m.group("bulk_del_rest") is not None:
        meta.update({
            "kind": "DELETED",
            "scope": "MULTIPLE_BY_INCIDENT_IDS",
            "ids_list_present": False,   # your sample doesn't enumerate them
        })
        return meta

    # CHANGED variants by incident id:
    if m.group("chg_out_inc"):
        meta.update({
            "kind": "CHANGED",
            "incident_id": int(m.group("chg_out_inc")),
            "fields_changed": ["OUTAGE_DATETIME"],
        })
        return meta

    if m.group("chg_each_inc"):
        meta.update({
            "kind": "CHANGED",
            "incident_id": int(m.group("chg_each_inc")),
            "scope": "EACH_MEMO_FOR_INCIDENT",
            "fields_changed": ["GENERIC"],
        })
        return meta

    if m.group("chg_inc"):
        fields = _fields_from_match(m)
        meta.update({
            "kind": "CHANGED",
            "incident_id": int(m.group("chg_inc")),
            "fields_changed": fields,
        })
        return meta

    # CHANGED variants by memo number:
    if m.group("chg_inc_memo"):
        meta.update({
            "kind": "CHANGED",
            "memo_id": int(m.group("chg_inc_memo")),
            "fields_changed": ["INCIDENT_ID"],
        })
        return meta

    if m.group("chg_both_memo"):
        meta.update({
            "kind": "CHANGED",
            "memo_id": int(m.group("chg_both_memo")),
            "fields_changed": ["OUTAGE_DATETIME", "UPD_DATETIME"],
        })
        return meta

    if m.group("chg_end_memo"):
        meta.update({
            "kind": "CHANGED",
            "memo_id": int(m.group("chg_end_memo")),
            "fields_changed": ["END_DATETIME"],
        })
        return meta

    # Fallback (shouldn't hit with our branches)
    meta.update({"kind": "CHANGED", "fields_changed": ["GENERIC"]})
    return meta

# Register (admin-ish; after Crew/Location/GO/Calls; before Archive housekeeping if you like)
rules.append(
    Rule("Memo Operation", 88, MEMO_DETECT, MEMO_EXTRACT, memo_handler)
)




ARCHIVE_OP_DETECT = re.compile(r'(?i)^\s*Archive:')

ARCHIVE_OP_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*Archive:\s*
    \[\s*(?P<op>[^\]]+)\s*\]
    (?:                                     # ---- optional tail ----
        \s*to\s*
        (?P<count>\d+)\s*                   # <-- digits only
        (?P<target>                         # crew locations / crew of locations / locations
            (?:crew\s+(?:of\s+)?locations?) |
            (?:locations?)
        )
        \s*\[\s*(?P<linkinfo>[^\]]*)\s*\]   # e.g., "NCC and Linked To Cad"
        \s*(?P<tail>.*)                     # e.g., "without data."
    )?
    \s*$
    '''
)

def archive_op_handler(m):
    op       = (m.group("op") or "").strip()
    count    = int(m.group("count")) if m.group("count") else None
    target_s = (m.group("target") or "").strip().lower() or None
    linkinfo = (m.group("linkinfo") or "").strip() or None
    tail     = (m.group("tail") or "").strip() or None

    target = None
    if target_s:
        target = "CREW_LOCATIONS" if "crew" in target_s else "LOCATIONS"

    def _contains(s, pat): return bool(s and re.search(pat, s, flags=re.I))

    meta = {
        "cat": "ARCHIVE",
        "kind": "OPERATION",
        "operation": op,
        "count": count,                 # integer or None
        "target": target,               # CREW_LOCATIONS / LOCATIONS / None
        "ncc": _contains(linkinfo, r'\bNCC\b') if linkinfo else None,
        "itc": _contains(linkinfo, r'\bITC\b') if linkinfo else None,
        "connected": _contains(linkinfo, r'\bConnected\b') if linkinfo else None,
        "linked_to_cad": _contains(linkinfo, r'\bLinked\s+To\s+CAD\b') if linkinfo else None,
        "not_linked_to_cad": _contains(linkinfo, r'\bNOT\s+Linked\s+To\s+CAD\b') if linkinfo else None,
        "without_data": _contains(tail, r'\bwithout\s+data\b') if tail else None,
        "raw_linkinfo": linkinfo or None,
        "raw_tail": tail or None,
    }
    if re.search(r'(?i)\ball\s+locations\b', op):
        meta["scope"] = "ALL_LOCATIONS"
    return meta



# --- Archive Operation (revised) ---
ARCHIVE_OP_DETECT = re.compile(r'(?i)^\s*Archive:')

ARCHIVE_OP_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*Archive:\s*
    \[\s*(?P<op>[^\]]+)\s*\]                 # e.g., "Copy Repair", "Duplicate Details for all locations"
    (?:                                      # ---- optional "to N <target>[...] ..." tail ----
        \s*to\s*
        (?P<count>\d+)\s*
        (?P<target>(?:crew\s+locations?|locations?))    # "crew location(s)" or "location(s)"
        \s*\[\s*(?P<linkinfo>[^\]]*)\s*\]               # e.g., "Connected and Linked To CAD"
        \s*(?P<tail>.*)                                 # e.g., "without data."
    )?
    \s*$
    '''
)

def _bool_contains(s, pat): return bool(s and re.search(pat, s, flags=re.I))

def archive_op_handler(m):
    op = (m.group("op") or "").strip()
    count = m.group("count")
    target = (m.group("target") or "").strip().lower() or None
    linkinfo = (m.group("linkinfo") or "").strip() or None
    tail = (m.group("tail") or "").strip() or None

    meta = {
        "cat": "ARCHIVE",
        "kind": "OPERATION",
        "operation": op,
        "count": int(count) if count else None,
        "target": ("CREW_LOCATIONS" if target and "crew" in target else "LOCATIONS") if target else None,
        "connected": _bool_contains(linkinfo, r'\bConnected\b') if linkinfo else None,
        "linked_to_cad": _bool_contains(linkinfo, r'\bLinked\s+To\s+CAD\b') if linkinfo else None,
        "not_linked_to_cad": _bool_contains(linkinfo, r'\bNOT\s+Linked\s+To\s+CAD\b') if linkinfo else None,
        "without_data": _bool_contains(tail, r'\bwithout\s+data\b') if tail else None,
        "raw_linkinfo": linkinfo,
        "raw_tail": tail,
    }

    # Helpful hint for “all locations” ops without a tail:
    if re.search(r'(?i)\ball\s+locations\b', op):
        meta["scope"] = "ALL_LOCATIONS"

    return meta

# keep priority ~90




# --- Incident Archived (revised) ---
INC_ARCHIVE_DETECT = re.compile(
    r'(?i)^\s*(?:Incident\s+)?(?:Archived|ARCHIVED)(?:\s+incident)?\b'
)

INC_ARCHIVE_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*
    (?:Incident\s+)?                 # optional leading "Incident"
    (?:Archived|ARCHIVED)            # Archived/ARCHIVED
    (?:\s+incident)?                 # optional trailing "incident"
    (?:\s+by\s+(?P<user>.+?))?       # <-- plain "by USER" (no brackets), optional
    \s*$                             # end of line
    '''
)

def inc_archive_handler(m):
    user = (m.group("user") or "").strip() or None
    return {"cat":"INCIDENT","kind":"ARCHIVED","by_user": user}

# keep your existing priority (e.g., 35)





# -------- Archive Snapshot: downstream/premise info for incident device --------
import re
from typing import Dict, Any

ARCH_INFO_DETECT = re.compile(
    r'(?i)^\s*Archived\s+(?:downstream|premise)\s+info\b'
)

ARCH_INFO_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*Archived\s+
    (?P<what>downstream|premise)\s+info
    # optional qualifier in parentheses; may appear as "(...)" or "for (...)"
    (?:\s*(?:for\s*)?
        \(\s*
            (?:
                (?P<count>\d+)\s*(?P<unit>transformers?|customers?)   # e.g., "3 transformers", "10 customers"
              | (?P<src>ITC|NCC)                                     # or source tag
            )
        \s*\)
    )?
    \s*for\s+incident\s+device\s+
    (?P<device>[A-Za-z0-9\-/]+)                                      # device id token
    \s*$
    '''
)

def arch_info_handler(m: re.Match) -> Dict[str, Any]:
    what = m.group("what").upper()          # DOWNSTREAM / PREMISE
    count = m.group("count")
    unit  = m.group("unit")
    src   = m.group("src")
    device = m.group("device")
    # normalize unit
    unit_norm = (unit or "").strip().lower() or None
    if unit_norm:
        unit_norm = "TRANSFORMERS" if unit_norm.startswith("transformer") else "CUSTOMERS"
    return {
        "cat": "ARCHIVE",
        "kind": f"{what}_INFO",             # DOWNSTREAM_INFO / PREMISE_INFO
        "device_id": device,
        "count": int(count) if count else None,
        "unit": unit_norm,                  # TRANSFORMERS / CUSTOMERS / None
        "source": (src.upper() if src else None),   # ITC / NCC / None
    }

# Register with similar priority to other archive ops, but separate from "Archive:" housekeeping.
rules.append(
    Rule("Archive Info (Device)", 92, ARCH_INFO_DETECT, ARCH_INFO_EXTRACT, arch_info_handler)
)




# -------- Archive Operation (Copy ...) --------
ARCHIVE_OP_DETECT = re.compile(
    r'(?i)^\s*Archive:'
)

ARCHIVE_OP_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*Archive:\s*
    \[\s*(?P<op>[^\]]+)\s*\]                # operation, e.g., "Copy Repair"
    \s*to\s*
    (?P<count>\d+)\s*
    (?P<target>(?:crew\s+locations?|locations?))   # "crew location(s)" or "location(s)"
    \s*
    \[\s*(?P<linkinfo>[^\]]*)\s*\]          # e.g., "Connected and Linked To CAD"
    \s*
    (?P<tail>.*)                             # e.g., "without data."
    \s*$
    '''
)

def _bool_contains(s: str | None, pat: str) -> bool:
    return bool(s and re.search(pat, s, flags=re.I))

def archive_op_handler(m: re.Match) -> Dict[str, Any]:
    op = (m.group("op") or "").strip()                 # "Copy Repair", "Copy Occurence", "Copy Remark", "Copy Cause"
    target = (m.group("target") or "").strip().lower() # "crew location(s)" vs "location(s)"
    linkinfo = (m.group("linkinfo") or "").strip()
    tail = (m.group("tail") or "").strip()

    return {
        "cat": "ARCHIVE",
        "kind": "OPERATION",
        "operation": op,                                # normalized name of the operation
        "count": int(m.group("count")),
        "target": "CREW_LOCATIONS" if "crew" in target else "LOCATIONS",
        # connection/link flags parsed from brackets:
        "connected": _bool_contains(linkinfo, r'\bConnected\b'),
        "linked_to_cad": _bool_contains(linkinfo, r'\bLinked\s+To\s+CAD\b'),
        "not_linked_to_cad": _bool_contains(linkinfo, r'\bNOT\s+Linked\s+To\s+CAD\b'),
        # tail flags:
        "without_data": _bool_contains(tail, r'\bwithout\s+data\b'),
        "raw_linkinfo": linkinfo or None,               # keep raw text for audits
        "raw_tail": tail or None,
    }

# Priority: after incident/location/crew states; these are admin/housekeeping entries.
rules.append(
    Rule("Archive Operation", 90, ARCHIVE_OP_DETECT, ARCHIVE_OP_EXTRACT, archive_op_handler)
)



# -------- Incident Archived --------
import re
from typing import Dict, Any

INC_ARCHIVE_DETECT = re.compile(
    r'(?i)^\s*(?:Incident\s+)?(?:Archived|ARCHIVED)(?:\s+incident)?\b'
)

INC_ARCHIVE_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*
    (?:Incident\s+)?                    # optional leading "Incident"
    (?:Archived|ARCHIVED)               # Archived (any case)
    (?:\s+incident)?                    # optional trailing "incident"
    (?:\s+by\s*\[\s*(?P<user>[^\]]+)\s*\])?   # optional "by [USER]"
    \s*$
    '''
)

def inc_archive_handler(m: re.Match) -> Dict[str, Any]:
    return {
        "cat": "INCIDENT",
        "kind": "ARCHIVED",
        "by_user": (m.group("user") or None),
    }

# Priority: close to other incident-level rules, after status changes.
rules.append(
    Rule("Incident Archived", 35, INC_ARCHIVE_DETECT, INC_ARCHIVE_EXTRACT, inc_archive_handler)
)


# -------- Location Status (changed status to ...) --------
import re
from typing import Dict, Any
from ..engine import Rule  # adjust import if not in a package

# Detect ONLY status-change lines; avoids "Energized Date has been set to ..."
LOC_STATUS_DETECT = re.compile(
    r'(?i)^\s*Location\s*\[\s*\d+\s*\].*?\bchanged\s+status\s+to\b'
)

# Examples handled:
# Location [2049692667] with Priority Score [] changed status to : Energized
# Location [2049692756] with Priority Score [18.72] changed status to : Working
# (Priority Score may be empty or absent)
LOC_STATUS_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*Location\s*\[\s*(?P<loc_id>\d+)\s*\]\s*
    (?:with\s+Priority\s+Score\s*\[\s*(?P<ps>[^\]]*)\s*\]\s*)?
    changed\s+status\s+to\s*:\s*
    (?P<state>[A-Za-z][A-Za-z ]*[A-Za-z])\s*$
    '''
)

LOC_STATES = {
    "ASSIGNED": "ASSIGNED",
    "UNASSIGNED": "UNASSIGNED",
    "DISPATCHED": "DISPATCHED",
    "WORKING": "WORKING",
    "COMPLETED": "COMPLETED",
    "CANCELLED": "CANCELLED",
    "ENERGIZED": "ENERGIZED",
}

def _canon_loc_state(s: str):
    up = re.sub(r'\s+', ' ', s.strip()).upper()
    canon = LOC_STATES.get(up)
    return (canon or up.replace(' ', '_')), (None if canon else "UNKNOWN_LOCATION_STATE")

def _to_float_or_none(s: str | None):
    if s is None: return None
    s = s.strip()
    if s == "": return None
    try: return float(s)
    except Exception: return None

def loc_status_handler(m: re.Match) -> Dict[str, Any]:
    state_raw = m.group("state")
    state, flag = _canon_loc_state(state_raw)
    meta = {
        "cat": "LOCATION",
        "kind": "STATUS",
        "location_id": int(m.group("loc_id")),
        "priority_score": _to_float_or_none(m.group("ps")),  # [] -> None, "18.72" -> 18.72
        "state_raw": state_raw.strip(),
        "state": state,
    }
    if flag:
        meta["_flags"] = flag
    return meta

# Register AFTER your future "Location Energized Date Set" rule (which should be higher priority, e.g., 75)
rules.append(
    Rule("Location Status Change", 80, LOC_STATUS_DETECT, LOC_STATUS_EXTRACT, loc_status_handler)
)



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
