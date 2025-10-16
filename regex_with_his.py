## updated Combiner
# --- Record Combine Operation (incidents/locations; live or HIS) ---
import re
from typing import Dict, Any, List

COMBINED_DETECT = re.compile(
    r'(?i)^\s*(?:His\s+incident|Incidents?|Locations?)\b.*\bhas\s+been\s+combined\b'
)

COMBINED_EXTRACT = re.compile(
    r'''(?isx)  # s=dotall to be newline-tolerant; x=verbose
    ^\s*
    (?:
        # (A) Incident(s) [id, id, ...] has been combined (into|to) Incident <target>
        Incidents?\s*\[\s*(?P<inc_srcs>[^\]]+)\s*\]\s*has\s*been\s*combined\s*(?:into|to)\s*Incident\s*(?P<inc_tgt>\d+)\.?
      |
        # (B) Location(s) [id, id, ...] has been combined (into|to) Location <target>
        Locations?\s*\[\s*(?P<loc_srcs>[^\]]+)\s*\]\s*has\s*been\s*combined\s*(?:into|to)\s*Location\s*(?P<loc_tgt>\d+)\.?
      |
        # (C) His incident [optional src] - has been combined (in history)? (into|to) incident <target>
        His\s+incident
        (?:\s*\[\s*(?P<his_src>\d+)\s*\])?
        (?:\s*[-–]\s*)?
        has\s*been\s*combined
        (?:\s*in\s*history)?
        \s*(?:into|to)\s*incident\s*(?P<his_tgt>\d+)\.?
    )
    \s*$
    '''
)

def _parse_id_list(blob: str) -> List[int]:
    # Accept "131514840, 131514847" or "131514840 131514847" etc.
    return [int(x) for x in re.findall(r'\d+', blob or '')]

def combined_handler(m: re.Match) -> Dict[str, Any]:
    # Incidents with list
    if m.group("inc_srcs") is not None:
        srcs = _parse_id_list(m.group("inc_srcs"))
        return {
            "cat": "HISTORY",  # semantic family; layer will tell HIS vs LIVE
            "kind": "INCIDENTS_COMBINED",
            "combined_type": "INCIDENT",
            "sources": srcs,
            "source_count": len(srcs),
            "target_incident_id": int(m.group("inc_tgt")),
        }
    # Locations with list
    if m.group("loc_srcs") is not None:
        srcs = _parse_id_list(m.group("loc_srcs"))
        return {
            "cat": "HISTORY",
            "kind": "LOCATIONS_COMBINED",
            "combined_type": "LOCATION",
            "sources": srcs,
            "source_count": len(srcs),
            "target_location_id": int(m.group("loc_tgt")),
        }
    # His incident with optional single source or no source
    his_src = m.group("his_src")
    meta = {
        "cat": "HISTORY",
        "kind": "INCIDENT_COMBINED",
        "combined_type": "INCIDENT",
        "sources": ([int(his_src)] if his_src else None),
        "source_count": (1 if his_src else None),
        "target_incident_id": int(m.group("his_tgt")),
    }
    return meta

# Register it early so it wins over other lines
RULE_COMBINED = Rule("Record Combine Operation", 12, COMBINED_DETECT, COMBINED_EXTRACT, combined_handler)




# Location - date fields (SET CHANGED REMOVED) (updater)
LOC_DATE_DETECT = re.compile(
    r'(?i)^\s*(?:His\s+)?Location\s*\[\s*\d+\s*\]\s*(?:Energized|Initial|Estimated\s+Restore)\s+Date\s+has\s+been\b'
)
LOC_DATE_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*(?:His\s+)?Location\s*\[\s*(?P<loc_id>\d+)\s*\]\s*
    (?P<field>Energized|Initial|Estimated\s+Restore)\s+Date\s+has\s+been\s+
    (?:
        set\s+to\s*\[\s*(?P<set_val>.*?)\s*\]
      | (?:Change|Changed)\s+from\s*\[\s*(?P<from_val>.*?)\s*\]\s*to\s*\[\s*(?P<to_val>.*?)\s*\]
      | removed
    )\s*$
    '''
)
FIELD_MAP = {"ENERGIZED":"ENERGIZED","INITIAL":"INITIAL","ESTIMATED RESTORE":"ESTIMATED_RESTORE"}

def _canon_field(s:str)->str:
    up = re.sub(r'\s+',' ',s.strip()).upper()
    return FIELD_MAP.get(up, up.replace(' ','_'))

def loc_date_handler(m: re.Match) -> Dict[str, Any]:
    loc_id = int(m.group("loc_id"))
    field = _canon_field(m.group("field"))
    if m.group("set_val") is not None:
        return {"cat":"LOCATION_DATE","action":"SET","field":field,"location_id":loc_id,"new_ts":_coerce_dt_maybe(m.group("set_val"))}
    if m.group("from_val") is not None or m.group("to_val") is not None:
        old_ts = _coerce_dt_maybe(m.group("from_val")); new_ts = _coerce_dt_maybe(m.group("to_val"))
        delta, flag = _delta_flag(old_ts, new_ts)
        meta = {"cat":"LOCATION_DATE","action":"CHANGED","field":field,"location_id":loc_id,"old_ts":old_ts,"new_ts":new_ts,"delta_min":delta}
        if flag: meta["_flags"]=flag
        return meta
    return {"cat":"LOCATION_DATE","action":"REMOVED","field":field,"location_id":loc_id}

RULE_LOC_DATE = Rule("Location Date Field (HIS)", 15, LOC_DATE_DETECT, LOC_DATE_EXTRACT, loc_date_handler)


## LOCATION - Cause / occurance set removed (updated) 
LOC_CODE_DETECT = re.compile(
    r'(?i)^\s*His\s+Location\s*\[\s*\d+\s*\]\s*(?:Cause|Occurn)\s+has\s+been\s+(?:set\s+to|removed)\b'
)
LOC_CODE_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*His\s+Location\s*\[\s*(?P<loc_id>\d+)\s*\]\s*
    (?P<which>Cause|Occurn)\s+has\s+been\s+
    (?:
        set\s+to\s*\[\s*(?P<val>.*?)\s*\]
      | removed
    )\s*$
    '''
)
def loc_code_handler(m: re.Match) -> Dict[str, Any]:
    which = m.group("which").upper()
    if m.group("val") is not None:
        return {"cat":"LOCATION_CODE","kind":f"{which}_SET","location_id":int(m.group("loc_id")),"value":m.group("val").strip()}
    return {"cat":"LOCATION_CODE","kind":f"{which}_REMOVED","location_id":int(m.group("loc_id"))}

RULE_LOC_CODE = Rule("Location Code Set/Removed (HIS)", 16, LOC_CODE_DETECT, LOC_CODE_EXTRACT, loc_code_handler)


## IncidentDevice - Boolean (planned/isolated/total-loss)  (device flag set -- NEW) 
DEV_FLAG_DETECT = re.compile(
    r'(?i)^\s*his\s+IncidentDevice\s*\[\s*\d+\s*\]\s*Set\s+(?:Planned\s+outage|Isolated\s+to\s+Customer|Total\s+loss\s+of\s+power)\s+flag\s+to\s+(?:TRUE|FALSE)\b'
)
DEV_FLAG_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*his\s+IncidentDevice\s*\[\s*(?P<dev>\d+)\s*\]\s*
    Set\s+(?P<flag>Planned\s+outage|Isolated\s+to\s+Customer|Total\s+loss\s+of\s+power)\s+flag\s+to\s+(?P<val>TRUE|FALSE)\s*$
    '''
)
FLAG_MAP = {
  "PLANNED OUTAGE":"PLANNED_OUTAGE",
  "ISOLATED TO CUSTOMER":"ISOLATED_TO_CUSTOMER",
  "TOTAL LOSS OF POWER":"TOTAL_LOSS_OF_POWER",
}
def dev_flag_handler(m: re.Match)->Dict[str,Any]:
    f = FLAG_MAP[re.sub(r'\s+',' ',m.group('flag').strip()).upper()]
    return {"cat":"DEVICE","kind":"FLAG_SET","device_id":int(m.group('dev')),"flag":f,"value":m.group('val').upper()=="TRUE"}

RULE_DEV_FLAG = Rule("Device Flag Set (HIS)", 18, DEV_FLAG_DETECT, DEV_FLAG_EXTRACT, dev_flag_handler)


## 4. IncidentDevice - Downstream (updatd/qty change)  (his incident device change downstream cust) 
DEV_DS_DETECT = re.compile(
    r'(?i)^\s*his\s+IncidentDevice\s*\[\s*\d+\s*\]\s*(?:History\s+Downstream\s+has\s+been\s+updated|Change\s+downstream\s+customer\s+quantity\s+from)\b'
)
DEV_DS_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*his\s+IncidentDevice\s*\[\s*(?P<dev>\d+)\s*\]\s*
    (?:
        History\s+Downstream\s+has\s+been\s+updated\.?
      |
        Change\s+downstream\s+customer\s+quantity\s+from\s*\[\s*(?P<old>\d+)\s*\]\s*to\s*\[\s*(?P<new>\d+)\s*\]
    )\s*$
    '''
)
def dev_ds_handler(m: re.Match)->Dict[str,Any]:
    if m.group('old'):
        old=int(m.group('old')); new=int(m.group('new'))
        return {"cat":"DEVICE","kind":"DOWNSTREAM_QTY_CHANGED","device_id":int(m.group('dev')),"old":old,"new":new,"delta":new-old}
    return {"cat":"DEVICE","kind":"HISTORY_DOWNSTREAM_UPDATED","device_id":int(m.group('dev'))}

RULE_DEV_DS = Rule("Device Downstream (HIS)", 19, DEV_DS_DETECT, DEV_DS_EXTRACT, dev_ds_handler)


## 5 IncidentDevice - Date/time change on device record  (NEW - change initial date from a to b)
DEV_DATE_DETECT = re.compile(
    r'(?i)^\s*His\s+IncidentDevice\s*\[\s*\d+\s*\]\s*-\s*Changed\s+(?:Initial)\s+date\s+time\s+from\b'
)
DEV_DATE_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*His\s+IncidentDevice\s*\[\s*(?P<dev>\d+)\s*\]\s*-\s*
    Changed\s+(?P<field>Initial)\s+date\s+time\s+from\s*\[\s*(?P<old>.*?)\s*\]\s*to\s*\[\s*(?P<new>.*?)\s*\]\s*$
    '''
)
def dev_date_handler(m: re.Match)->Dict[str,Any]:
    old=_coerce_dt_maybe(m.group('old')); new=_coerce_dt_maybe(m.group('new'))
    delta, flag = _delta_flag(old,new)
    meta = {"cat":"DEVICE_DATE","kind":"CHANGED","field":"INITIAL","device_id":int(m.group('dev')),"old_ts":old,"new_ts":new,"delta_min":delta}
    if flag: meta["_flags"]=flag
    return meta

RULE_DEV_DATE = Rule("Device Date Changed (HIS)", 20, DEV_DATE_DETECT, DEV_DATE_EXTRACT, dev_date_handler)


## 6 History Incident routing (Routine/Non-routine/Combined)  (history routing NEW changed / moved to / combined)
HIST_ROUTE_DETECT = re.compile(
    r'(?i)^\s*(?:History\s+Incident|His\s+incident)\b'
)
HIST_ROUTE_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*
    (?:
        History\s+Incident(?:\s*\[\s*(?P<inc>\d+)\s*\])?\s+is\s+(?:changed\s+to|moved\s+to)\s+(?P<state>Routine|Non-?Routine)\.?
      |
        His\s+incident\s*-\s*has\s+been\s+combined\s+in\s+history\s+to\s+incident\s+(?P<target>\d+)
    )
    \s*$
    '''
)
def hist_route_handler(m: re.Match)->Dict[str,Any]:
    if m.group('state'):
        st = re.sub(r'\s+',' ',m.group('state').strip()).upper().replace('-','_')
        return {"cat":"HISTORY","kind":"INCIDENT_STATE","incident_id": int(m.group('inc')) if m.group('inc') else None,"state":st}
    return {"cat":"HISTORY","kind":"INCIDENT_COMBINED","target_incident_id":int(m.group('target'))}

RULE_HIST_ROUTE = Rule("History Routing (HIS)", 12, HIST_ROUTE_DETECT, HIST_ROUTE_EXTRACT, hist_route_handler)


## 7 Incident - apply all locations ( repair / cause )   NEW applied to all locations..
INC_APPLY_ALL_DETECT = re.compile(
    r'(?i)^\s*His\s+Incident\s*\[\s*\d+\s*\]\s*(?:Repair|Cause)\s*\['
)
INC_APPLY_ALL_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*His\s+Incident\s*\[\s*(?P<inc>\d+)\s*\]\s*
    (?P<attr>Repair|Cause)\s*\[\s*(?P<val>.*?)\s*\]\s*
    has\s+been\s+appl(?:y|ied)\s+to\s+all\s+locations\s*$
    '''
)
def inc_apply_all_handler(m: re.Match)->Dict[str,Any]:
    return {"cat":"INCIDENT_BULK_APPLY","incident_id":int(m.group('inc')),
            "attribute":m.group('attr').upper(),"value":m.group('val').strip(),"scope":"ALL_LOCATIONS"}

RULE_INC_APPLY_ALL = Rule("Incident Apply to All Locations (HIS)", 17, INC_APPLY_ALL_DETECT, INC_APPLY_ALL_EXTRACT, inc_apply_all_handler)


## 8 - Call - clue code changed ]]


CALL_CLUE_DETECT = re.compile(
    r'(?i)^\s*His\s+Call\s*\[\s*\d+\s*\]\s*Clue\s+Code\b'
)
CALL_CLUE_EXTRACT = re.compile(
    r'''(?ix)
    ^\s*His\s+Call\s*\[\s*(?P<call>\d+)\s*\]\s*
    Clue\s+Code\s+(?P<code>\d+)\s+has\s+been\s+change(?:d)?\s+from\s*\[\s*(?P<old>.*?)\s*\]\s*to\s*\[\s*(?P<new>.*?)\s*\]\s*$
    '''
)
def call_clue_handler(m: re.Match)->Dict[str,Any]:
    return {"cat":"CALL","kind":"CLUE_CODE_CHANGED","call_id":int(m.group('call')),
            "clue_code":int(m.group('code')),"old":m.group('old').strip(),"new":m.group('new').strip()}

RULE_CALL_CLUE = Rule("Call Clue Code Changed (HIS)", 21, CALL_CLUE_DETECT, CALL_CLUE_EXTRACT, call_clue_handler)





#### EVENT META UPDATER
HIS_PREFIX = re.compile(r'(?i)^\s*(his|history)\b')

def tag_with_layer(text: str) -> tuple[str, str | None]:
    if HIS_PREFIX.search(text or ""):
        return "HIS", None
    return "LIVE", None

def apply_rules(text: str, rules: list[Rule]) -> dict:
    s = (text or "").strip()
    layer, _ = tag_with_layer(s)          # <-- NEW
    for rule in sorted(rules, key=lambda r: r.priority):
        if rule.detect.search(s):
            flags = None
            meta = {}
            if rule.extract:
                m = rule.extract.search(s)
                if not m:
                    continue
                if rule.handler:
                    meta = rule.handler(m)
                    flags = meta.pop("_flags", None)
            meta.setdefault("layer", layer)  # <-- attach once
            return {"Tag": rule.name, "Flags": flags, "event_meta": meta}
    return {"Tag": None, "Flags": None, "event_meta": {"layer": layer}}

