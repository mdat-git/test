import re
import pandas as pd
import numpy as np

# --- CONFIG ---
DESC_COL = "FOLLOWUP_DESC"  # change if your column name differs

# --- SIMPLE TAG DETECTORS (fast routing) ---
SYSTEM_ETR_TAG = re.compile(r'(?i)^\s*SYSTEM\s+ETR\b')
MANUAL_ETR_TAG = re.compile(r'(?i)^\s*MANUAL\s+ETR\b')

# --- DETAILED EXTRACTORS (with named groups) ---

# Datetime is MM/DD/YYYY HH:MM[:SS]
DT = r'(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}(?::\d{2})?)'

SYSTEM_ETR_EXTRACTOR = re.compile(
    rf'''(?ix)                           # i: case-insensitive, x: verbose
    ^\s*SYSTEM\s+ETR-?\s*Set\s+ETR\s+for\s+@\s*
    (?P<loc>.+?)\s+                      # location (non-greedy)
    To\s+(?:(?P<to_type>SYS)\s+ETR|ETR\s+(?P<to_type_alt>SYS))\s*[:\-]?\s*
    (?P<to_dt>{DT})\s*$
    '''
)

MANUAL_ETR_EXTRACTOR = re.compile(
    rf'''(?ix)
    ^\s*MANUAL\s+ETR-?\s*Set\s+ETR\s+for\s+@\s*
    (?P<loc>.+?)\s+
    From\s+ETR\s+(?P<from_type>MAN|SYS)\s*[:\-]?\s*
    (?P<from_dt>{DT})\s+
    To\s+(?:(?P<to_type>MAN)\s+ETR|ETR\s+(?P<to_type_alt>MAN))\s*[:\-]?\s*
    (?P<to_dt>{DT})\s*$
    '''
)

def parse_etr(line: str):
    """
    Classify a line as SYSTEM ETR / MANUAL ETR / None and extract fields.
    Returns a dict with: tag, loc, from_type, from_dt, to_type, to_dt, flags
    """
    s = (line or "").strip()

    # SYSTEM ETR
    if SYSTEM_ETR_TAG.search(s):
        m = SYSTEM_ETR_EXTRACTOR.search(s)
        if m:
            to_type = m.group("to_type") or m.group("to_type_alt")
            return {
                "tag": "SYSTEM ETR",
                "loc": m.group("loc"),
                "from_type": None,
                "from_dt": None,
                "to_type": (to_type or "").upper() or "SYS",
                "to_dt": m.group("to_dt"),
                "flags": None,
            }
        else:
            return {"tag": "SYSTEM ETR", "loc": None, "from_type": None, "from_dt": None,
                    "to_type": None, "to_dt": None, "flags": "PARSE_FAIL"}

    # MANUAL ETR
    if MANUAL_ETR_TAG.search(s):
        m = MANUAL_ETR_EXTRACTOR.search(s)
        if m:
            to_type = m.group("to_type") or m.group("to_type_alt")
            out = {
                "tag": "MANUAL ETR",
                "loc": m.group("loc"),
                "from_type": (m.group("from_type") or "").upper(),
                "from_dt": m.group("from_dt"),
                "to_type": (to_type or "").upper() or "MAN",
                "to_dt": m.group("to_dt"),
                "flags": None,
            }
            # chronology sanity check
            try:
                fd = pd.to_datetime(out["from_dt"])
                td = pd.to_datetime(out["to_dt"])
                if pd.notna(fd) and pd.notna(td) and td < fd:
                    out["flags"] = "TO_BEFORE_FROM"
            except Exception:
                out["flags"] = (out["flags"] + "|DT_PARSE_ERR") if out["flags"] else "DT_PARSE_ERR"
            return out
        else:
            return {"tag": "MANUAL ETR", "loc": None, "from_type": None, "from_dt": None,
                    "to_type": None, "to_dt": None, "flags": "PARSE_FAIL"}

    # Not an ETR line we care about in this step
    return {"tag": None, "loc": None, "from_type": None, "from_dt": None,
            "to_type": None, "to_dt": None, "flags": None}

# --- APPLY TO YOUR DF ---

# Ensure text
df = df.copy()
df[DESC_COL] = df[DESC_COL].astype(str)

parsed = df[DESC_COL].apply(parse_etr).apply(pd.Series)

# Normalize and coerce datetimes
parsed["to_type"] = parsed["to_type"].str.upper().replace({"": np.nan})
parsed["from_type"] = parsed["from_type"].str.upper().replace({"": np.nan})
for c in ["from_dt", "to_dt"]:
    parsed[c] = pd.to_datetime(parsed[c], errors="coerce")

# Attach results
df["ETR_tag"] = parsed["tag"]
df["ETR_loc"] = parsed["loc"]
df["ETR_from_type"] = parsed["from_type"]
df["ETR_from_dt"] = parsed["from_dt"]
df["ETR_to_type"] = parsed["to_type"]
df["ETR_to_dt"] = parsed["to_dt"]
df["ETR_flags"] = parsed["flags"]

# Optional: filter to only the rows we tagged in this step
etr_rows = df[df["ETR_tag"].notna()]

# Quick sanity view
print(etr_rows[[DESC_COL, "ETR_tag", "ETR_loc", "ETR_from_type", "ETR_from_dt", "ETR_to_type", "ETR_to_dt", "ETR_flags"]].head(20))
