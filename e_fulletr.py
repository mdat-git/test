from __future__ import annotations

import re
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any, List

# -------------------------
# Normalization
# -------------------------
_WS = re.compile(r"\s+")
def normalize(text: Optional[str]) -> str:
    if not text:
        return ""
    return _WS.sub(" ", str(text)).strip()

# -------------------------
# Regex atoms tuned to your FOLLOWUP_DESC logs
# -------------------------
SRC  = r"(?P<source>SYSTEM|MANUAL)"
DASH = r"[-–—]"  # handles -, en-dash, em-dash

# IMPORTANT: LOC must NOT consume the whitespace before "To"/"From"
# (this was the bug causing SYSTEM lines not to tag)
LOC = r"(?:for\s+)?@\s*(?P<location>.+?)(?=\s*(?:\bFrom\b|\bTo\b|$))"

# Dates observed in your logs:
#  - 01/03/2024 23:00:00
#  - 2024/01/04 07:00:00  (planned job message)
DT_CORE = r"(?:\d{2}/\d{2}/\d{4}|\d{4}/\d{2}/\d{2})\s+\d{2}:\d{2}:\d{2}"
DT_FROM = rf"(?P<from_dt>{DT_CORE})"
DT_TO   = rf"(?P<to_dt>{DT_CORE})"

FROM_BLOCK = (
    rf"From\s+(?:ETR\s+)?"
    rf"(?:(?P<from_kind>SYS|MAN)\s*[-:]?\s*)?"
    rf"(?:(?P<from_null>-NULL)|-?{DT_FROM})"
)

TO_BLOCK = rf"To\s+(?P<to_kind>SYS|MAN)\s+ETR\s*:?\s*{DT_TO}"

# -------------------------
# Output schema
# -------------------------
@dataclass
class ETRTag:
    group: str = "ETR"
    source: Optional[str] = None
    action: str = ""  # set | change | disable_recalc
    location: Optional[str] = None
    from_kind: Optional[str] = None
    from_dt: Optional[str] = None
    from_is_null: bool = False
    to_kind: Optional[str] = None
    to_dt: Optional[str] = None
    pattern_name: Optional[str] = None
    raw_match: Optional[str] = None
    confidence: float = 0.9

    def asdict(self) -> Dict[str, Any]:
        d = asdict(self)
        for k, v in list(d.items()):
            if isinstance(v, str):
                d[k] = v.strip()
        if d.get("location"):
            d["location"] = normalize(d["location"])
        if d.get("source"):
            d["source"] = d["source"].upper()
        if d.get("from_kind"):
            d["from_kind"] = d["from_kind"].upper()
        if d.get("to_kind"):
            d["to_kind"] = d["to_kind"].upper()
        return d

# -------------------------
# Patterns (priority ordered)
# -------------------------
PATTERNS: List[Dict[str, Any]] = [
    # MANUAL ETR- Disable ETR Re-calculation for @ ...
    {
        "name": "disable_recalc",
        "action": "disable_recalc",
        "priority": 100,
        "regex": re.compile(
            rf"^\s*{SRC}\s+ETR\s*{DASH}\s*Disable\s+ETR\s+Re-calculation\s+{LOC}\s*\.?\s*$",
            re.IGNORECASE,
        ),
        "fields": ["source", "location"],
    },

    # SYSTEM ETR- Change for @ ... From -DT To SYS ETR DT
    {
        "name": "system_change_from_to",
        "action": "change",
        "priority": 95,
        "regex": re.compile(
            rf"^\s*{SRC}\s+ETR\s*{DASH}\s*Change\s+{LOC}\s+{FROM_BLOCK}\s+{TO_BLOCK}\s*\.?\s*$",
            re.IGNORECASE,
        ),
        "fields": ["source", "location", "from_kind", "from_null", "from_dt", "to_kind", "to_dt"],
    },

    # MANUAL ETR- Set ETR ... From ... To ...
    {
        "name": "set_with_from_to",
        "action": "set",
        "priority": 90,
        "regex": re.compile(
            rf"^\s*{SRC}\s+ETR\s*{DASH}\s*Set\s+ETR\s+{LOC}\s+{FROM_BLOCK}\s+{TO_BLOCK}\s*\.?\s*$",
            re.IGNORECASE,
        ),
        "fields": ["source", "location", "from_kind", "from_null", "from_dt", "to_kind", "to_dt"],
    },

    # SYSTEM ETR- Set ETR ... To SYS ETR ...
    {
        "name": "set_to_only",
        "action": "set",
        "priority": 80,
        "regex": re.compile(
            rf"^\s*{SRC}\s+ETR\s*{DASH}\s*Set\s+ETR\s+{LOC}\s+{TO_BLOCK}\s*\.?\s*$",
            re.IGNORECASE,
        ),
        "fields": ["source", "location", "to_kind", "to_dt"],
    },

    # Initial ETR for the Planned Job is 2024/01/04 07:00:00
    {
        "name": "planned_job_initial_etr",
        "action": "set",
        "priority": 70,
        "regex": re.compile(
            rf"^\s*(?P<source>SYSTEM|MANUAL)?\s*Initial\s+ETR\b.*?\bis\s+(?P<to_dt>{DT_CORE})\s*$",
            re.IGNORECASE,
        ),
        "fields": ["source", "to_dt"],
    },
]

PATTERNS.sort(key=lambda p: p["priority"], reverse=True)

# -------------------------
# Engine
# -------------------------
def tag_etr_event(text: str) -> Optional[Dict[str, Any]]:
    s = normalize(text)
    if not s:
        return None

    for spec in PATTERNS:
        m = spec["regex"].match(s)
        if not m:
            continue

        gd = {k: (m.group(k) if k in m.re.groupindex else None) for k in spec.get("fields", [])}

        tag = ETRTag(
            source=gd.get("source"),
            action=spec["action"],
            location=gd.get("location"),
            from_kind=gd.get("from_kind"),
            from_dt=gd.get("from_dt"),
            from_is_null=bool(gd.get("from_null")),
            to_kind=gd.get("to_kind"),
            to_dt=gd.get("to_dt"),
            pattern_name=spec["name"],
            raw_match=m.group(0),
            confidence=0.95 if spec["priority"] >= 90 else 0.9,
        )
        return tag.asdict()

    return None
