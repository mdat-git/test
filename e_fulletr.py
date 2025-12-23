# =============================================================
# ETR Regex Tagger — log-faithful OMS version
# Paste into ONE Jupyter cell and run
# =============================================================

from __future__ import annotations

import re
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any, List

# =============================================================
# Normalization
# =============================================================

_WS = re.compile(r"\s+")

def normalize(text: Optional[str]) -> str:
    """Collapse all whitespace (incl. newlines) into single spaces."""
    if not text:
        return ""
    return _WS.sub(" ", str(text)).strip()

# =============================================================
# Core regex atoms (based ONLY on your logs)
# =============================================================

# SYSTEM ETR- ... or MANUAL ETR- ...
SRC = r"(?P<source>SYSTEM|MANUAL)"

# Location text after "for @"
LOC = r"for\s+@\s*(?P<location>.+?)\s*(?=(?:\bFrom\b|\bTo\b|$))"

# Datetime format in logs: MM/DD/YYYY HH:MM:SS
DT = r"(?P<dt>\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})"
DT_FROM = DT.replace("dt", "from_dt")
DT_TO   = DT.replace("dt", "to_dt")

# FROM block variants:
#   From ETR -NULL
#   From ETR MAN-06/04/2025 15:00:00
#   From ETR SYS-06/04/2025 06:00:00
#   From -06/04/2025 03:30:00
FROM_BLOCK = (
    rf"From\s+(?:ETR\s+)?"
    rf"(?:(?P<from_kind>SYS|MAN)\s*[-:]?\s*)?"
    rf"(?:(?P<from_null>-NULL)|-{DT_FROM}|{DT_FROM})"
)

# TO block:
#   To SYS ETR 06/03/2025 21:30:00
#   To MAN ETR : 06/04/2025 15:00:00
TO_BLOCK = (
    rf"To\s+(?P<to_kind>SYS|MAN)\s+ETR\s*:?\s*{DT_TO}"
)

# =============================================================
# Tag schema
# =============================================================

@dataclass
class ETRTag:
    group: str = "ETR"
    source: Optional[str] = None      # SYSTEM | MANUAL
    action: str = ""                  # set | change | disable_recalc
    location: Optional[str] = None
    from_kind: Optional[str] = None   # SYS | MAN
    from_dt: Optional[str] = None
    from_is_null: bool = False
    to_kind: Optional[str] = None     # SYS | MAN
    to_dt: Optional[str] = None
    pattern_name: Optional[str] = None
    raw_match: Optional[str] = None
    confidence: float = 0.9

    def asdict(self) -> Dict[str, Any]:
        d = asdict(self)
        for k in d:
            if isinstance(d[k], str):
                d[k] = d[k].strip()
        if d.get("location"):
            d["location"] = normalize(d["location"])
        if d.get("source"):
            d["source"] = d["source"].upper()
        if d.get("from_kind"):
            d["from_kind"] = d["from_kind"].upper()
        if d.get("to_kind"):
            d["to_kind"] = d["to_kind"].upper()
        return d

# =============================================================
# Patterns (priority ordered)
# =============================================================

PATTERNS: List[Dict[str, Any]] = [

    # 1) MANUAL ETR- Disable ETR Re-calculation
    {
        "name": "disable_recalc",
        "action": "disable_recalc",
        "priority": 100,
        "regex": re.compile(
            rf"^{SRC}\s+ETR\s*-\s*Disable\s+ETR\s+Re-calculation\s+{LOC}\s*$",
            re.IGNORECASE,
        ),
        "fields": ["source", "location"],
    },

    # 2) SYSTEM ETR- Change ... From ... To ...
    {
        "name": "system_change_from_to",
        "action": "change",
        "priority": 95,
        "regex": re.compile(
            rf"^{SRC}\s+ETR\s*-\s*Change\s+{LOC}\s+{FROM_BLOCK}\s+{TO_BLOCK}\s*$",
            re.IGNORECASE,
        ),
        "fields": ["source", "location", "from_kind", "from_null", "from_dt", "to_kind", "to_dt"],
    },

    # 3) SYSTEM / MANUAL ETR- Set ETR ... From ... To ...
    {
        "name": "set_with_from_to",
        "action": "set",
        "priority": 90,
        "regex": re.compile(
            rf"^{SRC}\s+ETR\s*-\s*Set\s+ETR\s+{LOC}\s+{FROM_BLOCK}\s+{TO_BLOCK}\s*$",
            re.IGNORECASE,
        ),
        "fields": ["source", "location", "from_kind", "from_null", "from_dt", "to_kind", "to_dt"],
    },

    # 4) SYSTEM ETR- Set ETR ... To SYS ETR ...
    {
        "name": "system_set_to_only",
        "action": "set",
        "priority": 80,
        "regex": re.compile(
            rf"^{SRC}\s+ETR\s*-\s*Set\s+ETR\s+{LOC}\s+{TO_BLOCK}\s*$",
            re.IGNORECASE,
        ),
        "fields": ["source", "location", "to_kind", "to_dt"],
    },
]

PATTERNS.sort(key=lambda p: p["priority"], reverse=True)

# =============================================================
# Engine
# =============================================================

def tag_etr_event(text: str) -> Optional[Dict[str, Any]]:
    s = normalize(text)
    if not s:
        return None

    for spec in PATTERNS:
        m = spec["regex"].match(s)
        if not m:
            continue

        gd = {k: (m.group(k) if k in m.re.groupindex else None)
              for k in spec.get("fields", [])}

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
