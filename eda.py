import re

PAT_ENERGIZED_SET = re.compile(r"""
(?ix)
\b(?:his\s+)?location      # 'Location' or 'His Location'
\s*\[\s*(?P<loc_id>\d+)\s*\]   # [11111111111]
\s*energized\s*date\s*has\s*been\s*set\s*to
\s*\[\s*(?P<dt>              # capture the datetime
    (?:\d{1,2}/\d{1,2}/\d{2,4})      # MM/DD/YYYY (or M/D/YY)
    \s+\d{1,2}:\d{2}:\d{2}           # HH:MM:SS (24h or with AM/PM below)
    (?:\s?(?:AM|PM))?                # optional AM/PM
    |                                # OR ISO-ish
    \d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}
)\s*\]
""")
