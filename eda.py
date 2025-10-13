import re

PAT_ENERGIZED_SET = re.compile(r"""
\b(?:his\s+)?location              # 'Location' or 'His Location'
\s*\[\s*(?P<loc_id>\d+)\s*\]       # [11111111111]
\s*energized\s*date\s*has\s*been\s*(?:set|updated)\s*to
\s*\[\s*(?P<dt>                    # capture the datetime
    (?:\d{1,2}/\d{1,2}/\d{2,4} | \d{4}-\d{2}-\d{2})
    \s+\d{1,2}:\d{2}:\d{2}
    (?:\s?(?:AM|PM))?
)\s*\]
""", re.IGNORECASE | re.VERBOSE)
