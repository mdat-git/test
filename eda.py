import re
from typing import Optional, Dict, Tuple

FLAGS = re.I  # case-insensitive

# ---------- ETR patterns ----------
# We try to capture: source (SYSTEM/MANUAL), action, final ETR datetime, and any free-text location.
ETR_PATTERNS = [
    # SYSTEM ETR set → final SYS ETR time
    (re.compile(
        r'\bSYSTEM\s+ETR\b.*?\bSET\s+ETR\b.*?\bTO\s+SYS\s+ETR\s+(?P<etr_ts>\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}(?::\d{2})?)',
        FLAGS),
     {"cat": "ETR", "sub": "SET", "source": "SYSTEM"}),

    # MANUAL ETR set → final MAN ETR time
    (re.compile(
        r'\bMANUAL\s+ETR\b.*?\bSET\s+ETR\b.*?\bTO\s+MAN\s+ETR\s*:?\s*(?P<etr_ts>\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}(?::\d{2})?)',
        FLAGS),
     {"cat": "ETR", "sub": "SET", "source": "MANUAL"}),

    # Disable ETR re-calculation (system or manual text often prefixes)
    (re.compile(
        r'\b(DISABLE|STOP)\s+ETR\s*RE-?CALC(ULATION)?\b',
        FLAGS),
     {"cat": "ETR", "sub": "DISABLE", "source": None}),

    # Generic SYSTEM ETR “change” lines → take the final SYS ETR
    (re.compile(
        r'\bSYSTEM\s+ETR\b.*?\bCHANGE\b.*?\bTO\s+SYS\s+ETR\s+(?P<etr_ts>\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}(?::\d{2})?)',
        FLAGS),
     {"cat": "ETR", "sub": "CHANGE", "source": "SYSTEM"}),

    # Fallback: any line mentioning ETR → try to grab a trailing date-time
    (re.compile(
        r'\bETR\b.*?(?P<etr_ts>\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}(?::\d{2})?)',
        FLAGS),
     {"cat": "ETR", "sub": "UNCLASSIFIED", "source": None}),
]

# ---------- CAUSE patterns ----------
# Capture a normalized "final" cause string inside brackets when present.
CAUSE_PATTERNS = [
    # New cause code recorded [XXXX]
    (re.compile(
        r'^New\s+cause\s+code\s+recorded\s*\[(?P<cause>[^\]]+)\]',
        FLAGS),
     {"cat": "CAUSE", "sub": "NEW"}),

    # Incident [id] Cause has been set to [XXXX] from CAD
    (re.compile(
        r'Cause\s+has\s+been\s+set\s+to\s*\[(?P<cause>[^\]]+)\]',
        FLAGS),
     {"cat": "CAUSE", "sub": "SET"}),

    # Cause has been apply to all locations (propagation)
    (re.compile(
        r'Cause\s+has\s+been\s+apply\s+to\s+all\s+locations',
        FLAGS),
     {"cat": "CAUSE", "sub": "APPLY_ALL"}),

    # Cause has been removed
    (re.compile(
        r'Cause\s+has\s+been\s+removed',
        FLAGS),
     {"cat": "CAUSE", "sub": "REMOVE"}),

    # Generic CAUSE line + bracket capture
    (re.compile(
        r'\bCAUSE\b.*?\[(?P<cause>[^\]]+)\]',
        FLAGS),
     {"cat": "CAUSE", "sub": "UNCLASSIFIED"}),
]

# ---------- AMI CALL patterns ----------
# Example: "Call reported at 2024/10/10 02:01:57 for Transformer [XXXXX] ... with AMI ESC METER"
AMI_PATTERNS = [
    (re.compile(
        r'^Call\s+reported\s+at\s+(?P<call_ts>\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}).*?\[(?P<asset>[0-9A-Z]+)\].*?\bAMI\b.*\bMETER\b',
        FLAGS),
     {"cat": "AMI_CALL", "sub": "REPORTED"}),
]

# ---------- Master dispatcher ----------
PATTERN_SETS = ETR_PATTERNS + CAUSE_PATTERNS + AMI_PATTERNS

def classify_event(text: Optional[str]) -> Tuple[str, Dict[str, Optional[str]]]:
    """
    Returns (label, extras) where label is one of:
      - ETR|CAUSE|AMI_CALL|OTHER
    'extras' may include: etr_ts, source, cause, call_ts, asset, sub
    """
    s = (text or "").strip()
    for rx, base in PATTERN_SETS:
        m = rx.search(s)
        if m:
            info = dict(base)  # copy
            # attach optional groups if present
            for k in ("etr_ts", "cause", "call_ts", "asset"):
                if k in m.re.groupindex:
                    info[k] = m.group(k)
            return info["cat"], info
    return "OTHER", {"cat": "OTHER", "sub": None}

# ---------- Vectorized tagging helper ----------
def tag_events(df, col="FOLLOWUP_DESC"):
    out = df[col].apply(lambda x: classify_event(x))
    df["event_cat"] = out.map(lambda t: t[0])
    df["event_meta"] = out.map(lambda t: t[1])  # dict per row (keep for debugging)
    # Lift common fields to columns for ease of use
    df["event_sub"]   = df["event_meta"].map(lambda d: d.get("sub"))
    df["etr_source"]  = df["event_meta"].map(lambda d: d.get("source"))
    df["etr_ts_txt"]  = df["event_meta"].map(lambda d: d.get("etr_ts"))
    df["cause_code"]  = df["event_meta"].map(lambda d: d.get("cause"))
    df["ami_call_ts"] = df["event_meta"].map(lambda d: d.get("call_ts"))
    df["ami_asset"]   = df["event_meta"].map(lambda d: d.get("asset"))
    return df



import pandas as pd

def fetch_single_incidents(cc, start_date: str, end_date: str):
    """
    Return a pandas DataFrame of INCIDENT_IDs that:
      - are in [start_date, end_date] by i."CREATION_DATETIME"
      - have exactly one location in HIS_LOCATION
      - exclude locations with OCCURN_DESC = 'NOT REPORTABLE/ALREADY REPORTED;'
    """
    sql = """
    SELECT
        i."INCIDENT_ID",
        i."CREATION_DATETIME",
        COUNT(DISTINCT l."LOCATION_ID") AS LOCATION_COUNT
    FROM "OMS"."HIS_INCIDENT" AS i
    JOIN "OMS"."HIS_LOCATION" AS l
      ON l."INCIDENT_ID" = i."INCIDENT_ID"
     AND COALESCE(l."OCCURN_DESC",'') <> ?
    WHERE i."CREATION_DATETIME" >= ?
      AND i."CREATION_DATETIME" <= ?
    GROUP BY i."INCIDENT_ID", i."CREATION_DATETIME"
    HAVING COUNT(DISTINCT l."LOCATION_ID") = 1
    ORDER BY i."CREATION_DATETIME"
    """
    params = ['NOT REPORTABLE/ALREADY REPORTED;', start_date, end_date]
    df = pd.read_sql_query(sql, cc.connection, params=params)
    # HANA will likely return INCIDENT_ID as float64 (DOUBLE). Make it int64 for clean merges.
    df['INCIDENT_ID'] = df['INCIDENT_ID'].astype('int64')
    return df





import pandas as pd

start_date   = '2025-10-10'
end_date     = '2025-10-14'
exclude_desc = 'NOT REPORTABLE/ALREADY REPORTED;'

# your SELECT list (already qualified with h.)
select_cols = """
  h."INCIDENT_ID",
  h."FOLLOWUP_ID",
  h."FOLLOWUP_DATETIME",
  h."FOLLOWUP_TYPE",
  h."FOLLOWUP_DESC",
  h."SYSTEM_OPID"
"""

sql = f"""
WITH single_incidents AS (
  SELECT i."INCIDENT_ID"
  FROM "OMS"."HIS_INCIDENT" AS i
  JOIN "OMS"."HIS_LOCATION" AS l
    ON l."INCIDENT_ID" = i."INCIDENT_ID"
   AND COALESCE(l."OCCURN_DESC",'') <> ?         -- param #1
  WHERE i."CREATION_DATETIME" >= ?               -- param #2
    AND i."CREATION_DATETIME" <= ?               -- param #3
  GROUP BY i."INCIDENT_ID"
  HAVING COUNT(DISTINCT l."LOCATION_ID") = 1
)
SELECT
  {select_cols}
FROM "OMS"."HIS_FOLLOWUP" AS h
JOIN single_incidents AS s
  ON s."INCIDENT_ID" = h."INCIDENT_ID"
"""

params = [exclude_desc, start_date, end_date]

# fast server-side count (no big fetch)
cnt = pd.read_sql_query(f'SELECT COUNT(*) AS N FROM ({sql}) q',
                        cc.connection, params=params).iloc[0,0]
print('[info] rows to fetch:', cnt)

# stream results (no parquet; process in memory by chunks)
for i, chunk in enumerate(pd.read_sql_query(sql, cc.connection,
                                            params=params, chunksize=100_000)):
    print(f'[info] chunk {i}: {len(chunk)} rows')
    # process chunk here (merge/agg/etc.)











from hana_ml.dataframe import ConnectionContext
import pandas as pd
from pathlib import Path
from typing import Iterable, Optional, List

def fetch_single_incident_followups(
    cc: ConnectionContext,
    start_date: str,           # 'YYYY-MM-DD'
    end_date: str,             # 'YYYY-MM-DD'  (inclusive end: use +1 day if you want half-open)
    incident_date_col: str = 'CREATE_DATE',  # change if your incident date col is different
    followup_cols: Optional[Iterable[str]] = None,  # columns to pull from HIS_FOLLOWUP
    chunksize: int = 100_000,
    parquet_dir: Optional[str] = None        # folder to write chunked Parquet; if None, returns an iterator
):
    """
    Stream HIS_FOLLOWUP rows for incidents in [start_date, end_date]
    that have exactly one location in HIS_LOCATION.

    Returns:
      - if parquet_dir is None: yields pandas DataFrame chunks
      - else: writes Parquet files to parquet_dir and yields their paths
    """
    # Default column subset from HIS_FOLLOWUP (adjust as needed)
    default_cols: List[str] = [
        '"INCIDENT_ID"',
        '"FOLLOWUP_TIME"',
        '"FOLLOWUP_DESC"',
        '"FOLLOWUP_TYPE"',
        '"CREATE_USER"',
        '"CREATE_DATE"'
    ]
    select_cols = ',\n    '.join(followup_cols or default_cols)

    # One SQL (CTEs): filter -> enforce single-location -> expand to followups
    sql = f"""
    WITH single_incidents AS (
        SELECT i."INCIDENT_ID"
        FROM "OMS"."HIS_INCIDENT" AS i
        JOIN "OMS"."HIS_LOCATION" AS l
          ON l."INCIDENT_ID" = i."INCIDENT_ID"
        WHERE i."{incident_date_col}" >= '{start_date}'
          AND i."{incident_date_col}" <= '{end_date}'
        GROUP BY i."INCIDENT_ID"
        HAVING COUNT(DISTINCT l."LOCATION_ID") = 1
    )
    SELECT
        {select_cols}
    FROM "OMS"."HIS_FOLLOWUP" AS h
    JOIN single_incidents AS s
      ON s."INCIDENT_ID" = h."INCIDENT_ID"
    """

    # Quick server-side sanity count (fast; no fetch of big rows)
    cnt = cc.sql(f"SELECT COUNT(*) AS N FROM ({sql}) q").collect().iloc[0,0]
    print(f"[info] rows to fetch: {cnt:,}")

    # Stream rows
    if parquet_dir:
        out = Path(parquet_dir); out.mkdir(parents=True, exist_ok=True)
        for i, chunk in enumerate(pd.read_sql_query(sql, cc.connection, chunksize=chunksize)):
            path = out / f"single_followup_{i:03}.parquet"
            chunk.to_parquet(path, index=False)
            print(f"[info] wrote {path.name}: {len(chunk):,} rows")
            yield str(path)
    else:
        for i, chunk in enumerate(pd.read_sql_query(sql, cc.connection, chunksize=chunksize)):
            print(f"[info] chunk {i}: {len(chunk):,} rows")
            yield chunk

# ---------- Example usage ----------
# cc = ConnectionContext(address="host", port=39015, user="USER", password="PWD", encrypt="true")
# for p in fetch_single_incident_followups(
#         cc, start_date="2024-10-10", end_date="2025-10-10",
#         incident_date_col="CREATE_DATE",
#         followup_cols=['"INCIDENT_ID"', '"FOLLOWUP_TIME"', '"FOLLOWUP_DESC"'],
#         chunksize=100_000,
#         parquet_dir="his_followup_single") :
#     pass  # files written; iterate again later to read/concat
#
# # Or if you want DataFrame chunks directly (no files):
# for df_chunk in fetch_single_incident_followups(cc, "2024-10-10", "2025-10-10", parquet_dir=None):
#     # process df_chunk
#     pass











import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# ---- 1) Configure columns and window ----
DATE_COL = 'start_dt'   # <-- change to your actual column (e.g., 'initial_date')
WINDOW_START = pd.Timestamp('2024-10-10')
WINDOW_END   = pd.Timestamp('2025-10-10')  # inclusive window end

# ---- 2) Parse datetime safely ----
df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors='coerce')

# (Optional) If your timestamps are UTC and you want local LA time:
# df[DATE_COL] = df[DATE_COL].dt.tz_localize('UTC').dt.tz_convert('America/Los_Angeles').dt.tz_localize(None)

# ---- 3) One row per incident (avoid counting multi-steps multiple times) ----
# Keep the earliest timestamp per incident as the outage occurrence time
incident_times = (
    df.dropna(subset=[DATE_COL])
      .groupby('incident_id', as_index=False)[DATE_COL]
      .min()
)

# ---- 4) Filter to the 1-year window ----
mask = (incident_times[DATE_COL] >= WINDOW_START) & (incident_times[DATE_COL] <= WINDOW_END)
incident_times = incident_times.loc[mask].copy()

# ---- 5) Daily counts (or switch to 'W' for weekly / 'MS' for monthly) ----
daily_counts = (
    incident_times
      .set_index(DATE_COL)
      .resample('D')
      .size()
      .rename('outages')
      .to_frame()
)

# ---- 6) Plot daily time series ----
plt.figure(figsize=(10, 4))
daily_counts['outages'].plot()
plt.title('Daily Outage Count (Oct 10, 2024 — Oct 10, 2025)')
plt.xlabel('Date')
plt.ylabel('Outages')
plt.tight_layout()
plt.show()

# ---- 7) (Optional) Monthly totals ----
monthly_counts = (
    incident_times
      .set_index(DATE_COL)
      .resample('MS')  # Month-start buckets
      .size()
      .rename('outages')
      .to_frame()
)

plt.figure(figsize=(8, 4))
monthly_counts['outages'].plot(kind='bar')
plt.title('Monthly Outage Count')
plt.xlabel('Month')
plt.ylabel('Outages')
plt.tight_layout()
plt.show()

# ---- 8) (Optional) Cumulative outages over the year ----
daily_counts['cum_outages'] = daily_counts['outages'].cumsum()
plt.figure(figsize=(10, 4))
daily_counts['cum_outages'].plot()
plt.title('Cumulative Outages (Oct 10, 2024 — Oct 10, 2025)')
plt.xlabel('Date')
plt.ylabel('Cumulative Outages')
plt.tight_layout()
plt.show()




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
