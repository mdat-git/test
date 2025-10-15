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
