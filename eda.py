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
