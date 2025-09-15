
# Transition Frequency Analysis from metadata_change_log
# ------------------------------------------------------
# Expected columns: DISTRB_OUTG_ID, Field_Changed, Value_Pending, Value_Validated

import pandas as pd
import matplotlib.pyplot as plt

# 1) Filter to actual changes only
mcl_changes = metadata_change_log[
    metadata_change_log["Value_Pending"].astype(str) != metadata_change_log["Value_Validated"].astype(str)
].copy()

# 2) Build transition table
transition_counts = (
    mcl_changes.groupby(["Field_Changed", "Value_Pending", "Value_Validated"])
    .size()
    .reset_index(name="Count")
)

# 3) Compute % of changes within each field
transition_counts["Pct_of_Field"] = (
    transition_counts.groupby("Field_Changed")["Count"]
    .apply(lambda x: (x / x.sum() * 100).round(2))
)

# 4) Sort by field + count
transition_counts = transition_counts.sort_values(
    ["Field_Changed", "Count"], ascending=[True, False]
)

# Show top transitions per field
topN = 10
top_transitions = (
    transition_counts.groupby("Field_Changed")
    .head(topN)
    .reset_index(drop=True)
)

display(top_transitions.head(30))  # show a sample across fields

# 5) Optional: bar chart for a given field (e.g., CauseCode)
field = "CauseCode"
field_trans = top_transitions[top_transitions["Field_Changed"] == field]

plt.figure(figsize=(10,6))
plt.barh(
    [f"{p} → {v}" for p, v in zip(field_trans["Value_Pending"], field_trans["Value_Validated"])],
    field_trans["Count"]
)
plt.title(f"Top {topN} {field} Transitions", fontweight="bold")
plt.xlabel("Change Count")
plt.ylabel("Pending → Validated")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()





# Outputs:
# - Per-district summary table with core decision metrics
# - Three matplotlib charts (one per figure):
#     1) % < 26.5 by district
#     2) Qual-change rate by district
#     3) Anomaly count (>300k) by district
# - CSV export of the summary table

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from caas_jupyter_tools import display_dataframe_to_user

# ---------------- Config ----------------
LOW_RISK_THRESH = 26.5
ANOM_THRESH = 300_000
EXPORT_CSV = "/mnt/data/district_breakdown_summary.csv"

# ---------------- Prep (non-mutating) ----------------
df_cc = df.copy()

# Ensure DISTRICTNAME exists
if "DISTRICTNAME" not in df_cc.columns:
    raise KeyError("Expected column 'DISTRICTNAME' not found in df.")

# Ensure/compute d_CMI
if "d_CMI" not in df_cc.columns:
    if {"CMI_pending","CMI_validated"}.issubset(df_cc.columns):
        df_cc["d_CMI"] = pd.to_numeric(df_cc["CMI_validated"], errors="coerce") - pd.to_numeric(df_cc["CMI_pending"], errors="coerce")
    else:
        raise KeyError("No 'd_CMI' column and no ('CMI_pending','CMI_validated') to derive it from.")

# Coerce and create helpers
d = pd.to_numeric(df_cc["d_CMI"], errors="coerce")
df_cc = df_cc.assign(
    d_CMI=d,
    abs_d_CMI=np.abs(d),
    under_26_5=(d < LOW_RISK_THRESH),
    anomaly_300k=(np.abs(d) > ANOM_THRESH),
    qual_change_flag=np.where(df_cc.get("Count_Qual_Changes", 0).fillna(0).astype(float) > 0, 1, 0)
)

# Optional cause flip flag
if {"CauseCode_pending","CauseCode_validated"}.issubset(df_cc.columns):
    df_cc["cause_flip_flag"] = (df_cc["CauseCode_pending"].astype(str) != df_cc["CauseCode_validated"].astype(str)).astype(int)
else:
    df_cc["cause_flip_flag"] = np.nan

# ---------------- Aggregate per district ----------------
group = df_cc.groupby("DISTRICTNAME", dropna=False)
summary = pd.DataFrame({
    "N_Outages": group.size(),
    "Median_d_CMI": group["d_CMI"].median(),
    "P95_d_CMI": group["d_CMI"].quantile(0.95),
    "Pct_below_26_5": (group["under_26_5"].mean() * 100).round(2),
    "Anomaly_300k_Count": group["anomaly_300k"].sum().astype(int),
    "Qual_Change_Rate_pct": (group["qual_change_flag"].mean() * 100).round(2),
    "Cause_Flip_Rate_pct": (group["cause_flip_flag"].mean() * 100).round(2)
}).reset_index()

# Sort by safest (highest % < 26.5, then lowest qual-change rate, then N desc for impact)
summary = summary.sort_values(
    by=["Pct_below_26_5", "Qual_Change_Rate_pct", "N_Outages"],
    ascending=[False, True, False]
).reset_index(drop=True)

# Export & display
summary.to_csv(EXPORT_CSV, index=False)
display_dataframe_to_user("District Breakdown — Stability & Flip Summary", summary)

# ---------------- Plots (matplotlib: one chart per figure) ----------------
def barh_plot(df_in, xcol, title, xlabel, filename):
    plt.figure(figsize=(10, 7))
    ylabels = df_in["DISTRICTNAME"].astype(str).values
    y = np.arange(len(ylabels))
    x = df_in[xcol].values
    plt.barh(y, x)
    plt.yticks(y, ylabels)
    plt.title(title, fontweight="bold")
    plt.xlabel(xlabel)
    plt.tight_layout()
    path = f"/mnt/data/{filename}"
    plt.savefig(path, dpi=150)
    plt.show()
    return path

paths = {}
paths["pct_below_26_5"] = barh_plot(
    summary, "Pct_below_26_5",
    "Percent of Outages Below 26.5 by District",
    "% of outages below 26.5",
    "district_Pct_below_26_5.png"
)
paths["qual_change_rate"] = barh_plot(
    summary, "Qual_Change_Rate_pct",
    "Qualitative Change Rate by District (Count_Qual_Changes > 0)",
    "% of outages with ≥1 qualitative change",
    "district_Qual_Change_Rate_pct.png"
)
paths["anomaly_count"] = barh_plot(
    summary, "Anomaly_300k_Count",
    "Anomaly Count by District (|d_CMI| > 300k)",
    "Anomaly count",
    "district_Anomaly_300k_Count.png"
)

print("Saved artifacts:")
print(f"- Summary CSV: {EXPORT_CSV}")
for k, v in paths.items():
    print(f"- {k}: {v}")







# --- Config (tweak as needed) ---
ANOM_THRESH = 300_000
CATS = ["REGION_NAME", "DISTRICT", "SUBSTATION", "SWTCH_CNTR_DESC", "CKT_NAM"]
TOPN = 10  # how many bars to show

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# 1) Prep
df = df.copy()  # assumes df already exists in your notebook
df["abs_d_CMI"] = pd.to_numeric(df["d_CMI"], errors="coerce").abs()
anoms = df.loc[df["abs_d_CMI"] > ANOM_THRESH].copy()

print(f"Total outages: {len(df):,}  |  Anomalies (> {ANOM_THRESH:,}): {len(anoms):,} "
      f"({len(anoms)/len(df)*100:.2f}%)")

# 2) Summary tables: count + anomaly rate within each category
summary_tables = {}
for col in CATS:
    if col not in df.columns:
        continue
    base = df.groupby(col).size().rename("Total_Outages")
    a = anoms.groupby(col).size().rename("Anomaly_Count")
    tab = (
        pd.concat([base, a], axis=1)
          .fillna(0)
          .astype({"Total_Outages":"int", "Anomaly_Count":"int"})
          .assign(Anomaly_Rate_pct=lambda t: (t["Anomaly_Count"]/t["Total_Outages"]*100).round(2))
          .sort_values(["Anomaly_Count","Anomaly_Rate_pct"], ascending=[False, False])
    )
    summary_tables[col] = tab
    display(tab.head(15).style.format({"Anomaly_Rate_pct": "{:.2f}"}).set_caption(f"Anomaly Summary by {col}"))

# 3) Quick sns bar charts for a few key views
def plot_bar(table: pd.DataFrame, colname: str, metric: str = "Anomaly_Count", topn: int = TOPN):
    if table.empty or metric not in table.columns: 
        return
    top = table.sort_values(metric, ascending=False).head(topn).reset_index()
    plt.figure(figsize=(8,5))
    sns.barplot(data=top, x=metric, y=colname)
    plt.title(f"Top {topn} {colname} by {metric} (> {ANOM_THRESH:,} d_CMI)", fontweight="bold")
    plt.xlabel(metric.replace("_", " "))
    plt.ylabel(colname)
    plt.tight_layout()
    plt.show()

for col in ["REGION_NAME", "SUBSTATION", "CKT_NAM"]:
    if col in summary_tables:
        plot_bar(summary_tables[col], col, "Anomaly_Count", topn=TOPN)
