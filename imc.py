# Thresholds (tune as needed)
CMI_threshold = 95     # % of outages with d_CMI < 26.5
Flip_threshold = 10    # % of outages with cause code flip

# Classify each district into quadrants
risk_summary["Category"] = risk_summary.apply(
    lambda r: (
        "Safe Skip" if r["Pct_CMI_Below_26.5"] >= CMI_threshold and r["Flip_Rate_pct"] <= Flip_threshold else
        "Cause Code Risk" if r["Pct_CMI_Below_26.5"] >= CMI_threshold and r["Flip_Rate_pct"] > Flip_threshold else
        "CMI Risk" if r["Pct_CMI_Below_26.5"] < CMI_threshold and r["Flip_Rate_pct"] <= Flip_threshold else
        "High Risk"
    ),
    axis=1
)

# Summary table
quadrant_table = (
    risk_summary.groupby("Category")["DISTRICTNAME"]
    .apply(list)
    .reset_index()
)

display(quadrant_table)



import matplotlib.pyplot as plt

# 1) Get per-district CMI stats
cmi_summary = (
    df_cc.groupby("DISTRICTNAME")
    .apply(lambda g: ( (g["d_CMI"].abs() < 26.5).sum() / g.shape[0] * 100 ).round(2))
    .reset_index(name="Pct_CMI_Below_26.5")
)

# 2) Merge with cause code flip rates
risk_summary = district_summary.merge(cmi_summary, on="DISTRICTNAME", how="left")

# 3) Scatter plot
plt.figure(figsize=(9,7))
plt.scatter(
    risk_summary["Pct_CMI_Below_26.5"],
    risk_summary["Flip_Rate_pct"],
    s=100, alpha=0.7
)

for _, row in risk_summary.iterrows():
    plt.text(row["Pct_CMI_Below_26.5"]+0.3, row["Flip_Rate_pct"]+0.3, row["DISTRICTNAME"], fontsize=8)

plt.axvline(95, color="green", linestyle="--", label="95% CMI below threshold")
plt.axhline(10, color="red", linestyle="--", label="10% cause code flip rate")
plt.xlabel("% outages with CMI < 26.5")
plt.ylabel("Cause code flip rate (%)")
plt.title("District Risk Map: CMI Stability vs Cause Code Flips", fontweight="bold")
plt.legend()
plt.tight_layout()
plt.show()



# All-field transition frequency
all_transitions = (
    metadata_change_log[
        metadata_change_log["Value_Pending"].astype(str) != metadata_change_log["Value_Validated"].astype(str)
    ]
    .groupby(["Field_Changed","Value_Pending","Value_Validated"])
    .size()
    .reset_index(name="Count")
    .sort_values("Count", ascending=False)
)

# % of changes within each field
all_transitions["Pct_of_Field"] = (
    all_transitions["Count"] / all_transitions.groupby("Field_Changed")["Count"].transform("sum") * 100
).round(2)

display(all_transitions.head(20))  # top 20 across all fields



# District-level CMI_delta analysis
# ---------------------------------
# Assumes df_cc has:
#   - 'DISTRICTNAME'
#   - 'CMI_delta' (numeric)

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Config
MATERIAL_THRESH = 26.5

# Prep
df_cc = df_cc.copy()
df_cc["abs_delta"] = pd.to_numeric(df_cc["CMI_delta"], errors="coerce").abs()
df_cc["material_change"] = df_cc["abs_delta"] > MATERIAL_THRESH
df_cc["any_change"] = df_cc["abs_delta"] > 0

# District-level summary
district_summary = (
    df_cc.groupby("DISTRICTNAME")
    .agg(
        N_Outages=("CMI_delta","size"),
        N_Material=("material_change","sum"),
        N_AnyChange=("any_change","sum"),
        Total_Abs_Delta=("abs_delta","sum"),
        Median_Abs_Delta=("abs_delta","median"),
        P95_Abs_Delta=("abs_delta", lambda x: np.percentile(x,95))
    )
    .reset_index()
)

# Add rates
district_summary["Pct_Material"] = (district_summary["N_Material"] / district_summary["N_Outages"] * 100).round(2)
district_summary["Pct_AnyChange"] = (district_summary["N_AnyChange"] / district_summary["N_Outages"] * 100).round(2)

# Sort for plotting
district_summary_sorted = district_summary.sort_values("Pct_Material", ascending=False)

display(district_summary_sorted.head(20))

# --- Chart 1: % of outages with material change (>26.5)
plt.figure(figsize=(9,6))
plt.barh(district_summary_sorted["DISTRICTNAME"], district_summary_sorted["Pct_Material"])
plt.xlabel("% outages with |CMI_delta| > 26.5")
plt.ylabel("District")
plt.title("Material CMI Changes by District", fontweight="bold")
plt.tight_layout()
plt.show()

# --- Chart 2: Total absolute CMI delta by district
district_summary_vol = district_summary.sort_values("Total_Abs_Delta", ascending=False)

plt.figure(figsize=(9,6))
plt.barh(district_summary_vol["DISTRICTNAME"], district_summary_vol["Total_Abs_Delta"])
plt.xlabel("Total |CMI_delta| (sum across outages)")
plt.ylabel("District")
plt.title("Total CMI Delta Volume by District", fontweight="bold")
plt.tight_layout()
plt.show()



# Cause Code Audit Log Processor
# ----------------------------------------------------
# Inputs:
#   - df_cc : outage-level dataframe with ['DISTRB_OUTG_ID','DISTRICTNAME']
#   - metadata_change_log : with ['DISTRB_OUTG_ID','Field_Changed','Value_Pending','Value_Validated']
# 
# Output:
#   - Transition tables per RMI level (3, 6, 19)
#   - District-level flip summary

import pandas as pd
import matplotlib.pyplot as plt

# 1) Filter to cause code fields
cause_fields = ["CauseCode_RMI3", "CauseCode_RMI6", "CauseCode_RMI19"]
cause_log = metadata_change_log[metadata_change_log["Field_Changed"].isin(cause_fields)].copy()

# 2) Keep only real flips
cause_log = cause_log[
    cause_log["Value_Pending"].astype(str) != cause_log["Value_Validated"].astype(str)
].copy()

# 3) Transition counts per level
transition_tables = {}
for level in cause_fields:
    tab = (
        cause_log[cause_log["Field_Changed"] == level]
        .groupby(["Value_Pending","Value_Validated"])
        .size()
        .reset_index(name="Count")
        .sort_values("Count", ascending=False)
    )
    # % of total for this level
    tab["Pct_of_Level"] = (tab["Count"] / tab["Count"].sum() * 100).round(2)
    transition_tables[level] = tab
    print(f"\nTop transitions for {level}:")
    display(tab.head(10))

# 4) Flip counts by district
#    (merge with outage-level df to attach DISTRICTNAME)
cause_flips = cause_log[["DISTRB_OUTG_ID","Field_Changed"]].drop_duplicates()
district_summary = (
    cause_flips.merge(df_cc[["DISTRB_OUTG_ID","DISTRICTNAME"]], on="DISTRB_OUTG_ID", how="left")
    .groupby("DISTRICTNAME")
    .size()
    .reset_index(name="Cause_Flip_Count")
)

# Add total outages per district
district_summary = district_summary.merge(
    df_cc.groupby("DISTRICTNAME").size().reset_index(name="Total_Outages"),
    on="DISTRICTNAME",
    how="left"
)

# Flip rate %
district_summary["Flip_Rate_pct"] = (
    district_summary["Cause_Flip_Count"] / district_summary["Total_Outages"] * 100
).round(2)

# Sort safest → riskiest
district_summary = district_summary.sort_values("Flip_Rate_pct", ascending=True).reset_index(drop=True)

print("\nDistrict-level cause code flip summary:")
display(district_summary)

# 5) Quick bar chart: flip rate by district
plt.figure(figsize=(8,5))
plt.barh(district_summary["DISTRICTNAME"], district_summary["Flip_Rate_pct"])
plt.xlabel("% outages with cause code flip")
plt.ylabel("District")
plt.title("Cause Code Flip Rate by District", fontweight="bold")
plt.tight_layout()
plt.show()






# 1) Filter to actual changes only
mcl_changes = metadata_change_log[
    metadata_change_log["Value_Pending"].astype(str)
    != metadata_change_log["Value_Validated"].astype(str)
].copy()

# 2) Transition counts
transition_counts = (
    mcl_changes
    .groupby(["Field_Changed", "Value_Pending", "Value_Validated"], dropna=False)
    .size()
    .reset_index(name="Count")
)

# 3) % of field (index-safe with transform)
transition_counts["Pct_of_Field"] = (
    transition_counts["Count"]
    / transition_counts.groupby("Field_Changed")["Count"].transform("sum")
    * 100
).round(2)

# 4) Sort and get top N per field
transition_counts = transition_counts.sort_values(
    ["Field_Changed", "Count"], ascending=[True, False]
)
topN = 10
top_transitions = (
    transition_counts
    .groupby("Field_Changed", group_keys=False)
    .head(topN)
    .reset_index(drop=True)
)

# 5) Optional: flag dominant transitions (e.g., >50% of a field’s changes)
dominant_threshold = 50.0
top_transitions["Dominant"] = top_transitions["Pct_of_Field"] >= dominant_threshold

display(top_transitions.head(30))

import matplotlib.pyplot as plt

field = "CauseCode"  # change as needed
field_trans = top_transitions[top_transitions["Field_Changed"] == field].copy()

plt.figure(figsize=(10, 6))
labels = [f"{p} → {v}" for p, v in zip(field_trans["Value_Pending"], field_trans["Value_Validated"])]
plt.barh(labels, field_trans["Count"])
plt.title(f"Top {topN} {field} transitions", fontweight="bold")
plt.xlabel("Change count")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()

matrix = (
    mcl_changes[mcl_changes["Field_Changed"] == field]
    .pivot_table(index="Value_Pending", columns="Value_Validated", values="DISTRB_OUTG_ID", aggfunc="count", fill_value=0)
)
display(matrix)






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
