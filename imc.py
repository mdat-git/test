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
