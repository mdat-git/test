# Group by Outage ID, DataStatus, and DataAsOf to count steps
step_counts = (
    df.groupby(["DISTRB_OUTG_ID", "DataAsOf", "DataStatus"])["Step_Num"]
    .nunique()
    .reset_index(name="StepCount")
)

# Pivot to have 'pending' and 'validated' side by side
step_pivot = (
    step_counts.pivot_table(
        index=["DISTRB_OUTG_ID", "DataAsOf"],
        columns="DataStatus",
        values="StepCount"
    )
    .reset_index()
)

# Find where step count changed
step_pivot["StepCountDiff"] = (step_pivot["pending"] != step_pivot["validated"])

# Get just those outages where step count changed
step_changed = step_pivot[step_pivot["StepCountDiff"] == True]

# First, get distinct status per outage per snapshot
status_history = (
    df[["DISTRB_OUTG_ID", "DataAsOf", "DataStatus"]]
    .drop_duplicates()
    .sort_values(["DISTRB_OUTG_ID", "DataAsOf"])
)

# Group per outage and collect ordered status list
status_flows = (
    status_history
    .groupby("DISTRB_OUTG_ID")["DataStatus"]
    .apply(list)
    .reset_index(name="StatusSequence")
)

# Detect outages where status switches back and forth
def has_inflection(statuses):
    return any(
        statuses[i] != statuses[i+1]
        for i in range(len(statuses)-1)
    )

def flip_count(statuses):
    return sum(
        statuses[i] != statuses[i+1]
        for i in range(len(statuses)-1)
    )

status_flows["HasStatusFlip"] = status_flows["StatusSequence"].apply(has_inflection)
status_flows["FlipCount"] = status_flows["StatusSequence"].apply(flip_count)

# Filter if you want only outages with >1 flip
flipped_outages = status_flows[status_flows["FlipCount"] > 1]


import os
import pandas as pd
import re

# 📁 Path to your folder — update this to match your setup
folder_path = r"C:\path\to\your\archive"

# 🧾 Get list of all .csv files
csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

# 📦 Container for all data
all_data = []

for file in csv_files:
    full_path = os.path.join(folder_path, file)

    # 🗓️ Extract date from filename
    date_match = re.search(r"(\d{4}-\d{2}-\d{2})", file)
    if not date_match:
        print(f"Skipping file (no date found): {file}")
        continue
    data_date = pd.to_datetime(date_match.group(1))

    # 📌 Extract data status from filename
    if "pending" in file.lower():
        data_status = "pending"
    elif "validated" in file.lower():
        data_status = "validated"
    else:
        print(f"Skipping file (unknown status): {file}")
        continue

    try:
        df = pd.read_csv(full_path)

        # ➕ Add metadata columns
        df["DataAsOf"] = data_date
        df["DataStatus"] = data_status

        # 🧩 Append to list
        all_data.append(df)
    except Exception as e:
        print(f"Error reading {file}: {e}")

# 📊 Combine everything
combined_df = pd.concat(all_data, ignore_index=True)

# 📝 Optional: save for inspection
combined_df.to_csv("combined_outage_data_with_status.csv", index=False)


import os
import pandas as pd
import re

# 🔧 Set your directory (modify this as needed)
folder_path = r"C:\path\to\your\archive"  # Example: r"C:\Users\you\Documents\outage_validation\data\archive"

# 🧺 List all .csv files
csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

# 📦 Store individual dataframes
all_data = []

for file in csv_files:
    full_path = os.path.join(folder_path, file)

    # 🗓️ Extract date from filename using regex
    match = re.search(r"(\d{4}-\d{2}-\d{2})", file)
    if match:
        date_str = match.group(1)
    else:
        print(f"Date not found in filename: {file}")
        continue

    try:
        df = pd.read_csv(full_path)

        # 🧱 Add DataAsOf column
        df["DataAsOf"] = pd.to_datetime(date_str)

        # 🧩 Store
        all_data.append(df)
    except Exception as e:
        print(f"Failed to load {file}: {e}")

# 📊 Combine all into one dataframe
combined_df = pd.concat(all_data, ignore_index=True)

# 📝 Optional: save to file
combined_df.to_csv("combined_outage_data.csv", index=False)


import pandas as pd
import re

def preprocess_wo_number(df):
    input_col = df["REQ_WO_NUM"].astype(str)
    df_output = pd.DataFrame()
    df_output["wo_original"] = input_col

    # Remove dashes or dots based on segment length
    df_output["removed_dashes"] = df_output["wo_original"].apply(
        lambda x: x.replace(",", "").replace("-", " ") if len(x.replace(",", "").split("-")[0]) < 9 
        else x.replace(",", "").replace("-", "")
    )

    # Extract work order numbers using regex
    pattern = r"TD[ ]?\d{6,7}|90\d{7,8}|80\d{7,8}"
    df_output["all_wo"] = df_output["removed_dashes"].str.upper().apply(
        lambda x: re.findall(pattern, x)
    )

    return df_output

df_sap_current["ReceiverWorkOrderID"] = (
    df_sap_current["ReceiverWorkOrderID"]
    .str.lstrip("0")
    .str.replace(r"\.0*", "", regex=True)
)
