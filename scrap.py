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
