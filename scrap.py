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



import pandas as pd
import re

# ------------------------------
# 1. Sample iTOA Data
# ------------------------------
itoa_data = pd.DataFrame({
    "APP_NUM": [1, 2, 3, 4, 5],
    "REQ_WO_NUM": [
        "TD11111",                  
        "TD22222, TD33333",         
        "TD44444",                  
        "TD55555",                  
        "TD66666, TD77777, TD88888"
    ],
    "REQ_SCHED_START_DATE": pd.to_datetime([
        "2025-04-06", "2025-04-07", "2025-03-01", "2025-04-10", "2025-04-05"
    ]),
    "REQ_SCHED_END_DATE": pd.to_datetime([
        "2025-04-08", "2025-04-09", "2025-04-10", "2025-04-12", "2025-04-06"
    ])
})

# ------------------------------
# 2. Sample SAP Data
# ------------------------------
sap_data = pd.DataFrame({
    "Work Order #": ["TD11111", "TD33333", "TD55555", "TD88888", "TD44444"],
    "Date": pd.to_datetime([
        "2025-04-07", "2025-04-08", "2025-04-11", "2025-04-06", "2025-04-10"
    ])
})

# ------------------------------
# 3. Preprocess iTOA WO numbers
# ------------------------------
def preprocess_wo_number(df):
    input_col = df["REQ_WO_NUM"].astype(str)
    df_output = pd.DataFrame()
    df_output["wo_original"] = input_col
    df_output["APP_NUM"] = df["APP_NUM"]

    df_output["removed_dashes"] = df_output["wo_original"].apply(
        lambda x: x.replace(",", "").replace("-", " ") if len(x.replace(",", "").split("-")[0]) < 9 
        else x.replace(",", "").replace("-", "")
    )

    pattern = r"TD[ ]?\d{5,7}|90\d{7,8}|80\d{7,8}"  # Accepts 5–7 digits
    df_output["all_wo"] = df_output["removed_dashes"].str.upper().apply(
        lambda x: re.findall(pattern, x)
    )

    return df_output

# ------------------------------
# 4. Analysis Date Range
# ------------------------------
analysis_start = pd.to_datetime("2025-04-06")
analysis_end = pd.to_datetime("2025-04-12")

# Filter iTOA to those with start or end within range
mask = (
    (itoa_data["REQ_SCHED_START_DATE"].between(analysis_start, analysis_end)) |
    (itoa_data["REQ_SCHED_END_DATE"].between(analysis_start, analysis_end))
)
itoa_filtered = itoa_data[mask].copy()

# ------------------------------
# 5. Explode Work Orders
# ------------------------------
df_itoa_processed = preprocess_wo_number(itoa_filtered)
df_exploded = df_itoa_processed.explode("all_wo").drop_duplicates()
df_exploded = df_exploded.rename(columns={"all_wo": "REQ_WO_NUM_CLEAN"})

# Add REQ_SCHED_* columns back
df_exploded = df_exploded.merge(
    itoa_filtered[["APP_NUM", "REQ_SCHED_START_DATE", "REQ_SCHED_END_DATE"]],
    on="APP_NUM", how="left"
)

# ------------------------------
# 6. Create Program Start/End Rows
# ------------------------------
df_start = df_exploded.copy()
df_start["Program Start/End"] = "Program Start"
df_start["Time Analyzed"] = df_start["REQ_SCHED_START_DATE"]

df_end = df_exploded.copy()
df_end["Program Start/End"] = "Program End"
df_end["Time Analyzed"] = df_end["REQ_SCHED_END_DATE"]

df_long = pd.concat([df_start, df_end], ignore_index=True)

# ------------------------------
# 7. Merge with SAP + Fuzzy Date Match
# ------------------------------
df_joined = df_long.merge(sap_data, how="left", left_on="REQ_WO_NUM_CLEAN", right_on="Work Order #")
df_joined["Matched"] = (df_joined["Date"] - df_joined["Time Analyzed"]).abs() <= pd.Timedelta(days=2)

# Flag APP_NUM-level matches
match_summary = df_joined.groupby("APP_NUM")["Matched"].any().reset_index().rename(columns={"Matched": "App_Match_Found"})
df_final = df_joined.merge(match_summary, on="APP_NUM", how="left")

# ------------------------------
# 8. Final Filter: Time Within Analysis Window
# ------------------------------
df_final_filtered = df_final[df_final["Time Analyzed"].between(analysis_start, analysis_end)].copy()

# ------------------------------
# 9. Result
# ------------------------------
print("✅ Final Match Report:")
print(df_final_filtered[[
    "APP_NUM", "REQ_WO_NUM_CLEAN", "Program Start/End", "Time Analyzed",
    "Work Order #", "Date", "Matched", "App_Match_Found"
]])


## Update logic
# Step 1: Create Start and End filtered datasets
start_rows = df_exploded[df_exploded["REQ_SCHED_START_DATE"].between(analysis_start, analysis_end)].copy()
start_rows["Program Start/End"] = "Program Start"
start_rows["Time Analyzed"] = start_rows["REQ_SCHED_START_DATE"]

end_rows = df_exploded[df_exploded["REQ_SCHED_END_DATE"].between(analysis_start, analysis_end)].copy()
end_rows["Program Start/End"] = "Program End"
end_rows["Time Analyzed"] = end_rows["REQ_SCHED_END_DATE"]

# Step 2: Only keep these filtered rows for matching
df_long = pd.concat([start_rows, end_rows], ignore_index=True)

# Step 3: Merge with SAP
df_joined = df_long.merge(sap_data, how="left", left_on="REQ_WO_NUM_CLEAN", right_on="Work Order #")
df_joined["Matched"] = (df_joined["Date"] - df_joined["Time Analyzed"]).abs() <= pd.Timedelta(days=2)

# Step 4: Flag App_Match_Found at the APP_NUM level
match_summary = df_joined.groupby("APP_NUM")["Matched"].any().reset_index().rename(columns={"Matched": "App_Match_Found"})
df_final = df_joined.merge(match_summary, on="APP_NUM", how="left")

def extract_pure_sql(sas_code: str) -> str:
    lines = sas_code.splitlines()
    in_sql_block = False
    clean_lines = []

    for line in lines:
        stripped = line.strip()

        if stripped.lower().startswith("proc sql"):
            in_sql_block = True

        if in_sql_block:
            if not stripped.startswith("/*") and not stripped.startswith("%") and "ods " not in stripped.lower():
                clean_lines.append(line)

        if stripped.lower().startswith("quit;"):
            break

    return "\n".join(clean_lines)

## Even newer
import pandas as pd
import re

# ------------------------------
# 1. Setup: Input Data
# ------------------------------
itoa_data = pd.DataFrame({
    "APP_NUM": [1, 2, 3, 4, 5],
    "REQ_WO_NUM": [
        "TD11111",
        "TD22222, TD33333",
        "TD44444",
        "TD55555",
        "TD66666, TD77777, TD88888"
    ],
    "REQ_SCHED_START_DATE": pd.to_datetime([
        "2025-04-06", "2025-04-07", "2025-03-01", "2025-04-10", "2025-04-05"
    ]),
    "REQ_SCHED_END_DATE": pd.to_datetime([
        "2025-04-08", "2025-04-09", "2025-04-10", "2025-04-12", "2025-04-06"
    ])
})

sap_data = pd.DataFrame({
    "Work Order #": ["TD11111", "TD33333", "TD55555", "TD88888", "TD44444"],
    "Date": pd.to_datetime([
        "2025-04-07", "2025-04-08", "2025-04-11", "2025-04-06", "2025-04-10"
    ])
})

# ------------------------------
# 2. Config: Date Filter Window
# ------------------------------
analysis_start = pd.to_datetime("2025-04-06")
analysis_end = pd.to_datetime("2025-04-12")

# ------------------------------
# 3. Clean Work Order Numbers
# ------------------------------
def preprocess_wo_number(df):
    input_col = df["REQ_WO_NUM"].astype(str)
    df_output = pd.DataFrame()
    df_output["wo_original"] = input_col
    df_output["APP_NUM"] = df["APP_NUM"]

    df_output["removed_dashes"] = df_output["wo_original"].apply(
        lambda x: x.replace(",", "").replace("-", " ") if len(x.replace(",", "").split("-")[0]) < 9
        else x.replace(",", "").replace("-", "")
    )

    pattern = r"TD[ ]?\d{5,7}|90\d{7,8}|80\d{7,8}"
    df_output["all_wo"] = df_output["removed_dashes"].str.upper().apply(
        lambda x: re.findall(pattern, x)
    )

    return df_output

df_itoa_processed = preprocess_wo_number(itoa_data)
df_exploded = df_itoa_processed.explode("all_wo").drop_duplicates()
df_exploded = df_exploded.rename(columns={"all_wo": "REQ_WO_NUM_CLEAN"})

# ------------------------------
# 4. Reattach Scheduling Info
# ------------------------------
df_exploded = df_exploded.merge(
    itoa_data[["APP_NUM", "REQ_SCHED_START_DATE", "REQ_SCHED_END_DATE"]],
    on="APP_NUM", how="left"
)

# ------------------------------
# 5. Filter & Create Start/End Rows
# ------------------------------
start_rows = df_exploded[df_exploded["REQ_SCHED_START_DATE"].between(analysis_start, analysis_end)].copy()
start_rows["Program Start/End"] = "Program Start"
start_rows["Time Analyzed"] = start_rows["REQ_SCHED_START_DATE"]

end_rows = df_exploded[df_exploded["REQ_SCHED_END_DATE"].between(analysis_start, analysis_end)].copy()
end_rows["Program Start/End"] = "Program End"
end_rows["Time Analyzed"] = end_rows["REQ_SCHED_END_DATE"]

df_long = pd.concat([start_rows, end_rows], ignore_index=True)

# ------------------------------
# 6. Match with SAP (±2 Days)
# ------------------------------
df_joined = df_long.merge(sap_data, how="left", left_on="REQ_WO_NUM_CLEAN", right_on="Work Order #")
df_joined["Matched"] = (df_joined["Date"] - df_joined["Time Analyzed"]).abs() <= pd.Timedelta(days=2)
df_joined["Date_Matched"] = df_joined.apply(
    lambda row: row["Date"] if row["Matched"] else pd.NaT, axis=1
)

# ------------------------------
# 7. Flag APP_NUM-Level Match
# ------------------------------
match_summary = df_joined.groupby("APP_NUM")["Matched"].any().reset_index().rename(columns={"Matched": "App_Match_Found"})
df_final = df_joined.merge(match_summary, on="APP_NUM", how="left")

# ------------------------------
# 8. Final Output
# ------------------------------
df_final_sorted = df_final.sort_values(["APP_NUM", "Program Start/End", "REQ_WO_NUM_CLEAN"])
final_report = df_final_sorted[[
    "APP_NUM", "Program Start/End", "Time Analyzed",
    "REQ_WO_NUM_CLEAN", "Work Order #", "Date_Matched",
    "Matched", "App_Match_Found"
]]

# Print or export
print(final_report)
# You can also export to Excel: final_report.to_excel("itoa_sap_match_report.xlsx", index=False)

