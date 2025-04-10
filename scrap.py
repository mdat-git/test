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




# 1. Merge
df_merged = df_long.merge(sap_data, how="left", left_on="REQ_WO_NUM_CLEAN", right_on="Work Order #")

# 2. Compute time delta
df_merged["TimeDelta"] = (df_merged["Date"] - df_merged["Time Analyzed"]).abs()

# 3. Filter to within 2-day window
df_windowed = df_merged[df_merged["TimeDelta"] <= pd.Timedelta(days=2)]

# 4. Keep only closest SAP match per row (based on APP_NUM + Program Start/End + WO)
deduped_idx = (
    df_windowed
    .groupby(["APP_NUM", "Program Start/End", "REQ_WO_NUM_CLEAN"])["TimeDelta"]
    .idxmin()
)

df_joined_best = df_windowed.loc[deduped_idx].copy()

# 5. Add match flags
df_joined_best["Matched"] = True
df_joined_best["Date_Matched"] = df_joined_best["Date"]

# 6. Fill in unmatched (i.e., rows that had no SAP match at all)
df_no_match = df_long.merge(
    df_joined_best[["APP_NUM", "Program Start/End", "REQ_WO_NUM_CLEAN"]],
    how="left",
    on=["APP_NUM", "Program Start/End", "REQ_WO_NUM_CLEAN"],
    indicator=True
).query("_merge == 'left_only'").drop(columns=["_merge"])

df_no_match["Matched"] = False
df_no_match["Date_Matched"] = pd.NaT

# 7. Final result
df_final = pd.concat([df_joined_best, df_no_match], ignore_index=True)






## EXPORT

import pandas as pd
import re
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font, PatternFill
from openpyxl import Workbook

# ------------------------------
# 2. Styled Excel Export Function
# ------------------------------
def export_styled_excel(df, filename="itoa_sap_report.xlsx"):
    wb = Workbook()
    ws = wb.active
    ws.title = "iTOA-SAP Match"

    # Write the DataFrame to the Excel sheet
    for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
        for c_idx, value in enumerate(row, 1):
            ws.cell(row=r_idx, column=c_idx, value=value)

    # Header styling
    header_fill = PatternFill(start_color="D9E1F2", fill_type="solid")
    bold_font = Font(bold=True)
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = bold_font

    # Identify key columns
    headers = list(df.columns)
    match_idx = headers.index("Matched")
    prog_type_idx = headers.index("Program Start/End")
    sched_start_idx = headers.index("REQ_SCHED_START_DATE")
    sched_end_idx = headers.index("REQ_SCHED_END_DATE")

    # Row formatting
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
        match_cell = row[match_idx]
        prog_type = row[prog_type_idx].value

        # Highlight unmatched
        if match_cell.value is False:
            for cell in row:
                cell.fill = PatternFill(start_color="FFF2CC", fill_type="solid")

        # Dim irrelevant schedule column
        if prog_type == "Program Start":
            row[sched_end_idx].font = Font(color="999999")  # dim END
        elif prog_type == "Program End":
            row[sched_start_idx].font = Font(color="999999")  # dim START

    # Autofit columns
    for col in ws.columns:
        max_len = max(len(str(cell.value)) if cell.value else 0 for cell in col)
        ws.column_dimensions[col[0].column_letter].width = max_len + 2

    wb.save(filename)
    print(f"✅ Exported to {filename}")
    return filename








{
    "name": "Modern Dashboard Fusion",
    "dataColors": [
        "#FF2D75",
        "#FFD6E5",
        "#61d87c",
        "#54c6e9",
        "#f6db5e",
        "#e95474",
        "#3183c8",
        "#9b6be8",
        "#f28b74",
        "#dbdcdc"
    ],
    "background": "#F8F8F8",
    "foreground": "#000000",
    "tableAccent": "#FF2D75",
    "visualStyles": {
        "*": {
            "*": {
                "color": [
                    {
                        "solid": {
                            "color": "#000000"
                        }
                    }
                ],
                "background": [
                    {
                        "solid": {
                            "color": "#FFFFFF"
                        }
                    }
                ]
            }
        }
    },
    "sentimentColors": [
        "#61d87c",
        "#f6db5e",
        "#e95474"
    ]
}{
    "name": "Modern Dashboard Fusion",
    "dataColors": [
        "#FF2D75",
        "#FFD6E5",
        "#61d87c",
        "#54c6e9",
        "#f6db5e",
        "#e95474",
        "#3183c8",
        "#9b6be8",
        "#f28b74",
        "#dbdcdc"
    ],
    "background": "#F8F8F8",
    "foreground": "#000000",
    "tableAccent": "#FF2D75",
    "visualStyles": {
        "*": {
            "*": {
                "color": [
                    {
                        "solid": {
                            "color": "#000000"
                        }
                    }
                ],
                "background": [
                    {
                        "solid": {
                            "color": "#FFFFFF"
                        }
                    }
                ]
            }
        }
    },
    "sentimentColors": [
        "#61d87c",
        "#f6db5e",
        "#e95474"
    ]
}
