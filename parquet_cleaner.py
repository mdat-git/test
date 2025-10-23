import pandas as pd, numpy as np, json
import pyarrow as pa
import pyarrow.dataset as ds

def sanitize_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # 1) Ensure datetime
    dt_cols = [c for c in ["FOLLOWUP_DATETIME","INSERTED_DATE","t_start","t_end",
                           "t_completed","a_tail_end","t_archive_first","t_archive_last",
                           "t_b_start","t_b_end","t_c1_start","t_c2_start"] if c in out.columns]
    for c in dt_cols:
        out[c] = pd.to_datetime(out[c], errors="coerce")

    # 2) Fix numeric columns that might be mixed (int/str/float)
    def to_int64_nullable(s):
        return pd.to_numeric(s, errors="coerce").astype("Int64")

    num_candidates = [c for c in ["INCIDENT_ID","incident_id","b_start_idx","b_end_idx",
                                  "n_events_total","n_live","n_doc_qc",
                                  "n_A_steps","n_B_steps","n_C1_steps","n_C2_steps"] if c in out.columns]
    for c in num_candidates:
        out[c] = to_int64_nullable(out[c])

    # 3) Serialize complex/object columns (dict/list) to JSON strings
    complex_cols = []
    for c in out.columns:
        if out[c].dtype == "object":
            # if it already looks like plain text, keep as string; otherwise json-dump
            sample = out[c].dropna().head(1)
            if not sample.empty and isinstance(sample.iloc[0], (dict, list)):
                complex_cols.append(c)

    for c in complex_cols:
        out[c] = out[c].apply(lambda x: json.dumps(x, default=str) if pd.notna(x) else None)

    # 4) Cast remaining texty columns to pandas StringDtype (Arrow -> string/large_string)
    text_cols = [c for c in out.columns if out[c].dtype == "object"]
    for c in text_cols:
        out[c] = out[c].astype("string")

    return out

# ---------- prepare & write ----------
events = sanitize_for_parquet(a)  # or your events DataFrame
events["date_key"] = events["FOLLOWUP_DATETIME"].dt.strftime("%Y%m%d").astype("Int32")

# (Optional) Drop very large text you don't need in this table (saves space)
# events = events.drop(columns=["FOLLOWUP_DESC","event_meta"], errors="ignore")

table = pa.Table.from_pandas(events, preserve_index=False)
ds.write_dataset(
    table,
    base_dir="EventLogsLabeled_parquet/",  # folder (dataset), not a single file
    format="parquet",
    partitioning=["date_key"],             # creates date_key=YYYYMMDD/...
    existing_data_behavior="overwrite_or_ignore"
)
