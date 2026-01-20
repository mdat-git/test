from __future__ import annotations

import os
import re
import uuid
import shutil
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd


# ----------------------------
# Config
# ----------------------------

GCP_DAILY_DIR = Path("./gcp/dailysnapshot")
LAKEHOUSE_DIR = Path("./LAKEHOUSE")

UTIL_DIR = LAKEHOUSE_DIR / "util"
BRONZE_DIR = LAKEHOUSE_DIR / "bronze"
SILVER_DIR = LAKEHOUSE_DIR / "silver"

PIPELINE_NAME = "deliveries_ytd"          # stable pipeline identifier
SOURCE_SYSTEM = "GCP_EMAIL_EXPORT"        # whatever you want here

# Logical dataset naming in storage
SUBJECT = "aerial_ytd_deliveries"         # folder grouping under bronze/silver

# Key columns (aggregate grain)
KEY_COLS = ["dataset_type", "vendor", "structureId", "folder"]


# ----------------------------
# Helpers
# ----------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def ensure_dirs() -> None:
    for d in [UTIL_DIR, BRONZE_DIR, SILVER_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    (BRONZE_DIR / SUBJECT / "raw_files").mkdir(parents=True, exist_ok=True)
    (BRONZE_DIR / SUBJECT / "current").mkdir(parents=True, exist_ok=True)
    (SILVER_DIR / SUBJECT).mkdir(parents=True, exist_ok=True)

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def atomic_write_csv(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(out_path)

def append_csv_row(row: dict, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([row])
    if out_path.exists():
        df.to_csv(out_path, mode="a", header=False, index=False)
    else:
        df.to_csv(out_path, mode="w", header=True, index=False)

def read_csv_if_exists(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()

def parse_filename_metadata(file_name: str) -> Tuple[str, date]:
    """
    Expects:
      aerial_ytd_export_transmission_2026_01_19.xlsx
      aerial_ytd_export_distribution_2026_01_19.xlsx

    Returns:
      dataset_type: transmission|distribution
      report_date: YYYY-MM-DD
    """
    m = re.match(r"aerial_ytd_export_(transmission|distribution)_(\d{4})_(\d{2})_(\d{2})\.xlsx$", file_name)
    if not m:
        raise ValueError(f"Filename not in expected format: {file_name}")

    dataset_type = m.group(1)
    y, mo, d = int(m.group(2)), int(m.group(3)), int(m.group(4))
    return dataset_type, date(y, mo, d)

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # strip whitespace and standardize column names a bit
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    return df

def validate_required_columns(df: pd.DataFrame) -> None:
    required = {"DateUploaded", "flight_date", "vendor", "structureId", "folder", "imageCount"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Dates: accept various formats (e.g. 01-12-2026)
    df["DateUploaded"] = pd.to_datetime(df["DateUploaded"], errors="coerce").dt.date
    df["flight_date"] = pd.to_datetime(df["flight_date"], errors="coerce").dt.date

    # imageCount numeric
    df["imageCount"] = pd.to_numeric(df["imageCount"], errors="coerce").astype("Int64")

    # strings
    for c in ["vendor", "structureId", "folder"]:
        df[c] = df[c].astype(str).str.strip()

    return df

def attach_row_metadata(
    df: pd.DataFrame,
    *,
    pipeline_name: str,
    run_id: str,
    file_id: str,
    ingested_at_utc: datetime,
    report_date: date,
    dataset_type: str,
    source_system: str,
    source_file_name: str,
    source_file_path: str
) -> pd.DataFrame:
    out = df.copy()

    out["pipeline_name"] = pipeline_name
    out["run_id"] = run_id
    out["file_id"] = file_id

    out["ingested_at_utc"] = ingested_at_utc.isoformat()
    out["report_date"] = report_date.isoformat()

    out["dataset_type"] = dataset_type

    out["source_system"] = source_system
    out["source_file_name"] = source_file_name
    out["source_file_path"] = source_file_path

    return out


# ----------------------------
# UTIL “tables”
# ----------------------------

INGEST_RUNS_PATH = UTIL_DIR / "ingest_runs.csv"
INGEST_FILES_PATH = UTIL_DIR / "ingest_files.csv"

def util_file_already_loaded(file_hash: str) -> bool:
    df = read_csv_if_exists(INGEST_FILES_PATH)
    if df.empty:
        return False
    # consider LOADED as terminal success
    return ((df.get("file_hash_sha256") == file_hash) & (df.get("status") == "LOADED")).any()

def util_upsert_file_row(file_hash: str, updates: dict) -> None:
    """
    Upsert by file_hash_sha256 (simple CSV implementation).
    - If exists: update the row
    - Else: append new row
    """
    df = read_csv_if_exists(INGEST_FILES_PATH)
    if df.empty:
        atomic_write_csv(pd.DataFrame([updates]), INGEST_FILES_PATH)
        return

    if "file_hash_sha256" not in df.columns:
        raise ValueError("UTIL.INGEST_FILES missing 'file_hash_sha256' column")

    mask = df["file_hash_sha256"] == file_hash
    if mask.any():
        for k, v in updates.items():
            if k not in df.columns:
                df[k] = pd.NA
            df.loc[mask, k] = v
        atomic_write_csv(df, INGEST_FILES_PATH)
    else:
        append_csv_row(updates, INGEST_FILES_PATH)

def util_log_run_start(run_row: dict) -> None:
    append_csv_row(run_row, INGEST_RUNS_PATH)

def util_log_run_end(run_id: str, status: str, ended_at_utc: datetime, error_message: Optional[str] = None) -> None:
    df = read_csv_if_exists(INGEST_RUNS_PATH)
    if df.empty:
        return

    if "run_id" not in df.columns:
        return

    mask = df["run_id"] == run_id
    if not mask.any():
        return

    # ensure columns exist
    for c in ["status", "ended_at_utc", "error_message"]:
        if c not in df.columns:
            df[c] = pd.NA

    df.loc[mask, "status"] = status
    df.loc[mask, "ended_at_utc"] = ended_at_utc.isoformat()
    if error_message:
        df.loc[mask, "error_message"] = error_message

    atomic_write_csv(df, INGEST_RUNS_PATH)


# ----------------------------
# Silver logic: STATE + CHANGES
# ----------------------------

SILVER_STATE_PATH = SILVER_DIR / SUBJECT / "deliveries_folder_state.csv"
SILVER_CHANGES_PATH = SILVER_DIR / SUBJECT / "deliveries_folder_changes.csv"

def build_changes_and_new_state(
    bronze_current: pd.DataFrame,
    report_date: date,
    run_id: str,
    file_id: str,
    ingested_at_utc: datetime
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns: (changes_df, new_state_df)
    - changes_df: only NEW_KEY or COUNT_CHANGED rows
    - new_state_df: updated full state table
    """
    # Load existing state
    state = read_csv_if_exists(SILVER_STATE_PATH)
    if state.empty:
        # initialize empty state with expected columns
        state = pd.DataFrame(columns=[
            "dataset_type","vendor","structureId","folder",
            "dateUploaded","flight_date","imageCount",
            "first_seen_report_date","last_seen_report_date",
            "first_seen_run_id","last_seen_run_id",
            "first_seen_file_id","last_seen_file_id",
            "last_ingested_at_utc"
        ])

    # Ensure consistent types for joins
    bronze = bronze_current.copy()
    for c in KEY_COLS:
        bronze[c] = bronze[c].astype(str)

    for c in KEY_COLS:
        if c in state.columns:
            state[c] = state[c].astype(str)

    # Reduce bronze to one row per key (defensive)
    # If duplicates exist, keep the max imageCount and latest dates
    bronze_reduced = (
        bronze.sort_values(["report_date", "ingested_at_utc"])
              .groupby(KEY_COLS, as_index=False)
              .agg({
                  "DateUploaded": "max",
                  "flight_date": "max",
                  "imageCount": "max",
                  "pipeline_name": "last",
                  "run_id": "last",
                  "file_id": "last",
                  "ingested_at_utc": "last",
                  "report_date": "last",
                  "source_system": "last",
                  "source_file_name": "last",
                  "source_file_path": "last",
              })
    )

    # Join to detect new keys & changes vs existing state
    # State imageCount may be string if loaded from CSV; coerce for compare
    if "imageCount" in state.columns:
        state["imageCount"] = pd.to_numeric(state["imageCount"], errors="coerce")

    bronze_reduced["imageCount"] = pd.to_numeric(bronze_reduced["imageCount"], errors="coerce")

    merged = bronze_reduced.merge(
        state[KEY_COLS + ["imageCount"]],
        on=KEY_COLS,
        how="left",
        suffixes=("", "_prev")
    )

    # Determine change type
    is_new = merged["imageCount_prev"].isna()
    is_changed = (~is_new) & (merged["imageCount"] != merged["imageCount_prev"])

    changes = merged.loc[is_new | is_changed, KEY_COLS + ["imageCount_prev", "imageCount"]].copy()
    if changes.empty:
        changes_df = pd.DataFrame(columns=[
            "dataset_type","vendor","structureId","folder",
            "report_date","change_type","old_imageCount","new_imageCount",
            "run_id","file_id","ingested_at_utc"
        ])
    else:
        changes["change_type"] = changes.apply(
            lambda r: "NEW_KEY" if pd.isna(r["imageCount_prev"]) else "COUNT_CHANGED",
            axis=1
        )
        changes_df = pd.DataFrame({
            "dataset_type": changes["dataset_type"],
            "vendor": changes["vendor"],
            "structureId": changes["structureId"],
            "folder": changes["folder"],
            "report_date": report_date.isoformat(),
            "change_type": changes["change_type"],
            "old_imageCount": changes["imageCount_prev"],
            "new_imageCount": changes["imageCount"],
            "run_id": run_id,
            "file_id": file_id,
            "ingested_at_utc": ingested_at_utc.isoformat(),
        })

    # Update/create new state:
    # - For new keys: insert with first_seen & last_seen
    # - For existing: update imageCount and last_seen info; keep first_seen intact

    # Build a state index for easy updates
    state_key = state.set_index(KEY_COLS, drop=False)

    for _, r in bronze_reduced.iterrows():
        key = tuple(str(r[c]) for c in KEY_COLS)
        # Make sure key exists in index
        if key not in state_key.index:
            # New key
            new_row = {
                "dataset_type": r["dataset_type"],
                "vendor": r["vendor"],
                "structureId": r["structureId"],
                "folder": r["folder"],
                "dateUploaded": str(r["DateUploaded"]),
                "flight_date": str(r["flight_date"]),
                "imageCount": int(r["imageCount"]) if pd.notna(r["imageCount"]) else None,
                "first_seen_report_date": report_date.isoformat(),
                "last_seen_report_date": report_date.isoformat(),
                "first_seen_run_id": run_id,
                "last_seen_run_id": run_id,
                "first_seen_file_id": file_id,
                "last_seen_file_id": file_id,
                "last_ingested_at_utc": ingested_at_utc.isoformat(),
            }
            state_key.loc[key, :] = pd.Series(new_row)
        else:
            # Existing key: update last_seen and current values
            state_key.loc[key, "dateUploaded"] = str(r["DateUploaded"])
            state_key.loc[key, "flight_date"] = str(r["flight_date"])
            state_key.loc[key, "imageCount"] = int(r["imageCount"]) if pd.notna(r["imageCount"]) else state_key.loc[key, "imageCount"]
            state_key.loc[key, "last_seen_report_date"] = report_date.isoformat()
            state_key.loc[key, "last_seen_run_id"] = run_id
            state_key.loc[key, "last_seen_file_id"] = file_id
            state_key.loc[key, "last_ingested_at_utc"] = ingested_at_utc.isoformat()

    new_state_df = state_key.reset_index(drop=True)

    return changes_df, new_state_df


# ----------------------------
# Main ingestion
# ----------------------------

def find_latest_snapshot_file() -> Path:
    if not GCP_DAILY_DIR.exists():
        raise FileNotFoundError(f"Missing directory: {GCP_DAILY_DIR.resolve()}")

    files = sorted(GCP_DAILY_DIR.glob("aerial_ytd_export_*_????_??_??.xlsx"))
    if not files:
        raise FileNotFoundError(f"No matching XLSX files found in {GCP_DAILY_DIR.resolve()}")

    # choose latest by filename date if present; fallback to mtime
    def key_fn(p: Path):
        try:
            _, rd = parse_filename_metadata(p.name)
            return (rd, p.stat().st_mtime)
        except Exception:
            return (date.min, p.stat().st_mtime)

    return sorted(files, key=key_fn)[-1]


def main():
    ensure_dirs()

    run_id = str(uuid.uuid4())
    started = utc_now()

    # Log run start
    util_log_run_start({
        "run_id": run_id,
        "pipeline_name": PIPELINE_NAME,
        "started_at_utc": started.isoformat(),
        "status": "STARTED",
        "trigger": "MANUAL",
        "executor": os.getenv("USERNAME") or os.getenv("USER") or "",
        "host_name": os.getenv("COMPUTERNAME") or os.uname().nodename if hasattr(os, "uname") else "",
    })

    file_id = str(uuid.uuid4())

    try:
        src_path = find_latest_snapshot_file()
        dataset_type, report_date = parse_filename_metadata(src_path.name)

        file_hash = sha256_file(src_path)

        # Idempotency: skip if already LOADED
        if util_file_already_loaded(file_hash):
            util_upsert_file_row(file_hash, {
                "file_hash_sha256": file_hash,
                "file_id": file_id,
                "run_id": run_id,
                "pipeline_name": PIPELINE_NAME,
                "status": "SKIPPED_DUPLICATE",
                "file_name": src_path.name,
                "source_path": str(src_path),
                "report_date": report_date.isoformat(),
                "updated_at_utc": utc_now().isoformat(),
            })
            util_log_run_end(run_id, "SUCCESS", utc_now())
            print(f"SKIPPED (duplicate): {src_path.name}")
            return

        # Register file (DISCOVERED)
        stat = src_path.stat()
        util_upsert_file_row(file_hash, {
            "file_hash_sha256": file_hash,
            "file_id": file_id,
            "run_id": run_id,
            "pipeline_name": PIPELINE_NAME,
            "status": "DISCOVERED",
            "file_name": src_path.name,
            "source_path": str(src_path),
            "file_size_bytes": stat.st_size,
            "file_modified_at_utc": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            "report_date": report_date.isoformat(),
            "created_at_utc": utc_now().isoformat(),
        })

        # Archive raw file into bronze/raw_files/<dataset_type>/<report_date>/
        raw_archive_dir = BRONZE_DIR / SUBJECT / "raw_files" / f"dataset_type={dataset_type}" / f"report_date={report_date.isoformat()}"
        raw_archive_dir.mkdir(parents=True, exist_ok=True)
        archived_path = raw_archive_dir / src_path.name
        shutil.copy2(src_path, archived_path)

        util_upsert_file_row(file_hash, {
            "file_hash_sha256": file_hash,
            "status": "ARCHIVED_RAW",
            "bronze_raw_path": str(archived_path),
            "updated_at_utc": utc_now().isoformat(),
        })

        # Parse XLSX
        df = pd.read_excel(src_path, engine="openpyxl")
        df = normalize_columns(df)
        validate_required_columns(df)
        df = coerce_types(df)

        # Add row metadata
        ingested_at = utc_now()
        df_bronze = attach_row_metadata(
            df,
            pipeline_name=PIPELINE_NAME,
            run_id=run_id,
            file_id=file_id,
            ingested_at_utc=ingested_at,
            report_date=report_date,
            dataset_type=dataset_type,
            source_system=SOURCE_SYSTEM,
            source_file_name=src_path.name,
            source_file_path=str(src_path),
        )

        util_upsert_file_row(file_hash, {
            "file_hash_sha256": file_hash,
            "status": "PARSED",
            "rows_parsed": int(len(df_bronze)),
            "updated_at_utc": utc_now().isoformat(),
        })

        # Write BRONZE CURRENT (overwrite)
        bronze_current_path = BRONZE_DIR / SUBJECT / "current" / f"deliveries_ytd_current__{dataset_type}.csv"
        atomic_write_csv(df_bronze, bronze_current_path)

        util_upsert_file_row(file_hash, {
            "file_hash_sha256": file_hash,
            "status": "LOADED_BRONZE_CURRENT",
            "bronze_current_path": str(bronze_current_path),
            "rows_loaded_bronze": int(len(df_bronze)),
            "updated_at_utc": utc_now().isoformat(),
        })

        # Build SILVER changes + state
        changes_df, new_state_df = build_changes_and_new_state(
            bronze_current=df_bronze,
            report_date=report_date,
            run_id=run_id,
            file_id=file_id,
            ingested_at_utc=ingested_at
        )

        # Append changes, overwrite state
        if not changes_df.empty:
            if SILVER_CHANGES_PATH.exists():
                changes_df.to_csv(SILVER_CHANGES_PATH, mode="a", header=False, index=False)
            else:
                changes_df.to_csv(SILVER_CHANGES_PATH, mode="w", header=True, index=False)

        atomic_write_csv(new_state_df, SILVER_STATE_PATH)

        util_upsert_file_row(file_hash, {
            "file_hash_sha256": file_hash,
            "status": "LOADED_SILVER",
            "rows_written_state": int(len(new_state_df)),
            "rows_written_changes": int(len(changes_df)),
            "updated_at_utc": utc_now().isoformat(),
        })

        # Mark file LOADED terminal
        util_upsert_file_row(file_hash, {
            "file_hash_sha256": file_hash,
            "status": "LOADED",
            "loaded_at_utc": utc_now().isoformat(),
            "updated_at_utc": utc_now().isoformat(),
        })

        util_log_run_end(run_id, "SUCCESS", utc_now())
        print(f"SUCCESS: loaded {src_path.name}")
        print(f"  Bronze current: {bronze_current_path}")
        print(f"  Silver state:  {SILVER_STATE_PATH}")
        print(f"  Silver changes:{SILVER_CHANGES_PATH}")

    except Exception as e:
        util_log_run_end(run_id, "FAILED", utc_now(), error_message=str(e))
        # also try to mark file failed if we have hash (best effort)
        print(f"FAILED: {e}")
        raise


if __name__ == "__main__":
    main()
