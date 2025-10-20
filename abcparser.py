## DROP IN REPLACEMENT FOR FIX
# g is already sorted and reset_index(drop=True)
mgr_mask = g[cfg.user_col].str.upper().eq(cfg._mgr_upper)
mgr_idx = np.flatnonzero(mgr_mask.values)

first_mgr_idx = int(mgr_idx.min()) if mgr_idx.size > 0 else None
last_mgr_idx  = int(mgr_idx.max()) if mgr_idx.size > 0 else None
c_start_idx   = (last_mgr_idx + 1) if mgr_idx.size > 0 else None  # C begins AFTER the manager block

# Completed (for guard/latency)
t_completed = g.loc[g["_is_completed"], cfg.time_col].min() if g["_is_completed"].any() else pd.NaT

# ---- FIXED: choose B_user = last NON-MANAGER user BEFORE the FIRST manager row ----
b_user = None
b_run_start_idx = None
if first_mgr_idx is not None:
    k = first_mgr_idx - 1
    # walk back to the nearest user that is not null AND not the manager
    while k >= 0:
        u = g.loc[k, cfg.user_col]
        if pd.notna(u) and (str(u).upper() != cfg._mgr_upper):
            b_user = u
            break
        k -= 1

    if b_user is not None:
        # walk backward to the start of this user's contiguous run
        i = k
        while i - 1 >= 0 and g.loc[i - 1, cfg.user_col] == b_user:
            i -= 1
        b_run_start_idx = i

# If no manager block, we can fallback to "first row after Completed" as B_start
b_start_idx = None
t_b = pd.NaT
if b_run_start_idx is not None:
    t_b = g.loc[b_run_start_idx, cfg.time_col]
    if pd.notna(t_completed):
        t_b = max(t_b, t_completed)             # never start B before Completed
    b_start_idx = int(np.flatnonzero((g[cfg.time_col] >= t_b).values)[0])
elif (first_mgr_idx is None) and pd.notna(t_completed):
    # no manager block: B starts at first row AFTER Completed
    idx_after_completed = np.flatnonzero((g[cfg.time_col] > t_completed).values)
    if idx_after_completed.size > 0:
        b_start_idx = int(idx_after_completed[0])
        t_b = g.loc[b_start_idx, cfg.time_col]
        # reviewer = first non-null user at/after b_start_idx
        uu = g.loc[b_start_idx:, cfg.user_col].dropna()
        b_user = None if uu.empty else uu.iloc[0]


## END 


# =========================
# Phase splitter (A/B/C)
# =========================
from dataclasses import dataclass
from typing import Tuple, Dict, List
import pandas as pd
import numpy as np
import re

@dataclass
class PhaseConfig:
    incident_col: str = "INCIDENT_ID"
    time_col: str = "FOLLOWUP_DATETIME"
    insert_col: str = "INSERTED_DATE"      # tie-break only
    desc_col: str = "FOLLOWUP_DESC"
    user_col: str = "SYSTEM_OPID"

    # anchors
    completed_regex: str = r"change status to\s*:?\s*Completed"
    his_manager_user: str = "CGI_HISMGR"   # canonical archival user

    # optional backups (kept off-path; can be useful for EDA)
    his_token_regex: str = r"(?<![A-Za-z])HIS(?![A-Za-z])"
    send_to_history_regex: str = r"(?:send|sent)(?:ed)?\s+to\s+history"

    def __post_init__(self):
        self._pat_completed = re.compile(self.completed_regex, re.IGNORECASE)
        self._pat_his_token = re.compile(self.his_token_regex, re.IGNORECASE)
        self._pat_send_hist = re.compile(self.send_to_history_regex, re.IGNORECASE)
        self._mgr_upper = self.his_manager_user.upper()

def _ensure_dt(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out and not np.issubdtype(out[c].dtype, np.datetime64):
            out[c] = pd.to_datetime(out[c], errors="coerce")
    return out

def _flag(df: pd.DataFrame, cfg: PhaseConfig) -> pd.DataFrame:
    desc = df[cfg.desc_col].fillna("")
    user = df[cfg.user_col].fillna("")
    return df.assign(
        _is_completed = desc.str.contains(cfg._pat_completed),
        _is_his_token = desc.str.contains(cfg._pat_his_token),
        _is_send_hist = desc.str.contains(cfg._pat_send_hist),
        _is_mgr_user  = user.str.upper().eq(cfg._mgr_upper),
    )

def _segment_single_incident(g: pd.DataFrame, cfg: PhaseConfig) -> Tuple[pd.DataFrame, Dict]:
    # Sort deterministically and reset to RangeIndex (index-based cuts survive equal timestamps)
    g = g.sort_values([cfg.time_col, cfg.insert_col], kind="stable").reset_index(drop=True)

    
    # --- locate archival block (all CGI_HISMGR rows) ---
    mgr_mask = g[cfg.user_col].str.upper().eq(cfg._mgr_upper)
    mgr_idx = np.flatnonzero(mgr_mask.values)
    if mgr_idx.size > 0:
        first_mgr_idx = int(mgr_idx.min())
        last_mgr_idx  = int(mgr_idx.max())
        c_start_idx   = last_mgr_idx + 1              # C begins AFTER archival block
        t_archive_first = g.loc[first_mgr_idx, cfg.time_col]
        t_archive_last  = g.loc[last_mgr_idx,  cfg.time_col]
    else:
        first_mgr_idx = last_mgr_idx = None
        c_start_idx = None
        t_archive_first = pd.NaT
        t_archive_last  = pd.NaT

    # --- Completed (for latency + B guard) ---
    t_completed = g.loc[g["_is_completed"], cfg.time_col].min() if g["_is_completed"].any() else pd.NaT

    # --- find DOC reviewer (B_user): user immediately BEFORE archival block ---
    b_user = None
    b_run_start_idx = None
    if c_start_idx is not None and c_start_idx > 0:
        k = c_start_idx - 1
        # nearest non-null user
        while k >= 0 and pd.isna(g.loc[k, cfg.user_col]):
            k -= 1
        if k >= 0:
            b_user = g.loc[k, cfg.user_col]
            # walk backward while contiguous with same user
            i = k
            while i - 1 >= 0 and g.loc[i - 1, cfg.user_col] == b_user:
                i -= 1
            b_run_start_idx = i

    # --- B start time (guarded by Completed) and its index ---
    b_start_idx = None
    t_b = pd.NaT
    if b_run_start_idx is not None:
        t_b = g.loc[b_run_start_idx, cfg.time_col]
        if pd.notna(t_completed):
            t_b = max(t_b, t_completed)   # never start B before Completed (if present)
        # first index whose time >= t_b (handles equal timestamps cleanly)
        b_start_idx = int(np.flatnonzero((g[cfg.time_col] >= t_b).values)[0])

    # --- if NO archival block: fall back to Completed→B only ---
    if c_start_idx is None:
        if pd.notna(t_completed):
            # B starts at first row AFTER Completed (by time), reviewer = first user there
            idx_after_completed = np.flatnonzero((g[cfg.time_col] > t_completed).values)
            if idx_after_completed.size > 0:
                b_start_idx = int(idx_after_completed[0])
                t_b = g.loc[b_start_idx, cfg.time_col]
                # reviewer = first non-null user at/after b_start_idx
                uu = g.loc[b_start_idx:, cfg.user_col].dropna()
                b_user = None if uu.empty else uu.iloc[0]
        # else: no Completed, no archival → everything is A

    # --- Phase labeling by index ---
    phase = pd.Series("A_LiveDispatch", index=g.index, dtype=object)
    if b_start_idx is not None:
        phase.iloc[:b_start_idx] = "A_LiveDispatch"
        if c_start_idx is not None:
            phase.iloc[b_start_idx:c_start_idx] = "B_DOC_QC"  # includes all CGI_HISMGR rows
            phase.iloc[c_start_idx:] = "C_RA_QC"
        else:
            phase.iloc[b_start_idx:] = "B_DOC_QC"
    else:
        if c_start_idx is not None:
            # no identifiable B start; split A/C only
            phase.iloc[:c_start_idx] = "A_LiveDispatch"
            phase.iloc[c_start_idx:] = "C_RA_QC"
        else:
            phase[:] = "A_LiveDispatch"

    g["_phase"] = phase

    # --- times for durations ---
    t_start = g[cfg.time_col].min()
    t_c_start = g.loc[c_start_idx, cfg.time_col] if (c_start_idx is not None and c_start_idx < len(g)) else pd.NaT
    t_end = g[cfg.time_col].max()

    # durations: B ends at LAST archival time (not the first RA row)
    dur_live   = (t_b - t_start)              if pd.notna(t_b) and pd.notna(t_start)       else pd.NaT
    dur_doc_qc = (t_archive_last - t_b)       if pd.notna(t_archive_last) and pd.notna(t_b) else pd.NaT
    dur_ra_qc  = (t_end - t_c_start)          if pd.notna(t_c_start)                        else pd.NaT
    latency_completed_to_archive = (t_archive_first - t_completed) if (pd.notna(t_archive_first) and pd.notna(t_completed)) else pd.NaT

    # users per phase (unique)
    users_live = g.loc[g["_phase"]=="A_LiveDispatch", cfg.user_col].dropna().unique().tolist()
    users_doc  = g.loc[g["_phase"]=="B_DOC_QC",      cfg.user_col].dropna().unique().tolist()
    users_ra   = g.loc[g["_phase"]=="C_RA_QC",       cfg.user_col].dropna().unique().tolist()

    summary = dict(
        incident_id = g[cfg.incident_col].iloc[0],
        # index boundaries
        b_start_idx = b_start_idx,
        c_start_idx = c_start_idx,
        first_mgr_idx = first_mgr_idx,
        last_mgr_idx  = last_mgr_idx,
        # time anchors
        t_start=t_start,
        t_completed=t_completed,
        t_b_start=t_b,
        t_archive_first=t_archive_first,
        t_archive_last=t_archive_last,
        t_c_start=t_c_start,
        t_end=t_end,
        # durations
        dur_live=dur_live,
        dur_doc_qc=dur_doc_qc,
        dur_ra_qc=dur_ra_qc,
        dur_live_min=float(dur_live/np.timedelta64(1,"m")) if pd.notna(dur_live) else np.nan,
        dur_doc_qc_min=float(dur_doc_qc/np.timedelta64(1,"m")) if pd.notna(dur_doc_qc) else np.nan,
        dur_ra_qc_min=float(dur_ra_qc/np.timedelta64(1,"m")) if pd.notna(dur_ra_qc) else np.nan,
        latency_completed_to_archive_min=float(latency_completed_to_archive/np.timedelta64(1,"m")) if pd.notna(latency_completed_to_archive) else np.nan,
        # counts + actors
        n_events_total=int(len(g)),
        n_live=int((g["_phase"]=="A_LiveDispatch").sum()),
        n_doc_qc=int((g["_phase"]=="B_DOC_QC").sum()),
        n_ra_qc=int((g["_phase"]=="C_RA_QC").sum()),
        doc_reviewer=b_user,
        users_live=users_live, users_doc=users_doc, users_ra=users_ra,
        has_completed=bool(pd.notna(t_completed)),
        has_archival_block=bool(mgr_idx.size > 0),
        c_start_source=("after_last_" + cfg.his_manager_user) if (mgr_idx.size > 0) else "none",
    )
    return g, summary

def segment_phases(events: pd.DataFrame, cfg: PhaseConfig):
    # validate & prep
    needed = [cfg.incident_col, cfg.time_col, cfg.insert_col, cfg.desc_col, cfg.user_col]
    miss = [c for c in needed if c not in events.columns]
    if miss: raise KeyError(f"Missing required columns: {miss}")

    wk = _ensure_dt(events, [cfg.time_col, cfg.insert_col])
    wk = wk.sort_values([cfg.incident_col, cfg.time_col, cfg.insert_col], kind="stable")
    wk = _flag(wk, cfg)

    parts, rows = [], []
    for _, gi in wk.groupby(cfg.incident_col, sort=False):
        gx, sx = _segment_single_incident(gi, cfg)
        parts.append(gx); rows.append(sx)

    return pd.concat(parts, ignore_index=True), pd.DataFrame(rows)
