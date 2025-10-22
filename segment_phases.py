from dataclasses import dataclass
from typing import Set, Tuple, Dict, List
import pandas as pd, numpy as np, re

# ==============================
# Config
# ==============================
@dataclass
class PhaseConfig:
    incident_col: str = "INCIDENT_ID"
    time_col: str = "FOLLOWUP_DATETIME"
    insert_col: str = "INSERTED_DATE"
    desc_col: str = "FOLLOWUP_DESC"
    user_col: str = "SYSTEM_OPID"

    completed_regex: str = r"change status to\s*:?\s*Completed"
    his_manager_user: str = "CGI_HISMGR"              # archival user (canonical)
    ra_users: Set[str] = None                         # e.g., {"RA1","RA2","RA3"}
    ignorable_users: Set[str] = None                  # e.g., {"CGI_SDU_USER","USEROMS","CAD"}

    # B-phase shaping
    post_archive_grace_min: int = 10                  # keep same DOC reviewer’s quick fixes in B
    a_tail_minutes: int = 2                           # force A up to Completed + N minutes
    b_lookback_hours: int = 24                        # bounded lookback when finding B start
    enforce_same_day_for_b: bool = False              # optional midnight cap for B start

    # Sessionization for C1/C2 durations
    c1_session_gap_hours: int = 2
    c1_window_days: int = 3
    c1_duration_mode: str = "first_session"          # ["first_session","first_window","sum_sessions_in_window"]

    c2_session_gap_hours: int = 6
    c2_window_days: int = 14
    c2_duration_mode: str = "first_session"          # ["first_session","first_window","sum_sessions_in_window"]

    def __post_init__(self):
        self._pat_completed = re.compile(self.completed_regex, re.IGNORECASE)
        self._mgr_upper = self.his_manager_user.upper()
        self._ra_upper = {u.upper() for u in (self.ra_users or set())}
        self._ign_upper = {u.upper() for u in (self.ignorable_users or set())}

# ==============================
# Helpers
# ==============================
def _ensure_dt(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out and not np.issubdtype(out[c].dtype, np.datetime64):
            out[c] = pd.to_datetime(out[c], errors="coerce")
    return out

def _flag(df: pd.DataFrame, cfg: PhaseConfig) -> pd.DataFrame:
    # Adds _is_completed flag
    desc = df[cfg.desc_col].fillna("")
    return df.assign(_is_completed = desc.str.contains(cfg._pat_completed))

def _first_index_at_or_after(ts: pd.Series, threshold) -> int:
    """
    ts: sorted datetime Series
    threshold: Timestamp-like (or NaT)
    Returns first integer position i such that ts[i] >= threshold.
    If threshold is NaT -> 0. If threshold after all rows -> len(ts).
    """
    if pd.isna(threshold):
        return 0
    th = pd.Timestamp(threshold)
    return int(pd.Index(ts.values).searchsorted(th, side="left"))

def _phase_duration_minutes(ts: pd.Series,
                            phase: pd.Series,
                            start_idx_after_b: int,
                            label: str,
                            session_gap_hours: int,
                            window_days: int,
                            mode: str) -> float:
    """
    Sessionized duration (minutes) for a phase label after B_end.
    Splits sessions when gap between consecutive events > session_gap_hours.
    Modes:
      - "first_session": span of the first session only
      - "first_window": span from first event to last event within window_days
      - "sum_sessions_in_window": sum of session spans whose start is within window_days
    Returns np.nan if no events with 'label' occur after B_end.
    """
    post = phase.iloc[start_idx_after_b:]
    mask = (post == label)
    if not mask.any():
        return np.nan

    s = ts.iloc[start_idx_after_b:][mask].sort_values()
    if len(s) == 1:
        return 0.0

    gap = pd.Timedelta(hours=session_gap_hours)
    diffs = s.diff()
    sess_id = (diffs > gap).cumsum()

    first_t = s.iloc[0]
    win_end = first_t + pd.Timedelta(days=window_days)

    if mode == "first_session":
        sid0 = sess_id.iloc[0]
        block = s[sess_id == sid0]
        return float((block.iloc[-1] - block.iloc[0]) / pd.Timedelta(minutes=1))

    if mode == "first_window":
        block = s[s <= win_end]
        if block.empty:
            block = s.iloc[[0]]
        return float((block.iloc[-1] - block.iloc[0]) / pd.Timedelta(minutes=1))

    if mode == "sum_sessions_in_window":
        total = 0.0
        for sid in sess_id.unique():
            block = s[sess_id == sid]
            if block.iloc[0] <= win_end:
                total += float((block.iloc[-1] - block.iloc[0]) / pd.Timedelta(minutes=1))
        return total

    # fallback
    sid0 = sess_id.iloc[0]
    block = s[sess_id == sid0]
    return float((block.iloc[-1] - block.iloc[0]) / pd.Timedelta(minutes=1))

# ==============================
# Core: per-incident segmentation
# ==============================
def _segment_single_incident(g: pd.DataFrame, cfg: PhaseConfig) -> Tuple[pd.DataFrame, Dict]:
    # Sort deterministically and use RangeIndex so index-based cuts work with equal timestamps
    g = g.sort_values([cfg.time_col, cfg.insert_col], kind="stable").reset_index(drop=True)
    user = g[cfg.user_col].astype(str)
    ts   = g[cfg.time_col]

    # --- Locate archival block (all CGI_HISMGR rows) ---
    mgr_mask = user.str.upper().eq(cfg._mgr_upper)
    mgr_idx = np.flatnonzero(mgr_mask.values)

    if mgr_idx.size == 0:
        # No archive -> per policy: everything is A
        g["_phase"] = "A_LiveDispatch"
        return g, dict(
            incident_id=g.iloc[0][cfg.incident_col],
            has_archival_block=False,
            doc_reviewer=None,
            b_start_idx=None, b_end_idx=None,
            t_completed=pd.NaT, a_tail_end=pd.NaT,
            t_archive_first=pd.NaT, t_archive_last=pd.NaT,
            t_b_start=pd.NaT, t_b_end=pd.NaT,
            t_c1_start=pd.NaT, t_c2_start=pd.NaT,
            dur_doc_qc_min=np.nan, dur_c1_min=np.nan, dur_c2_min=np.nan,
            n_events_total=int(len(g)), n_live=int(len(g)),
            n_doc_qc=0, n_c1=0, n_c2=0
        )

    first_mgr_idx = int(mgr_idx.min())
    last_mgr_idx  = int(mgr_idx.max())
    t_archive_first = ts.iloc[first_mgr_idx]
    t_archive_last  = ts.iloc[last_mgr_idx]

    # --- Completed anchor + A-tail buffer (A has priority up to Completed+tail) ---
    t_completed = ts.loc[g["_is_completed"]].min() if g["_is_completed"].any() else pd.NaT
    a_tail_end = (t_completed + np.timedelta64(cfg.a_tail_minutes, "m")) if pd.notna(t_completed) else pd.NaT

    # --- DOC reviewer (B_user): last NON-manager user BEFORE FIRST manager row (skip ignorable) ---
    b_user = None
    k = first_mgr_idx - 1
    while k >= 0:
        u_up = (user.iloc[k] or "").upper()
        if (u_up != "") and (u_up != cfg._mgr_upper) and (u_up not in cfg._ign_upper):
            b_user = user.iloc[k]
            break
        k -= 1

    # --- Walk backward to the start of that user's run, allowing ignorable interruptions,
    #     but enforce time bounds so we don't pull month-old rows into B ---
    b_run_start_idx = None
    if b_user is not None:
        lower_time_bound = t_archive_first - np.timedelta64(cfg.b_lookback_hours, "h")
        if pd.notna(a_tail_end):
            lower_time_bound = max(lower_time_bound, a_tail_end)
        if cfg.enforce_same_day_for_b:
            lower_time_bound = max(lower_time_bound, pd.Timestamp(t_archive_first.normalize()))  # midnight

        i = k
        while i - 1 >= 0:
            u_prev_up = (user.iloc[i-1] or "").upper()
            t_prev    = ts.iloc[i-1]
            # stop if earlier than allowed window
            if pd.notna(lower_time_bound) and pd.notna(t_prev) and (t_prev < lower_time_bound):
                break
            # otherwise allow contiguous same-user OR ignorable users
            if (u_prev_up == b_user.upper()) or (u_prev_up in cfg._ign_upper):
                i -= 1
            else:
                break

        # ensure run starts on a real b_user row (skip leading ignorables)
        j = i
        while j <= k and (user.iloc[j] or "").upper() in cfg._ign_upper:
            j += 1
        b_run_start_idx = j if (j <= k and (user.iloc[j] or "").upper() == b_user.upper()) else k

    # --- B start: never before A-tail; and never after first_mgr_idx (clamped) ---
    if b_run_start_idx is not None:
        t_b_candidate = ts.iloc[b_run_start_idx]
        if pd.notna(a_tail_end):
            t_b_candidate = max(t_b_candidate, a_tail_end)
    else:
        t_b_candidate = t_archive_first
        if pd.notna(a_tail_end):
            t_b_candidate = max(t_b_candidate, a_tail_end)

    i = _first_index_at_or_after(ts, t_b_candidate)
    if i >= len(g):
        i = first_mgr_idx
    elif i > first_mgr_idx:
        i = first_mgr_idx

    b_start_idx = i
    if b_start_idx >= len(g):
        b_start_idx = max(0, first_mgr_idx)
    t_b_start = ts.iloc[b_start_idx]

    # --- B initially ends right AFTER the archival block; extend by grace if same DOC reviewer edits soon after ---
    b_end_idx = last_mgr_idx + 1  # exclusive
    grace = np.timedelta64(cfg.post_archive_grace_min, "m")
    cursor = b_end_idx
    while cursor < len(g):
        u_cur = (g.loc[cursor, cfg.user_col] or "")
        same_user = (u_cur.upper() == (b_user or "").upper())
        within_grace = (ts.loc[cursor] <= (t_archive_last + grace))
        if same_user and within_grace:
            cursor += 1
            b_end_idx = cursor
        else:
            break

    # --- Phase labeling (A-lock & A-tail applied first) ---
    phase = pd.Series("A_LiveDispatch", index=g.index, dtype=object)

    if pd.notna(t_completed):
        phase.loc[ts < t_completed] = "A_LiveDispatch"
        if pd.notna(a_tail_end):
            phase.loc[(ts >= t_completed) & (ts <= a_tail_end)] = "A_LiveDispatch"

    # B range
    phase.iloc[b_start_idx:b_end_idx] = "B_DOC_QC"

    # After B: RA → C2, else C1
    if b_end_idx < len(g):
        post = g.iloc[b_end_idx:]
        is_ra = post[cfg.user_col].str.upper().isin(cfg._ra_upper)
        phase.iloc[b_end_idx:] = np.where(is_ra.values, "C2_RA_QC", "C1_DOC_POSTHIST")

    g["_phase"] = phase

    # --- Durations (mins) ---
    t_start = ts.min()
    t_end   = ts.max()
    t_b_end = ts.iloc[b_end_idx-1] if b_end_idx > b_start_idx else t_b_start

    dur_doc_qc = (t_b_end - t_b_start) if pd.notna(t_b_end) and pd.notna(t_b_start) else pd.NaT

    # First-occurrence timestamps for C1/C2 (for reference)
    t_c1_rows = g.index[g["_phase"]=="C1_DOC_POSTHIST"]
    t_c2_rows = g.index[g["_phase"]=="C2_RA_QC"]
    t_c1_start = (ts.iloc[t_c1_rows[0]] if len(t_c1_rows) > 0 else pd.NaT)
    t_c2_start = (ts.iloc[t_c2_rows[0]] if len(t_c2_rows) > 0 else pd.NaT)

    # Sessionized durations to avoid inflation from late stragglers
    dur_c1_min = _phase_duration_minutes(
        ts=ts,
        phase=g["_phase"],
        start_idx_after_b=b_end_idx,
        label="C1_DOC_POSTHIST",
        session_gap_hours=cfg.c1_session_gap_hours,
        window_days=cfg.c1_window_days,
        mode=cfg.c1_duration_mode,
    )
    dur_c2_min = _phase_duration_minutes(
        ts=ts,
        phase=g["_phase"],
        start_idx_after_b=b_end_idx,
        label="C2_RA_QC",
        session_gap_hours=cfg.c2_session_gap_hours,
        window_days=cfg.c2_window_days,
        mode=cfg.c2_duration_mode,
    )

    summary = dict(
        incident_id=g.iloc[0][cfg.incident_col],
        has_archival_block=True,
        doc_reviewer=b_user,
        b_start_idx=b_start_idx, b_end_idx=b_end_idx,
        t_completed=t_completed, a_tail_end=a_tail_end,
        t_archive_first=t_archive_first, t_archive_last=t_archive_last,
        t_b_start=t_b_start, t_b_end=t_b_end,
        t_c1_start=t_c1_start, t_c2_start=t_c2_start,
        dur_doc_qc_min=float(dur_doc_qc/np.timedelta64(1,"m")) if pd.notna(dur_doc_qc) else np.nan,
        dur_c1_min=float(dur_c1_min) if pd.notna(dur_c1_min) else np.nan,
        dur_c2_min=float(dur_c2_min) if pd.notna(dur_c2_min) else np.nan,
        n_events_total=int(len(g)),
        n_live=int((g["_phase"]=="A_LiveDispatch").sum()),
        n_doc_qc=int((g["_phase"]=="B_DOC_QC").sum()),
        n_c1=int((g["_phase"]=="C1_DOC_POSTHIST").sum()),
        n_c2=int((g["_phase"]=="C2_RA_QC").sum()),
    )
    return g, summary
