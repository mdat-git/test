#patch for B
# --- B starts: NEVER before A-tail end (NEW guard) ---
if b_run_start_idx is not None:
    t_b_candidate = ts.iloc[b_run_start_idx]
    if pd.notna(a_tail_end):
        t_b_candidate = max(t_b_candidate, a_tail_end)

    i = _first_index_at_or_after(ts, t_b_candidate, upper_cap=first_mgr_idx)
    if i >= len(g): 
        i = first_mgr_idx
    elif i > first_mgr_idx:
        i = first_mgr_idx

    b_start_idx = i
    if b_start_idx >= len(g):
        b_start_idx = max(0, first_mgr_idx)

    t_b_start = ts.iloc[b_start_idx]

else:
    # fallback: start at archive day while respecting A-tail
    t_b_candidate = t_archive_first
    if pd.notna(a_tail_end):
        t_b_candidate = max(t_b_candidate, a_tail_end)

    i = _first_index_at_or_after(ts, t_b_candidate, upper_cap=first_mgr_idx)
    if i >= len(g): 
        i = first_mgr_idx
    elif i > first_mgr_idx:
        i = first_mgr_idx

    b_start_idx = i
    if b_start_idx >= len(g):
        b_start_idx = max(0, first_mgr_idx)

    t_b_start = ts.iloc[b_start_idx]



## updat for A block + 2 mins, B block max lookback

# ---- PhaseConfig: add knobs for A-tail + B lookback ----
from dataclasses import dataclass
from typing import Set, Tuple, Dict, List
import pandas as pd, numpy as np, re

@dataclass
class PhaseConfig:
    incident_col: str = "INCIDENT_ID"
    time_col: str = "FOLLOWUP_DATETIME"
    insert_col: str = "INSERTED_DATE"
    desc_col: str = "FOLLOWUP_DESC"
    user_col: str = "SYSTEM_OPID"

    completed_regex: str = r"change status to\s*:?\s*Completed"
    his_manager_user: str = "CGI_HISMGR"            # archival user (canonical)
    ra_users: Set[str] = None                       # e.g., {"RA1","RA2","RA3"}
    post_archive_grace_min: int = 10                # keep same DOC reviewer’s quick fixes in B
    ignorable_users: Set[str] = None                # e.g., {"CGI_SDU_USER","CAD","SYSTEM","USEROMS"}

    # NEW: guards to prevent B from ballooning
    a_tail_minutes: int = 2                         # force A up to Completed + N minutes
    b_lookback_hours: int = 24                      # when walking back to B start, don't cross this window
    enforce_same_day_for_b: bool = False            # optional: cap B to archive-day midnight

    def __post_init__(self):
        self._pat_completed = re.compile(self.completed_regex, re.IGNORECASE)
        self._mgr_upper = self.his_manager_user.upper()
        self._ra_upper = {u.upper() for u in (self.ra_users or set())}
        self._ign_upper = {u.upper() for u in (self.ignorable_users or set())}


def _segment_single_incident(g: pd.DataFrame, cfg: PhaseConfig) -> Tuple[pd.DataFrame, Dict]:
    # sort deterministically & use RangeIndex so index-based cuts handle equal timestamps
    g = g.sort_values([cfg.time_col, cfg.insert_col], kind="stable").reset_index(drop=True)
    user = g[cfg.user_col].astype(str)
    ts   = g[cfg.time_col]

    # --- archival block (all CGI_HISMGR rows) ---
    mgr_mask = user.str.upper().eq(cfg._mgr_upper)
    mgr_idx = np.flatnonzero(mgr_mask.values)
    if mgr_idx.size == 0:
        # No archive → per policy: all A
        g["_phase"] = "A_LiveDispatch"
        return g, dict(
            incident_id=g.iloc[0][cfg.incident_col],
            has_archival_block=False,
            doc_reviewer=None,
            b_start_idx=None, b_end_idx=None,
            t_completed=pd.NaT, t_b_start=pd.NaT,
            t_archive_first=pd.NaT, t_archive_last=pd.NaT,
            t_c1_start=pd.NaT, t_c2_start=pd.NaT,
            dur_doc_qc_min=np.nan, dur_c1_min=np.nan, dur_c2_min=np.nan,
            n_events_total=len(g), n_live=len(g), n_doc_qc=0, n_c1=0, n_c2=0
        )

    first_mgr_idx = int(mgr_idx.min())
    last_mgr_idx  = int(mgr_idx.max())
    t_archive_first = ts.iloc[first_mgr_idx]
    t_archive_last  = ts.iloc[last_mgr_idx]

    # --- Completed (A-lock) + NEW: A-tail buffer ---
    t_completed = ts.loc[g["_is_completed"]].min() if g["_is_completed"].any() else pd.NaT
    a_tail_end = (t_completed + np.timedelta64(cfg.a_tail_minutes, "m")) if pd.notna(t_completed) else pd.NaT

    # --- DOC reviewer (B_user): last NON-manager user BEFORE FIRST manager row (skip ignorable) ---
    b_user = None
    k = first_mgr_idx - 1
    while k >= 0:
        u = user.iloc[k]
        u_up = (u or "").upper()
        if (u_up != "") and (u_up != cfg._mgr_upper) and (u_up not in cfg._ign_upper):
            b_user = u
            break
        k -= 1

    # Walk backward to start of the user's run,
    # allowing ignorable users but with NEW time bounds so we don't pull month-old rows.
    b_run_start_idx = None
    if b_user is not None:
        # lower bound for walking back:
        lower_time_bound = t_archive_first - np.timedelta64(cfg.b_lookback_hours, "h")
        if pd.notna(a_tail_end):
            lower_time_bound = max(lower_time_bound, a_tail_end)
        if cfg.enforce_same_day_for_b:
            lower_time_bound = max(lower_time_bound, pd.Timestamp(t_archive_first.normalize()))  # midnight of archive day

        i = k
        while i - 1 >= 0:
            u_prev_up = (user.iloc[i-1] or "").upper()
            t_prev    = ts.iloc[i-1]
            # stop if earlier than allowed window
            if pd.notna(lower_time_bound) and pd.notna(t_prev) and (t_prev < lower_time_bound):
                break
            # otherwise allow contiguous same-user OR ignorable interruptions
            if (u_prev_up == b_user.upper()) or (u_prev_up in cfg._ign_upper):
                i -= 1
            else:
                break

        # ensure run starts on a real b_user row (skip leading ignorables)
        j = i
        while j <= k and (user.iloc[j] or "").upper() in cfg._ign_upper:
            j += 1
        b_run_start_idx = j if (j <= k and (user.iloc[j] or "").upper() == b_user.upper()) else k  # fallback

    # --- B starts: NEVER before A-tail end (NEW guard) ---
    if b_run_start_idx is not None:
        t_b_candidate = ts.iloc[b_run_start_idx]
        if pd.notna(a_tail_end):
            t_b_candidate = max(t_b_candidate, a_tail_end)
        b_start_idx = int(np.flatnonzero((ts.values >= t_b_candidate).astype(bool))[0])  # handles ties
        t_b_start = ts.iloc[b_start_idx]
    else:
        # fallback: start at archive day while respecting A-tail
        t_b_candidate = t_archive_first
        if pd.notna(a_tail_end):
            t_b_candidate = max(t_b_candidate, a_tail_end)
        b_start_idx = int(np.flatnonzero((ts.values >= t_b_candidate).astype(bool))[0])
        t_b_start = ts.iloc[b_start_idx]

    # --- B initially ends right after the archival block; extend by grace if same DOC reviewer edits right after ---
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

    # --- Phase labeling with A-lock + A-tail priority (A always wins before Completed + tail) ---
    phase = pd.Series("A_LiveDispatch", index=g.index, dtype=object)

    if pd.notna(t_completed):
        phase.loc[ts < t_completed] = "A_LiveDispatch"
        # A-tail: force A from Completed through Completed + tail
        phase.loc[(ts >= t_completed) & (ts <= a_tail_end)] = "A_LiveDispatch"

    # B range
    phase.iloc[b_start_idx:b_end_idx] = "B_DOC_QC"

    # After B: RA → C2, else C1
    if b_end_idx < len(g):
        post = g.iloc[b_end_idx:]
        is_ra = post[cfg.user_col].str.upper().isin(cfg._ra_upper)
        phase.iloc[b_end_idx:] = np.where(is_ra.values, "C2_RA_QC", "C1_DOC_POSTHIST")

    g["_phase"] = phase

    # --- durations (mins) ---
    t_start = ts.min()
    t_end   = ts.max()
    t_b_end = ts.iloc[b_end_idx-1] if b_end_idx > b_start_idx else t_b_start

    dur_doc_qc = (t_b_end - t_b_start) if pd.notna(t_b_end) and pd.notna(t_b_start) else pd.NaT
    t_c1_rows = g.index[g["_phase"]=="C1_DOC_POSTHIST"]
    t_c2_rows = g.index[g["_phase"]=="C2_RA_QC"]

    dur_c1 = (ts.iloc[t_c1_rows[-1]] - ts.iloc[t_c1_rows[0]]) if len(t_c1_rows) > 0 else pd.NaT
    dur_c2 = (t_end - ts.iloc[t_c2_rows[0]]) if len(t_c2_rows) > 0 else pd.NaT

    summary = dict(
        incident_id=g.iloc[0][cfg.incident_col],
        has_archival_block=True,
        doc_reviewer=b_user,
        b_start_idx=b_start_idx, b_end_idx=b_end_idx,
        t_completed=t_completed, a_tail_end=a_tail_end,
        t_archive_first=t_archive_first, t_archive_last=t_archive_last,
        t_b_start=t_b_start, t_b_end=t_b_end,
        t_c1_start=(ts.iloc[t_c1_rows[0]] if len(t_c1_rows)>0 else pd.NaT),
        t_c2_start=(ts.iloc[t_c2_rows[0]] if len(t_c2_rows)>0 else pd.NaT),
        dur_doc_qc_min=float(dur_doc_qc/np.timedelta64(1,"m")) if pd.notna(dur_doc_qc) else np.nan,
        dur_c1_min=float(dur_c1/np.timedelta64(1,"m")) if pd.notna(dur_c1) else np.nan,
        dur_c2_min=float(dur_c2/np.timedelta64(1,"m")) if pd.notna(dur_c2) else np.nan,
        n_events_total=int(len(g)),
        n_live=int((g["_phase"]=="A_LiveDispatch").sum()),
        n_doc_qc=int((g["_phase"]=="B_DOC_QC").sum()),
        n_c1=int((g["_phase"]=="C1_DOC_POSTHIST").sum()),
        n_c2=int((g["_phase"]=="C2_RA_QC").sum()),
    )
    return g, summary













### UPDATE FOR HANDLING B BLOCK
from dataclasses import dataclass
from typing import Set, Tuple, Dict, List
import pandas as pd, numpy as np, re

@dataclass
class PhaseConfig:
    incident_col: str = "INCIDENT_ID"
    time_col: str = "FOLLOWUP_DATETIME"
    insert_col: str = "INSERTED_DATE"
    desc_col: str = "FOLLOWUP_DESC"
    user_col: str = "SYSTEM_OPID"

    completed_regex: str = r"change status to\s*:?\s*Completed"
    his_manager_user: str = "CGI_HISMGR"            # archival user (canonical)
    ra_users: Set[str] = None                       # e.g., {"RA1","RA2","RA3"}
    post_archive_grace_min: int = 10                # keep same DOC reviewer’s quick fixes in B
    ignorable_users: Set[str] = None                # e.g., {"CGI_SDU_USER","CAD","SYSTEM","USEROMS"} (optional)

    def __post_init__(self):
        self._pat_completed = re.compile(self.completed_regex, re.IGNORECASE)
        self._mgr_upper = self.his_manager_user.upper()
        self._ra_upper = {u.upper() for u in (self.ra_users or set())}
        self._ign_upper = {u.upper() for u in (self.ignorable_users or set())}

def _ensure_dt(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out and not np.issubdtype(out[c].dtype, np.datetime64):
            out[c] = pd.to_datetime(out[c], errors="coerce")
    return out

def _flag_completed(df: pd.DataFrame, cfg: PhaseConfig) -> pd.DataFrame:
    desc = df[cfg.desc_col].fillna("")
    return df.assign(_is_completed = desc.str.contains(cfg._pat_completed))

def _segment_single_incident(g: pd.DataFrame, cfg: PhaseConfig) -> Tuple[pd.DataFrame, Dict]:
    # sort deterministically & use RangeIndex so index-based cuts handle equal timestamps
    g = g.sort_values([cfg.time_col, cfg.insert_col], kind="stable").reset_index(drop=True)
    user = g[cfg.user_col].astype(str)
    ts   = g[cfg.time_col]

    # --- archival block (all CGI_HISMGR rows) ---
    mgr_mask = user.str.upper().eq(cfg._mgr_upper)
    mgr_idx = np.flatnonzero(mgr_mask.values)
    if mgr_idx.size == 0:
        # No archive → per your current policy, we keep it simple: all A
        g["_phase"] = "A_LiveDispatch"
        return g, dict(
            incident_id=g.iloc[0][cfg.incident_col],
            has_archival_block=False,
            doc_reviewer=None,
            b_start_idx=None, b_end_idx=None,
            t_completed=pd.NaT, t_b_start=pd.NaT,
            t_archive_first=pd.NaT, t_archive_last=pd.NaT,
            t_c1_start=pd.NaT, t_c2_start=pd.NaT,
            dur_doc_qc_min=np.nan, dur_c1_min=np.nan, dur_c2_min=np.nan,
            n_events_total=len(g), n_live=len(g), n_doc_qc=0, n_c1=0, n_c2=0
        )

    first_mgr_idx = int(mgr_idx.min())
    last_mgr_idx  = int(mgr_idx.max())
    t_archive_first = ts.iloc[first_mgr_idx]
    t_archive_last  = ts.iloc[last_mgr_idx]

    # --- Completed (A-lock guard) ---
    t_completed = ts.loc[g["_is_completed"]].min() if g["_is_completed"].any() else pd.NaT

    # --- DOC reviewer (B_user): last NON-manager user BEFORE FIRST manager row (skip ignorable) ---
    b_user = None
    k = first_mgr_idx - 1
    while k >= 0:
        u = user.iloc[k]
        u_up = (u or "").upper()
        if (u_up != "") and (u_up != cfg._mgr_upper) and (u_up not in cfg._ign_upper):
            b_user = u
            break
        k -= 1

    # Walk backward to start of that user's effective "contiguous" run,
    # allowing interruptions by ignorable users.
    b_run_start_idx = None
    if b_user is not None:
        i = k
        while i - 1 >= 0:
            u_prev = (user.iloc[i-1] or "").upper()
            if (u_prev == b_user.upper()) or (u_prev in cfg._ign_upper):
                i -= 1
            else:
                break
        # ensure run starts on a real b_user row (skip leading ignorable rows if any)
        j = i
        while j <= k and (user.iloc[j] or "").upper() in cfg._ign_upper:
            j += 1
        b_run_start_idx = j if j <= k and (user.iloc[j] or "").upper() == b_user.upper() else k  # fallback

    # --- B starts at start of DOC reviewer run, but NEVER before Completed (A-lock) ---
    if b_run_start_idx is not None:
        t_b_candidate = ts.iloc[b_run_start_idx]
        if pd.notna(t_completed):
            t_b = max(t_b_candidate, t_completed)
        else:
            t_b = t_b_candidate
        # first index whose time >= t_b (handles ties)
        b_start_idx = int(np.flatnonzero((ts.values >= t_b).astype(bool))[0])
    else:
        # fallback: start B at the first non-ignorable, non-manager user before manager block
        b_start_idx = max(first_mgr_idx - 1, 0)
        t_b = ts.iloc[b_start_idx]

    # --- B initially ends right after the archival block ---
    b_end_idx = last_mgr_idx + 1  # exclusive

    # --- grace: extend B if the same DOC reviewer edits shortly after archive ---
    grace = np.timedelta64(getattr(cfg, "post_archive_grace_min", 10), "m")
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

    # --- Label phases with A-lock (everything strictly before Completed is A) ---
    phase = pd.Series("A_LiveDispatch", index=g.index, dtype=object)

    # A-lock: force A where ts < t_completed
    if pd.notna(t_completed):
        phase.loc[ts < t_completed] = "A_LiveDispatch"

    # B range
    phase.iloc[b_start_idx:b_end_idx] = "B_DOC_QC"

    # After B: C2 if RA user, else C1
    if b_end_idx < len(g):
        post = g.iloc[b_end_idx:]
        is_ra = post[cfg.user_col].str.upper().isin(cfg._ra_upper)
        phase.iloc[b_end_idx:] = np.where(is_ra.values, "C2_RA_QC", "C1_DOC_POSTHIST")

    g["_phase"] = phase

    # --- durations (mins) ---
    t_start = ts.min()
    t_end   = ts.max()
    t_b_start = ts.iloc[b_start_idx]
    t_b_end   = ts.iloc[b_end_idx-1] if b_end_idx > b_start_idx else t_b_start

    dur_doc_qc = (t_b_end - t_b_start) if pd.notna(t_b_end) and pd.notna(t_b_start) else pd.NaT
    t_c1_rows = g.index[g["_phase"]=="C1_DOC_POSTHIST"]
    t_c2_rows = g.index[g["_phase"]=="C2_RA_QC"]

    dur_c1 = (ts.iloc[t_c1_rows[-1]] - ts.iloc[t_c1_rows[0]]) if len(t_c1_rows) > 0 else pd.NaT
    dur_c2 = (t_end - ts.iloc[t_c2_rows[0]]) if len(t_c2_rows) > 0 else pd.NaT

    summary = dict(
        incident_id=g.iloc[0][cfg.incident_col],
        has_archival_block=True,
        doc_reviewer=b_user,
        b_start_idx=b_start_idx, b_end_idx=b_end_idx,
        t_completed=t_completed,
        t_archive_first=t_archive_first, t_archive_last=t_archive_last,
        t_b_start=t_b_start, t_b_end=t_b_end,
        t_c1_start=(ts.iloc[t_c1_rows[0]] if len(t_c1_rows)>0 else pd.NaT),
        t_c2_start=(ts.iloc[t_c2_rows[0]] if len(t_c2_rows)>0 else pd.NaT),
        dur_doc_qc_min=float(dur_doc_qc/np.timedelta64(1,"m")) if pd.notna(dur_doc_qc) else np.nan,
        dur_c1_min=float(dur_c1/np.timedelta64(1,"m")) if pd.notna(dur_c1) else np.nan,
        dur_c2_min=float(dur_c2/np.timedelta64(1,"m")) if pd.notna(dur_c2) else np.nan,
        n_events_total=int(len(g)),
        n_live=int((g["_phase"]=="A_LiveDispatch").sum()),
        n_doc_qc=int((g["_phase"]=="B_DOC_QC").sum()),
        n_c1=int((g["_phase"]=="C1_DOC_POSTHIST").sum()),
        n_c2=int((g["_phase"]=="C2_RA_QC").sum()),
    )
    return g, summary












def _segment_single_incident(g: pd.DataFrame, cfg: PhaseConfig) -> Tuple[pd.DataFrame, Dict]:
    # Sort deterministically and use RangeIndex so index-based cuts survive equal timestamps
    g = g.sort_values([cfg.time_col, cfg.insert_col], kind="stable").reset_index(drop=True)

    # --- locate archival block (all CGI_HISMGR rows) ---
    mgr_mask = g[cfg.user_col].str.upper().eq(cfg._mgr_upper)
    mgr_idx = np.flatnonzero(mgr_mask.values)

    if mgr_idx.size > 0:
        first_mgr_idx = int(mgr_idx.min())
        last_mgr_idx  = int(mgr_idx.max())
        c_start_idx   = last_mgr_idx + 1                         # C begins AFTER the archival block
        t_archive_first = g.loc[first_mgr_idx, cfg.time_col]
        t_archive_last  = g.loc[last_mgr_idx,  cfg.time_col]
    else:
        first_mgr_idx = last_mgr_idx = None
        c_start_idx = None
        t_archive_first = pd.NaT
        t_archive_last  = pd.NaT

    # --- Completed (for latency + B guard) ---
    t_completed = g.loc[g["_is_completed"], cfg.time_col].min() if g["_is_completed"].any() else pd.NaT

    # --- DOC reviewer (B_user): last NON-MANAGER user BEFORE the FIRST manager row ---
    b_user = None
    b_run_start_idx = None
    if first_mgr_idx is not None:
        k = first_mgr_idx - 1
        while k >= 0:
            u = g.loc[k, cfg.user_col]
            if pd.notna(u) and (str(u).upper() != cfg._mgr_upper):
                b_user = u
                break
            k -= 1

        if b_user is not None:
            # walk backward to start of this user's contiguous run
            i = k
            while i - 1 >= 0 and g.loc[i - 1, cfg.user_col] == b_user:
                i -= 1
            b_run_start_idx = i

    # --- B start (time guarded by Completed) and its index ---
    b_start_idx = None
    t_b = pd.NaT
    if b_run_start_idx is not None:
        t_b = g.loc[b_run_start_idx, cfg.time_col]
        if pd.notna(t_completed):
            t_b = max(t_b, t_completed)                   # never start B before Completed
        b_start_idx = int(np.flatnonzero((g[cfg.time_col] >= t_b).values)[0])
    elif (first_mgr_idx is None) and pd.notna(t_completed):
        # no manager block: B starts at first row AFTER Completed
        idx_after_completed = np.flatnonzero((g[cfg.time_col] > t_completed).values)
        if idx_after_completed.size > 0:
            b_start_idx = int(idx_after_completed[0])
            t_b = g.loc[b_start_idx, cfg.time_col]
            # pick reviewer as first non-null user at/after B start
            uu = g.loc[b_start_idx:, cfg.user_col].dropna()
            b_user = None if uu.empty else uu.iloc[0]

    # --- Phase labeling by index slices ---
    phase = pd.Series("A_LiveDispatch", index=g.index, dtype=object)
    if b_start_idx is not None:
        phase.iloc[:b_start_idx] = "A_LiveDispatch"
        if c_start_idx is not None:
            phase.iloc[b_start_idx:c_start_idx] = "B_DOC_QC"   # includes the CGI_HISMGR archival block
            if c_start_idx < len(g):
                phase.iloc[c_start_idx:] = "C_RA_QC"
        else:
            phase.iloc[b_start_idx:] = "B_DOC_QC"
    else:
        if c_start_idx is not None:
            phase.iloc[:c_start_idx] = "A_LiveDispatch"
            if c_start_idx < len(g):
                phase.iloc[c_start_idx:] = "C_RA_QC"
        else:
            phase[:] = "A_LiveDispatch"

    g["_phase"] = phase

    # --- times for durations ---
    t_start    = g[cfg.time_col].min()
    t_c_start  = g.loc[c_start_idx, cfg.time_col] if (c_start_idx is not None and c_start_idx < len(g)) else pd.NaT
    t_end      = g[cfg.time_col].max()

    dur_live   = (t_b - t_start)           if (pd.notna(t_b) and pd.notna(t_start)) else pd.NaT
    dur_doc_qc = (t_archive_last - t_b)    if (pd.notna(t_archive_last) and pd.notna(t_b)) else pd.NaT
    dur_ra_qc  = (t_end - t_c_start)       if pd.notna(t_c_start) else pd.NaT

    latency_completed_to_archive = (
        (t_archive_first - t_completed) if (pd.notna(t_archive_first) and pd.notna(t_completed)) else pd.NaT
    )

    # users per phase
    users_live = g.loc[g["_phase"]=="A_LiveDispatch", cfg.user_col].dropna().unique().tolist()
    users_doc  = g.loc[g["_phase"]=="B_DOC_QC",      cfg.user_col].dropna().unique().tolist()
    users_ra   = g.loc[g["_phase"]=="C_RA_QC",       cfg.user_col].dropna().unique().tolist()

    summary = dict(
        incident_id=g[cfg.incident_col].iloc[0],
        # indexes
        b_start_idx=b_start_idx,
        c_start_idx=c_start_idx,
        first_mgr_idx=first_mgr_idx,
        last_mgr_idx=last_mgr_idx,
        # anchors
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
