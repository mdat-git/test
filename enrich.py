def _count_contiguous_runs(idxs: np.ndarray) -> int:
    """Count contiguous runs in a sorted integer array (e.g., manager row indices)."""
    if idxs.size == 0:
        return 0
    diffs = np.diff(idxs)
    return int((diffs > 1).sum() + 1)

def _sessions_count(times: pd.Series, gap_hours: int) -> int:
    """How many sessions in a sorted datetime Series with a given inactivity gap."""
    if times.empty:
        return 0
    if len(times) == 1:
        return 1
    diffs = times.sort_values().diff()
    return int((diffs > pd.Timedelta(hours=gap_hours)).sum() + 1)

def enrich_incident_summary(
    events_labeled: pd.DataFrame,
    incident_summary: pd.DataFrame,
    cfg: PhaseConfig,
    dim_incident: pd.DataFrame | None = None,   # optional: to attach district/device/etc.
) -> pd.DataFrame:
    """
    Returns an enhanced incident summary with: durations, phase mix, densities,
    sessions, late-edit flags, step counts, reopened count, ra_primary_user, and guard flags.
    """

    # --- Ensure minimal columns present
    need_evt = {cfg.incident_col, cfg.time_col, cfg.user_col, "_phase"}
    miss_evt = [c for c in need_evt if c not in events_labeled.columns]
    if miss_evt:
        raise KeyError(f"events_labeled missing columns: {miss_evt}")

    need_sum = {"incident_id","t_start","t_end","t_b_start","t_b_end","t_archive_last",
                "t_c1_start","t_c2_start","dur_doc_qc_min","dur_c1_min","dur_c2_min",
                "a_tail_end","doc_reviewer","b_start_idx","b_end_idx"}
    miss_sum = [c for c in need_sum if c not in incident_summary.columns]
    if miss_sum:
        # fallbacks if some timestamps aren't present
        pass

    # --- Step counts per phase
    step_counts = (
        events_labeled
        .groupby([cfg.incident_col, "_phase"])
        .size()
        .unstack(fill_value=0)
        .rename(columns={
            "A_LiveDispatch":"n_A_steps",
            "B_DOC_QC":"n_B_steps",
            "C1_DOC_POSTHIST":"n_C1_steps",
            "C2_RA_QC":"n_C2_steps"
        })
        .reset_index()
        .rename(columns={cfg.incident_col: "incident_id"})
    )

    # --- Reopened count (how many CGI_HISMGR blocks in the whole incident)
    def count_his_blocks(g):
        idxs = np.flatnonzero(g[cfg.user_col].astype(str).str.upper().eq(cfg._mgr_upper).values)
        return _count_contiguous_runs(idxs)

    reopened = (
        events_labeled
        .sort_values([cfg.incident_col, cfg.time_col, cfg.insert_col if cfg.insert_col in events_labeled.columns else cfg.time_col])
        .groupby(cfg.incident_col, sort=False)
        .apply(count_his_blocks)
        .rename("reopened_blocks")
        .reset_index()
        .rename(columns={cfg.incident_col:"incident_id"})
    )

    # --- RA primary user (first RA after B)
    def ra_primary(g):
        ra = g[g["_phase"]=="C2_RA_QC"]
        return ra[cfg.user_col].iloc[0] if not ra.empty else np.nan

    ra1 = (
        events_labeled
        .sort_values([cfg.incident_col, cfg.time_col, cfg.insert_col if cfg.insert_col in events_labeled.columns else cfg.time_col])
        .groupby(cfg.incident_col, sort=False)
        .apply(ra_primary)
        .rename("ra_primary_user")
        .reset_index()
        .rename(columns={cfg.incident_col:"incident_id"})
    )

    # --- Compose base frame
    out = incident_summary.copy()

    # Durations: live (A) and total tracked
    out["incident_span_min"] = (out["t_end"] - out["t_start"]) / np.timedelta64(1,"m")
    out["dur_live_min"] = (out["t_b_start"] - out["t_start"]) / np.timedelta64(1,"m")

    # Phase total tracked = B + C1 + C2 (sessionized for C1/C2)
    out["phase_total_tracked_min"] = (
        out["dur_doc_qc_min"].fillna(0) +
        out["dur_c1_min"].fillna(0) +
        out["dur_c2_min"].fillna(0)
    )

    # Shares (guard against 0/NaN)
    denom = out["phase_total_tracked_min"].replace(0, np.nan)
    out["share_B"]  = out["dur_doc_qc_min"] / denom
    out["share_C1"] = out["dur_c1_min"]     / denom
    out["share_C2"] = out["dur_c2_min"]     / denom

    # Flags
    out["a_tail_used"] = out["a_tail_end"].notna()
    # Grace used if B extended past archive last timestamp
    out["b_grace_used"] = (out["t_b_end"] > out["t_archive_last"])
    # Same-day B
    out["same_day_B"] = (out["t_b_start"].dt.normalize() == out["t_archive_last"].dt.normalize())

    # Lags (re-assert)
    out["completed_to_archive_min"] = (out["t_archive_last"] - out["t_b_start"] + (out["t_b_start"] - out["t_b_start"])) / np.timedelta64(1,"m")
    # Better: if you stored t_completed and t_archive_first in summary, do:
    if "t_completed" in out.columns and "t_archive_first" in out.columns:
        out["completed_to_archive_min"] = (out["t_archive_first"] - out["t_completed"]) / np.timedelta64(1,"m")

    out["lag_B_to_C2_min"] = np.where(
        out["t_c2_start"].notna(),
        (out["t_c2_start"] - out["t_archive_last"]) / np.timedelta64(1,"m"),
        np.nan
    )

    # Join step counts, reopened count, RA primary user
    out = out.merge(step_counts, on="incident_id", how="left")
    out = out.merge(reopened,     on="incident_id", how="left")
    out = out.merge(ra1,          on="incident_id", how="left")

    # NaN-safe zeros for steps
    for c in ["n_A_steps","n_B_steps","n_C1_steps","n_C2_steps"]:
        if c in out.columns:
            out[c] = out[c].fillna(0).astype(int)

    # Effort densities (steps per hour)
    def density(steps, minutes):
        return np.where(minutes>0, steps / (minutes/60.0), np.nan)

    out["B_steps_per_hr"]  = density(out.get("n_B_steps",0),  out["dur_doc_qc_min"])
    out["C1_steps_per_hr"] = density(out.get("n_C1_steps",0), out["dur_c1_min"])
    out["C2_steps_per_hr"] = density(out.get("n_C2_steps",0), out["dur_c2_min"])

    # Sessions + late edits from events_labeled (post-B by definition for C1/C2 labels)
    def session_meta(g, label, gap_hours, window_days):
        s = g.loc[g["_phase"]==label, cfg.time_col].sort_values()
        sessions = _sessions_count(s, gap_hours)
        if s.empty:
            return pd.Series({"sessions":0,"late":False})
        first = s.iloc[0]
        win_end = first + pd.Timedelta(days=window_days)
        late = (s.iloc[-1] > win_end)  # anything past the window = late
        return pd.Series({"sessions":sessions,"late":bool(late)})

    sess = (
        events_labeled
        .groupby(cfg.incident_col, sort=False)
        .apply(lambda g: pd.concat([
            session_meta(g, "C1_DOC_POSTHIST", cfg.c1_session_gap_hours, cfg.c1_window_days).add_prefix("c1_"),
            session_meta(g, "C2_RA_QC",       cfg.c2_session_gap_hours, cfg.c2_window_days).add_prefix("c2_"),
            pd.Series({
                "doc_reviewer_same_as_dispatcher":
                  bool(set(g.loc[g["_phase"]=="A_LiveDispatch", cfg.user_col].dropna().unique())
                       & set([incident_summary.loc[incident_summary["incident_id"]==g[cfg.incident_col].iloc[0], "doc_reviewer"].fillna("").iloc[0]]))
            })
        ]))
        .reset_index()
        .rename(columns={cfg.incident_col:"incident_id"})
    )
    out = out.merge(sess, on="incident_id", how="left")

    # Attach incident metadata (district/device/etc.) if provided
    if dim_incident is not None and cfg.incident_col in dim_incident.columns:
        meta_cols = [c for c in dim_incident.columns if c != cfg.incident_col]
        out = out.merge(dim_incident[[cfg.incident_col] + meta_cols]
                        .drop_duplicates(cfg.incident_col),
                        left_on="incident_id", right_on=cfg.incident_col, how="left") \
                 .drop(columns=[cfg.incident_col])

    # (Optional) wire in change-impact flags if your events have them:
    tag_cols = {"tag_change_cause","tag_change_occur","tag_change_times"}
    if tag_cols.issubset(set(events_labeled.columns)):
        def change_flags(g):
            ra = g[g["_phase"]=="C2_RA_QC"]
            doc= g[g["_phase"]=="B_DOC_QC"]
            return pd.Series({
                "ra_changed_cause":  int(ra["tag_change_cause"].fillna(False).any()),
                "ra_changed_occur":  int(ra["tag_change_occur"].fillna(False).any()),
                "ra_changed_times":  int(ra["tag_change_times"].fillna(False).any()),
                "doc_changed_cause": int(doc["tag_change_cause"].fillna(False).any()),
                "doc_changed_occur": int(doc["tag_change_occur"].fillna(False).any()),
                "doc_changed_times": int(doc["tag_change_times"].fillna(False).any()),
            })
        chg = (events_labeled.groupby(cfg.incident_col)
               .apply(change_flags).reset_index()
               .rename(columns={cfg.incident_col:"incident_id"}))
        out = out.merge(chg, on="incident_id", how="left")

    # Make percentages nice for BI if you want (leave as float 0-1 for now)
    return out
