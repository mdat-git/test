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
    dim_incident: pd.DataFrame | None = None,   # optional: attach district/device/etc.
) -> pd.DataFrame:
    """
    Enhance per-incident summary with spans, phase shares, step counts/densities,
    sessions, late-edit flags, reopened count, RA primary user, and process flags.
    Safe if t_start/t_end are absent in incident_summary (they're derived from events).
    """

    # ---------- Required columns check ----------
    need_evt = {cfg.incident_col, cfg.time_col, cfg.user_col, "_phase"}
    miss_evt = [c for c in need_evt if c not in events_labeled.columns]
    if miss_evt:
        raise KeyError(f"events_labeled missing columns: {miss_evt}")

    out = incident_summary.copy()

    # ---------- Ensure t_start / t_end exist ----------
    if ("t_start" not in out.columns) or ("t_end" not in out.columns):
        base_times = (events_labeled
                      .groupby(cfg.incident_col)[cfg.time_col]
                      .agg(t_start="min", t_end="max")
                      .reset_index()
                      .rename(columns={cfg.incident_col: "incident_id"}))
        out = out.merge(base_times, on="incident_id", how="left")

    # Fill if still missing
    if "t_start" not in out.columns: out["t_start"] = pd.NaT
    if "t_end"   not in out.columns: out["t_end"]   = pd.NaT

    # ---------- Step counts per phase ----------
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

    # ---------- Reopened count (how many CGI_HISMGR blocks) ----------
    def count_his_blocks(g):
        idxs = np.flatnonzero(g[cfg.user_col].astype(str).str.upper().eq(cfg._mgr_upper).values)
        return _count_contiguous_runs(idxs)

    reopened = (
        events_labeled
        .sort_values([cfg.incident_col, cfg.time_col,
                      cfg.insert_col if cfg.insert_col in events_labeled.columns else cfg.time_col])
        .groupby(cfg.incident_col, sort=False)
        .apply(count_his_blocks)
        .rename("reopened_blocks")
        .reset_index()
        .rename(columns={cfg.incident_col:"incident_id"})
    )

    # ---------- RA primary user (first RA after B) ----------
    def ra_primary(g):
        ra = g[g["_phase"]=="C2_RA_QC"]
        return ra[cfg.user_col].iloc[0] if not ra.empty else np.nan

    ra1 = (
        events_labeled
        .sort_values([cfg.incident_col, cfg.time_col,
                      cfg.insert_col if cfg.insert_col in events_labeled.columns else cfg.time_col])
        .groupby(cfg.incident_col, sort=False)
        .apply(ra_primary)
        .rename("ra_primary_user")
        .reset_index()
        .rename(columns={cfg.incident_col:"incident_id"})
    )

    # ---------- Core time features ----------
    # Spans (NaN-safe)
    out["incident_span_min"] = ((out["t_end"] - out["t_start"]) / np.timedelta64(1, "m")).astype("float")
    out["dur_live_min"] = ((out["t_b_start"] - out["t_start"]) / np.timedelta64(1, "m")).astype("float")

    # Completed → Archive latency (only when both present)
    t_completed = out.get("t_completed", pd.Series(index=out.index, dtype="datetime64[ns]"))
    t_arch_first = out.get("t_archive_first", pd.Series(index=out.index, dtype="datetime64[ns]"))
    out["completed_to_archive_min"] = np.where(
        t_completed.notna() & t_arch_first.notna(),
        (t_arch_first - t_completed) / np.timedelta64(1, "m"),
        np.nan
    ).astype("float")

    # Lag from B end (archive end) to first RA
    t_b_end = out.get("t_b_end", pd.Series(index=out.index, dtype="datetime64[ns]"))
    t_c2_start = out.get("t_c2_start", pd.Series(index=out.index, dtype="datetime64[ns]"))
    t_arch_last = out.get("t_archive_last", pd.Series(index=out.index, dtype="datetime64[ns]"))
    out["lag_B_to_C2_min"] = np.where(
        t_c2_start.notna() & t_arch_last.notna(),
        (t_c2_start - t_arch_last) / np.timedelta64(1, "m"),
        np.nan
    ).astype("float")

    # ---------- Phase totals & shares ----------
    out["phase_total_tracked_min"] = (
        out.get("dur_doc_qc_min", np.nan).fillna(0) +
        out.get("dur_c1_min", np.nan).fillna(0) +
        out.get("dur_c2_min", np.nan).fillna(0)
    )
    denom = out["phase_total_tracked_min"].replace(0, np.nan)
    out["share_B"]  = out.get("dur_doc_qc_min", np.nan) / denom
    out["share_C1"] = out.get("dur_c1_min", np.nan)      / denom
    out["share_C2"] = out.get("dur_c2_min", np.nan)      / denom

    # ---------- Process flags ----------
    out["a_tail_used"] = out.get("a_tail_end", pd.Series(index=out.index)).notna()
    out["b_grace_used"] = np.where(
        t_b_end.notna() & t_arch_last.notna(),
        t_b_end > t_arch_last,
        False
    )
    out["same_day_B"] = np.where(
        out.get("t_b_start", pd.Series(index=out.index, dtype="datetime64[ns]")).notna() &
        t_arch_last.notna(),
        out["t_b_start"].dt.normalize() == t_arch_last.dt.normalize(),
        np.nan
    )

    # ---------- Join step counts / reopened / RA primary ----------
    out = out.merge(step_counts, on="incident_id", how="left")
    out = out.merge(reopened,     on="incident_id", how="left")
    out = out.merge(ra1,          on="incident_id", how="left")

    for c in ["n_A_steps","n_B_steps","n_C1_steps","n_C2_steps"]:
        if c in out.columns:
            out[c] = out[c].fillna(0).astype(int)
        else:
            out[c] = 0

    # ---------- Effort densities (steps per hour) ----------
    def density(steps, minutes):
        return np.where((minutes > 0) & np.isfinite(minutes), steps / (minutes/60.0), np.nan)

    out["B_steps_per_hr"]  = density(out["n_B_steps"],  out.get("dur_doc_qc_min", np.nan))
    out["C1_steps_per_hr"] = density(out["n_C1_steps"], out.get("dur_c1_min", np.nan))
    out["C2_steps_per_hr"] = density(out["n_C2_steps"], out.get("dur_c2_min", np.nan))

    # ---------- Sessions & late flags from events (post-B phases by construction) ----------
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
            session_meta(g, "C1_DOC_POSTHIST", getattr(cfg, "c1_session_gap_hours", 2), getattr(cfg, "c1_window_days", 3)).add_prefix("c1_"),
            session_meta(g, "C2_RA_QC",       getattr(cfg, "c2_session_gap_hours", 6), getattr(cfg, "c2_window_days", 14)).add_prefix("c2_"),
        ]))
        .reset_index()
        .rename(columns={cfg.incident_col:"incident_id"})
    )
    out = out.merge(sess, on="incident_id", how="left")

    # ---------- Optional: change-impact flags if event tags exist ----------
    tag_cols = {"tag_change_cause","tag_change_occur","tag_change_times"}
    if tag_cols.issubset(set(events_labeled.columns)):
        def change_flags(g):
            ra  = g[g["_phase"]=="C2_RA_QC"]
            doc = g[g["_phase"]=="B_DOC_QC"]
            return pd.Series({
                "ra_changed_cause":  int(ra.get("tag_change_cause", pd.Series(dtype=bool)).fillna(False).any()),
                "ra_changed_occur":  int(ra.get("tag_change_occur",  pd.Series(dtype=bool)).fillna(False).any()),
                "ra_changed_times":  int(ra.get("tag_change_times",  pd.Series(dtype=bool)).fillna(False).any()),
                "doc_changed_cause": int(doc.get("tag_change_cause", pd.Series(dtype=bool)).fillna(False).any()),
                "doc_changed_occur": int(doc.get("tag_change_occur",  pd.Series(dtype=bool)).fillna(False).any()),
                "doc_changed_times": int(doc.get("tag_change_times",  pd.Series(dtype=bool)).fillna(False).any()),
            })
        chg = (events_labeled.groupby(cfg.incident_col)
               .apply(change_flags).reset_index()
               .rename(columns={cfg.incident_col:"incident_id"}))
        out = out.merge(chg, on="incident_id", how="left")

    # ---------- Attach meta (district/device/etc.) if provided ----------
    if dim_incident is not None and cfg.incident_col in dim_incident.columns:
        meta_cols = [c for c in dim_incident.columns if c != cfg.incident_col]
        out = out.merge(
            dim_incident[[cfg.incident_col] + meta_cols].drop_duplicates(cfg.incident_col),
            left_on="incident_id", right_on=cfg.incident_col, how="left"
        ).drop(columns=[cfg.incident_col])

    return out
