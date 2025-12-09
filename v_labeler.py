# label_p1_app.py

import streamlit as st
import pandas as pd
from pathlib import Path

# --------- CONFIG ---------
DATA_PATH = Path("p1_incidents_with_replies.csv")
LABEL_COL = "label"

LABEL_OPTIONS = {
    "P1_CONFIRMED": "P1 confirmed",
    "P2_DOWNGRADED": "Downgraded to P2",
    "NOT_P1": "Not P1 / reject",
    "FOLLOW_UP": "Needs more info / attach photos",
    "OTHER": "Other / unclear",
}
# --------------------------


@st.cache_data
def load_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Ensure label column exists
    if LABEL_COL not in df.columns:
        df[LABEL_COL] = pd.NA
    return df


def save_data(df: pd.DataFrame, path: Path):
    df.to_csv(path, index=False)


def get_unlabeled_indices(df: pd.DataFrame) -> list[int]:
    return df[df[LABEL_COL].isna()].index.tolist()


def main():
    st.set_page_config(
        page_title="P1 Labeling UI",
        layout="wide",
    )

    st.title("🔖 P1 Incident Labeling")

    if not DATA_PATH.exists():
        st.error(f"CSV not found: {DATA_PATH}")
        st.stop()

    # Load into session_state once
    if "df" not in st.session_state:
        st.session_state.df = load_data(DATA_PATH)

    df = st.session_state.df

    # Track current index in session_state
    if "current_idx" not in st.session_state:
        unlabeled = get_unlabeled_indices(df)
        st.session_state.current_idx = unlabeled[0] if unlabeled else 0

    # Progress
    total = len(df)
    labeled_count = df[LABEL_COL].notna().sum()
    unlabeled_count = total - labeled_count
    st.sidebar.markdown("### Progress")
    st.sidebar.write(f"Total incidents: **{total}**")
    st.sidebar.write(f"Labeled: **{labeled_count}**")
    st.sidebar.write(f"Remaining: **{unlabeled_count}**")
    st.sidebar.progress(labeled_count / total if total else 0.0)

    # Navigation helpers
    def go_to_next_unlabeled():
        unlabeled = get_unlabeled_indices(df)
        if not unlabeled:
            return
        # find next unlabeled after current_idx
        after = [i for i in unlabeled if i > st.session_state.current_idx]
        st.session_state.current_idx = (after[0] if after else unlabeled[0])

    def go_to_prev():
        if st.session_state.current_idx > 0:
            st.session_state.current_idx -= 1

    idx = st.session_state.current_idx
    if idx < 0 or idx >= total:
        st.write("No records to show.")
        st.stop()

    row = df.loc[idx]

    st.caption(f"Row {idx + 1} / {total}")

    # --- TOP META ---
    col1, col2, col3 = st.columns([2, 2, 1])

    with col1:
        st.markdown(f"**Conversation ID:** `{row.get('conversation_id', '')}`")
        st.markdown(f"**Subject:** {row.get('subject', '')}")

    with col2:
        st.markdown(f"**Received:** {row.get('received_time', '')}")
        st.markdown(f"**Sender:** {row.get('sender', '')}")

    with col3:
        current_label = row.get(LABEL_COL, pd.NA)
        st.markdown("**Current label:**")
        st.write(str(current_label) if pd.notna(current_label) else "❌ (unlabeled)")

    st.markdown("---")

    # --- CONTEXT TEXT FIELDS ---
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Problem Statement")
        ps = str(row.get("problem_statement", "") or "").strip()
        st.text(ps or "(none)")

        st.subheader("Comments")
        cm = str(row.get("comments", "") or "").strip()
        st.text(cm or "(none)")

    with c2:
        st.subheader("First Reply")
        fr = str(row.get("first_reply_text", "") or "").strip()
        st.text(fr or "(none)")

        with st.expander("All Replies (full thread text)", expanded=False):
            all_rep = str(row.get("all_replies_text", "") or "").strip()
            st.text(all_rep or "(none)")

    st.markdown("---")

    # --- LABEL BUTTONS ---
    st.subheader("Assign label")

    cols = st.columns(len(LABEL_OPTIONS) + 2)

    # label buttons
    for (i, (code, desc)) in enumerate(LABEL_OPTIONS.items()):
        if cols[i].button(f"{code}", help=desc):
            df.at[idx, LABEL_COL] = code
            save_data(df, DATA_PATH)
            go_to_next_unlabeled()
            st.experimental_rerun()

    # navigation buttons
    if cols[-2].button("⏮️ Previous"):
        go_to_prev()
        st.experimental_rerun()

    if cols[-1].button("⏭️ Next unlabeled"):
        go_to_next_unlabeled()
        st.experimental_rerun()

    st.info(
        "Tip: Click a label button to save & jump to the next unlabeled row. "
        "Use **Previous** to review earlier ones."
    )


if __name__ == "__main__":
    main()
