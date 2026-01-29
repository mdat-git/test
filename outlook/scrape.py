import datetime as dt
import re

import numpy as np
import pandas as pd
import win32com.client


def get_outlook_folder(
    *,
    mailbox_display_name: str | None,
    folder_path: list[str],
):
    """
    mailbox_display_name:
      - None -> your default mailbox
      - otherwise the exact Display Name of a shared mailbox you have access to

    folder_path examples:
      ["Inbox"]
      ["Inbox", "LiDAR Notifications"]
    """
    outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")

    if mailbox_display_name:
        root = outlook.Folders.Item(mailbox_display_name)
    else:
        root = outlook.GetDefaultFolder(6).Parent  # 6 = Inbox; Parent = mailbox root

    folder = root
    for name in folder_path:
        folder = folder.Folders.Item(name)

    return folder


def looks_like_reply_or_forward(subject: str) -> bool:
    if not subject:
        return False
    s = subject.strip().lower()
    return s.startswith("re:") or s.startswith("fw:") or s.startswith("fwd:")


def normalize_subject(subject: str) -> str:
    if not subject:
        return ""
    s = subject.strip()
    s = re.sub(r"^(re:|fw:|fwd:)\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s)
    return s.lower()



##3
def scrape_messages(
    *,
    mailbox_display_name: str | None,
    folder_path: list[str],
    start_date: dt.datetime,
    end_date: dt.datetime,
    max_items: int | None = None,
) -> pd.DataFrame:
    """
    Scrapes Outlook emails in [start_date, end_date) from the target folder.
    """
    folder = get_outlook_folder(
        mailbox_display_name=mailbox_display_name,
        folder_path=folder_path,
    )

    items = folder.Items
    items.Sort("[ReceivedTime]", True)  # descending

    # Outlook Restrict often wants mm/dd/yyyy hh:mm AM/PM
    def fmt(d: dt.datetime) -> str:
        return d.strftime("%m/%d/%Y %I:%M %p")

    restriction = (
        f"[ReceivedTime] >= '{fmt(start_date)}' AND [ReceivedTime] < '{fmt(end_date)}'"
    )
    restricted = items.Restrict(restriction)

    rows = []
    count = 0

    for msg in restricted:
        try:
            # 43 = MailItem
            if getattr(msg, "Class", None) != 43:
                continue

            received = msg.ReceivedTime
            subject = getattr(msg, "Subject", "") or ""
            sender_name = getattr(msg, "SenderName", "") or ""

            try:
                sender_email = getattr(msg, "SenderEmailAddress", "") or ""
            except Exception:
                sender_email = ""

            conv_id = getattr(msg, "ConversationID", "") or ""
            conv_topic = getattr(msg, "ConversationTopic", "") or ""

            rows.append(
                {
                    "received_time": pd.to_datetime(received),
                    "received_date": pd.to_datetime(received).date(),
                    "subject": subject,
                    "subject_norm": normalize_subject(subject),
                    "is_reply_fwd_subject": looks_like_reply_or_forward(subject),
                    "sender_name": sender_name,
                    "sender_email": sender_email,
                    "conversation_id": conv_id,
                    "conversation_topic": conv_topic,
                    "entry_id": getattr(msg, "EntryID", ""),
                }
            )

            count += 1
            if max_items and count >= max_items:
                break

        except Exception:
            continue

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.sort_values("received_time").reset_index(drop=True)
    return df


#4

# ---- CONFIGURE THESE ----
MAILBOX_DISPLAY_NAME = "LiDAR Shared Mailbox"   # set None for your own mailbox
FOLDER_PATH = ["Inbox"]                        # e.g. ["Inbox", "LiDAR Notifications"]

# Time window (last 30 days)
end = dt.datetime.now()
start = end - dt.timedelta(days=30)

df = scrape_messages(
    mailbox_display_name=MAILBOX_DISPLAY_NAME,
    folder_path=FOLDER_PATH,
    start_date=start,
    end_date=end,
    max_items=None,   # or set to 20000 if you want a cap
)

print(f"Pulled {len(df):,} messages")
df.head(10)

#5
daily_all = (
    df.groupby("received_date")
    .size()
    .rename("email_count")
    .reset_index()
    .sort_values("received_date")
)

daily_all.tail(15)


#6
df["thread_key"] = np.where(
    df["conversation_id"].fillna("").str.len() > 0,
    df["conversation_id"],
    df["subject_norm"],  # fallback if ConversationID is missing
)

first_in_thread = (
    df.sort_values("received_time")
    .groupby("thread_key", as_index=False)
    .first()
)

daily_initial = (
    first_in_thread.groupby(first_in_thread["received_time"].dt.date)
    .size()
    .rename("initial_thread_count")
    .reset_index()
    .rename(columns={"received_time": "received_date"})
    .sort_values("received_date")
)

daily_initial.tail(15)

#7
df_sorted = df.sort_values("received_time").copy()
df_sorted["delta_minutes"] = df_sorted["received_time"].diff().dt.total_seconds() / 60.0

deltas = df_sorted["delta_minutes"].dropna()

interarrival_summary = pd.DataFrame(
    {
        "metric": [
            "count_intervals",
            "mean_minutes",
            "median_minutes",
            "p10_minutes",
            "p25_minutes",
            "p75_minutes",
            "p90_minutes",
            "min_minutes",
            "max_minutes",
        ],
        "value": [
            len(deltas),
            deltas.mean() if len(deltas) else np.nan,
            deltas.median() if len(deltas) else np.nan,
            deltas.quantile(0.10) if len(deltas) else np.nan,
            deltas.quantile(0.25) if len(deltas) else np.nan,
            deltas.quantile(0.75) if len(deltas) else np.nan,
            deltas.quantile(0.90) if len(deltas) else np.nan,
            deltas.min() if len(deltas) else np.nan,
            deltas.max() if len(deltas) else np.nan,
        ],
    }
)

interarrival_summary


#8
df_sorted["date"] = df_sorted["received_time"].dt.date

interarrival_by_day = (
    df_sorted.dropna(subset=["delta_minutes"])
    .groupby("date")["delta_minutes"]
    .agg(
        intervals="count",
        mean_minutes="mean",
        median_minutes="median",
        p10_minutes=lambda s: s.quantile(0.10),
        p90_minutes=lambda s: s.quantile(0.90),
    )
    .reset_index()
    .sort_values("date")
)

interarrival_by_day.tail(15)


#9
hourly = (
    df.groupby(df["received_time"].dt.hour)
    .size()
    .rename("email_count")
    .reset_index()
    .rename(columns={"received_time": "hour"})
    .sort_values("hour")
)

hourly


#10
burst_rows = []
for n in [1, 2, 5, 10, 30]:
    pct = float((deltas <= n).mean() * 100 if len(deltas) else 0.0)
    burst_rows.append({"within_minutes": n, "pct_of_emails": pct})

burstiness = pd.DataFrame(burst_rows)
burstiness

#11
top_senders = (
    df.groupby(["sender_name", "sender_email"])
    .size()
    .rename("email_count")
    .reset_index()
    .sort_values("email_count", ascending=False)
    .head(25)
)

top_topics = (
    df["conversation_topic"]
    .fillna("")
    .replace("", np.nan)
    .dropna()
    .value_counts()
    .head(25)
    .rename_axis("conversation_topic")
    .reset_index(name="email_count")
)

top_senders, top_topics


#12
out_dir = "."  # change as needed

df.to_csv(f"{out_dir}/lidar_inbox_raw.csv", index=False)
daily_all.to_csv(f"{out_dir}/daily_all_emails.csv", index=False)
daily_initial.to_csv(f"{out_dir}/daily_initial_emails.csv", index=False)
interarrival_summary.to_csv(f"{out_dir}/interarrival_summary.csv", index=False)
interarrival_by_day.to_csv(f"{out_dir}/interarrival_by_day.csv", index=False)
hourly.to_csv(f"{out_dir}/hourly_distribution.csv", index=False)
burstiness.to_csv(f"{out_dir}/burstiness.csv", index=False)
top_senders.to_csv(f"{out_dir}/top_senders.csv", index=False)
top_topics.to_csv(f"{out_dir}/top_conversation_topics.csv", index=False)

print("Exported CSVs.")



#13 b
# Cell 12 — Export everything into ONE Excel workbook (multiple tabs)

import pandas as pd

out_path = "lidar_inbox_analysis.xlsx"  # change filename/path as needed

with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
    # Raw data
    df.to_excel(writer, sheet_name="raw_emails", index=False)

    # Key summaries
    daily_all.to_excel(writer, sheet_name="daily_all_emails", index=False)
    daily_initial.to_excel(writer, sheet_name="daily_initial_threads", index=False)
    interarrival_summary.to_excel(writer, sheet_name="interarrival_summary", index=False)
    interarrival_by_day.to_excel(writer, sheet_name="interarrival_by_day", index=False)
    hourly.to_excel(writer, sheet_name="hourly_distribution", index=False)
    burstiness.to_excel(writer, sheet_name="burstiness", index=False)

    # Optional breakdowns
    top_senders.to_excel(writer, sheet_name="top_senders", index=False)
    top_topics.to_excel(writer, sheet_name="top_conv_topics", index=False)

print(f"Wrote: {out_path}")

