import win32com.client as win32
import datetime as dt
import pandas as pd

def scrape_trouble_tickets_from_inbox():
    outlook = win32.Dispatch("Outlook.Application").GetNamespace("MAPI")

    # 🔧 Adjust this to whatever mailbox/folder you’re already using
    root = outlook.Folders.Item("DSO")          # or your own mailbox name
    inbox = root.Folders.Item("Inbox")          # main inbox

    items = inbox.Items
    items.Sort("[ReceivedTime]", True)

    # Optional safety: only scan 2025+ (delete this block if you truly want *everything*)
    start_dt = dt.datetime(2025, 1, 1)
    items = items.Restrict(
        "[ReceivedTime] >= '" + start_dt.strftime("%m/%d/%Y %H:%M %p") + "'"
    )

    rows = []

    for msg in items:
        body = msg.Body or ""

        # 🔍 Hard filter: only emails that contain BOTH headers
        if TT_HEADER not in body or P1_HEADER not in body:
            continue

        # Split into trouble-ticket block and embedded P1 form block
        ticket_block, p1_block = split_trouble_ticket_sections(body)
        if not ticket_block or not p1_block:
            continue

        # Parse the TROUBLE TICKET block
        tt = parse_trouble_ticket_block(ticket_block)

        # Extra guard: skip if no CSS SEQ NO was actually found
        if not tt.get("css_seq_no"):
            continue

        # Parse the embedded P1 form with your existing parser
        p1 = parse_p1_form_from_body(p1_block)
        p1 = normalize_sap_fields(p1)
        p1["comments"] = clean_comments(p1.get("comments") or "")
        match_key = build_match_key(p1)
        p1["match_key"] = match_key

        rows.append({
            "match_key": match_key,
            "css_seq_no": tt.get("css_seq_no"),
            "ticket_date": tt.get("ticket_date"),
            "ticket_structure": tt.get("ticket_structure"),
            "ticket_incident_id": tt.get("ticket_incident_id"),
            "ticket_remarks": tt.get("ticket_remarks"),
            "email_received_time": msg.ReceivedTime,
            "email_subject": msg.Subject,
            "email_sender": getattr(msg, "SenderName", None),
            "email_sender_addr": getattr(msg, "SenderEmailAddress", None),
        })

    df = pd.DataFrame(rows)
    return df


# One-time historical pass:
trouble_df = scrape_trouble_tickets_from_inbox()
trouble_df.to_csv("p1_trouble_tickets_raw.csv", index=False)
print(f"Parsed {len(trouble_df)} trouble-ticket emails.")
