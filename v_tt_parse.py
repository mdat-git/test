import re
from typing import Optional, Dict

TT_HEADER = "TROUBLE TICKET"
P1_HEADER = "OVERHEAD DETAIL INSPECTION P1 FORM"

CSS_SEQ_PATTERN = re.compile(r"CSS SEQ NO:\s*([0-9A-Za-z]+)")
DATE_PATTERN = re.compile(r"DATE:\s*([0-9/]+[^\n]*)")
STRUCTURE_PATTERN = re.compile(r"\bSTRUCTURE:\s*([0-9A-Za-z\-]+)")
INCIDENT_ID_PATTERN = re.compile(r"\bINCIDENT:([0-9]+)")
REMARKS_PATTERN = re.compile(r"\bREMARKS:\s*(.*)", re.DOTALL)


def split_trouble_ticket_sections(body: str) -> tuple[Optional[str], Optional[str]]:
    """
    Given a full email body that contains a TROUBLE TICKET and a P1 FORM,
    split into:
      ticket_block, p1_block
    """
    if TT_HEADER not in body or P1_HEADER not in body:
        return None, None

    tt_idx = body.index(TT_HEADER)
    p1_idx = body.index(P1_HEADER)

    ticket_block = body[tt_idx:p1_idx]
    p1_block = body[p1_idx:]

    return ticket_block, p1_block


def parse_trouble_ticket_block(block: str) -> Dict[str, Optional[str]]:
    """
    Parse CSS SEQ NO, date, structure, incident id, and remarks
    from the TROUBLE TICKET block.
    """
    out: Dict[str, Optional[str]] = {
        "css_seq_no": None,
        "ticket_date": None,
        "ticket_structure": None,
        "ticket_incident_id": None,
        "ticket_remarks": None,
    }

    if not block:
        return out

    m = CSS_SEQ_PATTERN.search(block)
    if m:
        out["css_seq_no"] = m.group(1).strip()

    m = DATE_PATTERN.search(block)
    if m:
        out["ticket_date"] = m.group(1).strip()

    m = STRUCTURE_PATTERN.search(block)
    if m:
        out["ticket_structure"] = m.group(1).strip()

    m = INCIDENT_ID_PATTERN.search(block)
    if m:
        out["ticket_incident_id"] = m.group(1).strip()

    # Remarks can be multi-line, but we don't want to eat the whole block,
    # so stop at the next "____" line or section marker if present.
    m = REMARKS_PATTERN.search(block)
    if m:
        remarks_raw = m.group(1)
        # Optional: cut at the next underline separator
        sep_idx = remarks_raw.find("________________________________")
        if sep_idx != -1:
            remarks_raw = remarks_raw[:sep_idx]
        out["ticket_remarks"] = remarks_raw.strip()

    return out




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
