import re
import datetime as dt
from typing import Optional, Dict, Tuple, List

import win32com.client as win32
import pandas as pd


# -------------------------------------------------------------------
# 1. Constants / headers
# -------------------------------------------------------------------

TT_HEADER = "TROUBLE TICKET"
P1_HEADER = "OVERHEAD DETAIL INSPECTION P1 FORM"


# -------------------------------------------------------------------
# 2. Match-key builder (used everywhere to tie things back to origin)
# -------------------------------------------------------------------

def build_match_key(p1: Dict[str, Optional[str]]) -> str:
    """
    Deterministic key to join initial forms, DOC forms, and trouble tickets.

    We use:
      - floc          (pole / structure)
      - created_at    (ISO string from the form)
      - reporter_email

    All normalized (upper/lower, stripped).
    """
    floc = (p1.get("floc") or "").strip().upper()
    created_at = (p1.get("created_at") or "").strip()
    reporter_email = (p1.get("reporter_email") or "").strip().lower()

    return "|".join([floc, created_at, reporter_email])


# -------------------------------------------------------------------
# 3. Trouble-ticket parsing helpers
# -------------------------------------------------------------------

CSS_SEQ_PATTERN = re.compile(r"CSS SEQ NO:\s*([0-9A-Za-z]+)")
DATE_PATTERN = re.compile(r"DATE:\s*([0-9/]+[^\n]*)")
STRUCTURE_PATTERN = re.compile(r"\bSTRUCTURE:\s*([0-9A-Za-z\-]+)")
INCIDENT_ID_PATTERN = re.compile(r"\bINCIDENT:([0-9]+)")
REMARKS_PATTERN = re.compile(r"\bREMARKS:\s*(.*)", re.DOTALL)


def split_trouble_ticket_sections(body: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Given a full email body that contains a TROUBLE TICKET and a P1 FORM,
    split into:
        ticket_block, p1_block

    Returns (None, None) if either header is missing.
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

    # Remarks can be multi-line; stop at the next underline separator if present
    m = REMARKS_PATTERN.search(block)
    if m:
        remarks_raw = m.group(1)
        sep_idx = remarks_raw.find("________________________________")
        if sep_idx != -1:
            remarks_raw = remarks_raw[:sep_idx]
        out["ticket_remarks"] = remarks_raw.strip()

    return out


def is_trouble_ticket_email(body: str) -> bool:
    """
    Python-side guard: we only treat emails that contain both
    the TROUBLE TICKET block and the embedded P1 form.
    """
    return (TT_HEADER in body) and (P1_HEADER in body)


# -------------------------------------------------------------------
# 4. Main scraper: walk inbox, pick out trouble tickets, parse them
# -------------------------------------------------------------------

def scrape_trouble_tickets_from_inbox(
    mailbox_name: str = "DSO",
    folder_name: str = "Inbox",
    start_date: dt.datetime = dt.datetime(2025, 1, 1),
) -> pd.DataFrame:
    """
    One-time historical pass over the given Outlook folder.

    - Iterates all messages (optionally restricted by start_date)
    - Keeps only those that contain both TROUBLE TICKET and P1 FORM
    - Parses:
        * CSS SEQ NO, structure, incident, remarks, ticket date
        * Embedded P1 form (via parse_p1_form_from_body)
        * Builds match_key from P1 data

    Returns:
        DataFrame with one row per trouble ticket.
    """

    # --- connect to Outlook
    outlook = win32.Dispatch("Outlook.Application").GetNamespace("MAPI")
    root = outlook.Folders.Item(mailbox_name)       # e.g. "DSO" or your own name
    inbox = root.Folders.Item(folder_name)          # e.g. "Inbox"

    items = inbox.Items
    items.Sort("[ReceivedTime]", True)

    # Optional: restrict to >= start_date for performance
    if start_date is not None:
        items = items.Restrict(
            "[ReceivedTime] >= '" + start_date.strftime("%m/%d/%Y %H:%M %p") + "'"
        )

    rows: List[Dict[str, object]] = []

    for msg in items:
        body = msg.Body or ""

        # Only keep emails that clearly look like trouble tickets
        if not is_trouble_ticket_email(body):
            continue

        ticket_block, p1_block = split_trouble_ticket_sections(body)
        if not ticket_block or not p1_block:
            continue

        # --- Parse trouble ticket portion
        tt = parse_trouble_ticket_block(ticket_block)

        # If we couldn't find a CSS sequence number, skip
        if not tt.get("css_seq_no"):
            continue

        # --- Parse embedded P1 form (re-uses your existing logic)
        # NOTE: assumes these functions are defined elsewhere in your code:
        #   parse_p1_form_from_body(body: str) -> dict
        #   normalize_sap_fields(parsed: dict) -> dict
        #   clean_comments(text: str) -> str

        p1 = parse_p1_form_from_body(p1_block)
        p1 = normalize_sap_fields(p1)
        p1["comments"] = clean_comments(p1.get("comments") or "")
        match_key = build_match_key(p1)
        p1["match_key"] = match_key

        rows.append(
            {
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
            }
        )

    df = pd.DataFrame(rows)
    return df


# -------------------------------------------------------------------
# 5. Example one-shot run
# -------------------------------------------------------------------

if __name__ == "__main__":
    # Adjust mailbox/folder as needed
    trouble_df = scrape_trouble_tickets_from_inbox(
        mailbox_name="DSO",     # or your mailbox name
        folder_name="Inbox",    # or another folder if needed
        start_date=dt.datetime(2025, 1, 1),
    )

    trouble_df.to_csv("p1_trouble_tickets_raw.csv", index=False)
    print(f"Parsed {len(trouble_df)} trouble-ticket emails.")
