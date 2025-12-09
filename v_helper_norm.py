import re

SAP_NOTIF_PATTERN = re.compile(r"\b415\d{6}\b")  # 415 + 6 digits = 9 total
SAP_ORDER_PATTERN = re.compile(r"\b904\d{6}\b")  # 904 + 6 digits = 9 total


def normalize_sap_fields(parsed: dict) -> dict:
    """
    Clean/standardize SAP Notification and SAP Order fields.

    Rules:
    - SAP Notification Number: 9 digits, starts with 415
    - SAP Order #: 9 digits, starts with 904
    - Problem Statement: keep text, strip out pure numeric lines we promoted to SAP fields
    """

    # Raw fields as parsed (may be messy / multi-line)
    notif_raw = (parsed.get("sap_notification_number") or "").strip()
    order_raw = (parsed.get("sap_order_number") or "").strip()
    ps_raw = (parsed.get("problem_statement") or "").strip()

    # Search across ALL three, because humans sometimes paste everything
    # into the wrong box.
    pool_text = "\n".join(filter(None, [notif_raw, order_raw, ps_raw]))

    notif_matches = SAP_NOTIF_PATTERN.findall(pool_text)
    order_matches = SAP_ORDER_PATTERN.findall(pool_text)

    # Prefer what we find by pattern; if no match, keep whatever was there.
    notif = parsed.get("sap_notification_number") or None
    order = parsed.get("sap_order_number") or None

    if notif_matches:
        notif = notif_matches[-1]  # use last match in case of corrections
    if order_matches:
        order = order_matches[-1]

    # Now rebuild a cleaner problem statement:
    # - remove any lines that are just these IDs
    # - keep descriptive text like "REPAIR CLEARNC PRI CBL/CND POLE"
    ps_lines = [ln for ln in ps_raw.splitlines() if ln.strip()]
    clean_ps_lines = []

    for ln in ps_lines:
        stripped = ln.strip()
        # If the line is exactly the notif or order ID, drop it
        if stripped == notif or stripped == order:
            continue
        # If it's just a 9-digit 415… or 904… ID, drop it
        if SAP_NOTIF_PATTERN.fullmatch(stripped) or SAP_ORDER_PATTERN.fullmatch(stripped):
            continue
        clean_ps_lines.append(ln)

    clean_ps = "\n".join(clean_ps_lines).strip() if clean_ps_lines else ps_raw

    # Write back into parsed dict
    parsed["sap_notification_number"] = notif
    parsed["sap_order_number"] = order
    parsed["problem_statement"] = clean_ps

    return parsed
