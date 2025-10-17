# Allowed OMS HIS tables (whitelist)
_ALLOWED_HIS_TABLES = {
    "HIS_CALL",
    "HIS_CREW_ACTION",
    "HIS_FOLLOWUP",
    "HIS_INCIDENT",
    "HIS_INCIDENT_DEVICE",
    "HIS_INCIDENT_DEVICE_PREMISE",
    "HIS_LOCATION",
    "HIS_LOCATION_UDF",
    "HIS_MEMO",
}

def fetch_his_by_incident(cc, table: str, incident_id):
    """
    Query: SELECT * FROM "OMS".<table> WHERE "INCIDENT_ID" = {incident_id}
    Returns: list of rows (cc.sql(query).collect())

    Args
    ----
    cc : connection context with a .sql(...) method
    table : str, one of the whitelisted HIS tables above
    incident_id : int or str (will be SQL-formatted safely)
    """
    # Normalize & validate table name
    t = str(table).strip().upper()
    if t not in _ALLOWED_HIS_TABLES:
        raise ValueError(f"Table '{table}' is not an allowed HIS table.")

    # Format INCIDENT_ID safely (numbers as-is; strings single-quoted & escaped)
    if isinstance(incident_id, int):
        id_sql = str(incident_id)
    else:
        s = str(incident_id)
        s = s.replace("'", "''")  # basic SQL escaping for single quotes
        id_sql = f"'{s}'"

    query = f'SELECT * FROM "OMS"."{t}" WHERE "INCIDENT_ID" = {id_sql}'

    # Run and collect
    return cc.sql(query).collect()
