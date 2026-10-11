import re

FORBIDDEN_KEYWORDS = [
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "truncate",
    "merge",
    "grant",
    "revoke",
    "call",
    "execute",
    "unload",
]

ALLOWED_TABLES = {
    "mobility_gold.fact_trips",
    "mobility_gold.dim_date",
    "mobility_gold.dim_payment_type",
    "mobility_gold.dim_zone",
    "mobility_gold.fact_sensor_hourly",
    "mobility_gold.dim_sensor",
}


def validate_sql(sql):
    if not sql or not sql.strip():
        return False, "SQL query is empty."

    sql = sql.strip()

    if len(sql) > 10000:
        return False, "SQL query exceeds the maximum allowed length."

    # Reject SQL comments.
    if "--" in sql or "/*" in sql or "*/" in sql:
        return False, "SQL comments are not allowed."

    # Allow one optional trailing semicolon.
    if sql.endswith(";"):
        sql = sql[:-1].strip()

    # Reject multiple statements.
    if ";" in sql:
        return False, "Multiple SQL statements are not allowed."

    # Only SELECT statements are allowed.
    if not re.match(r"^select\b", sql, re.IGNORECASE):
        return False, "Only SELECT queries are allowed."

    # Mask string literals before checking forbidden keywords.
    # This avoids rejecting words such as 'update' inside a string.
    sql_without_strings = re.sub(
        r"'(?:''|[^'])*'",
        "''",
        sql,
    )

    sql_lower = sql_without_strings.lower()

    # Reject forbidden SQL operations as complete words.
    for keyword in FORBIDDEN_KEYWORDS:
        pattern = rf"\b{re.escape(keyword)}\b"

        if re.search(pattern, sql_lower):
            return False, f"Forbidden SQL operation: {keyword}"

    # Find table references after FROM and JOIN.
    table_pattern = (
        r"\b(?:from|join)\s+"
        r"([a-zA-Z_][a-zA-Z0-9_]*"
        r"(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)"
    )

    tables = re.findall(
        table_pattern,
        sql_without_strings,
        re.IGNORECASE,
    )

    if not tables:
        return False, "No valid table reference found."

    # Restrict queries to approved Gold tables.
    for table in tables:
        if table.lower() not in ALLOWED_TABLES:
            return False, f"Table is not allowed: {table}"

    return True, "SQL validation passed."
