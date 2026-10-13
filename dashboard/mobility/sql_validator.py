"""Guardrail for LLM-generated SQL before it reaches Athena.

The SQL is parsed into an AST (sqlglot, Trino dialect = Athena engine v3)
instead of being pattern-matched, so table references hidden in comma joins,
subqueries or CTEs are still checked against the allow-list.

This is defence in depth, not the only control: the Athena credentials used
by the dashboard should be read-only on mobility_gold, and the workgroup
should enforce a bytes-scanned limit.
"""

import sqlglot
from sqlglot import exp

MAX_SQL_LENGTH = 10000

ALLOWED_TABLES = {
    "mobility_gold.fact_trips",
    "mobility_gold.dim_date",
    "mobility_gold.dim_payment_type",
    "mobility_gold.dim_zone",
    "mobility_gold.fact_sensor_hourly",
    "mobility_gold.dim_sensor",
}

ALLOWED_CATALOGS = {"", "awsdatacatalog"}

READ_ONLY_ROOTS = (exp.Select, exp.Union, exp.Intersect, exp.Except)

FORBIDDEN_NODES = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.Command,
)


def validate_sql(sql):
    if not sql or not sql.strip():
        return False, "SQL query is empty."

    sql = sql.strip()

    if len(sql) > MAX_SQL_LENGTH:
        return False, "SQL query exceeds the maximum allowed length."

    # Reject SQL comments.
    if "--" in sql or "/*" in sql or "*/" in sql:
        return False, "SQL comments are not allowed."

    try:
        statements = [s for s in sqlglot.parse(sql, read="trino") if s is not None]
    except sqlglot.errors.ParseError as exc:
        return False, f"SQL could not be parsed: {exc}"

    if len(statements) != 1:
        return False, "Exactly one SQL statement is allowed."

    statement = statements[0]

    # Only read queries (SELECT, optionally with WITH / UNION) are allowed.
    if not isinstance(statement, READ_ONLY_ROOTS):
        return False, "Only SELECT queries are allowed."

    for node in statement.walk():
        if isinstance(node, FORBIDDEN_NODES):
            return False, f"Forbidden SQL operation: {node.key.upper()}"

    cte_names = {cte.alias_or_name.lower() for cte in statement.find_all(exp.CTE)}

    tables = list(statement.find_all(exp.Table))

    if not tables:
        return False, "No valid table reference found."

    # Restrict queries to approved Gold tables.
    for table in tables:
        name = table.name.lower()

        # References to a CTE defined in this query are fine.
        if not table.db and name in cte_names:
            continue

        full_name = f"{table.db}.{name}".lower()

        if (
            table.catalog.lower() not in ALLOWED_CATALOGS
            or full_name not in ALLOWED_TABLES
        ):
            return False, f"Table is not allowed: {table.sql(dialect='trino')}"

    return True, "SQL validation passed."
