# ============================================================================
# METADATA-DRIVEN GOLD AGGREGATION ENGINE
# ============================================================================
# DO NOT MODIFY THIS FILE.
# To onboard new gold tables, edit: config/gold_tables.yml
# ============================================================================
#
# Supports two output modes per table (controlled by materialized_view flag):
#   - materialized_view: true  → dp.materialized_view()
#       Auto-refreshes when upstream silver data changes.
#       Optimized for BI/dashboard reads.
#   - materialized_view: false → dp.table()
#       Standard batch table.
# ============================================================================

import os
import yaml
from pyspark import pipelines as dp
from pyspark.sql import functions as F


def _table_dq_to_expectations(dq_rules: list) -> list:
    """Convert table-level data_quality rules to SDP expectation tuples."""
    expectations = []
    for rule in dq_rules:
        expectations.append((
            rule["name"],
            rule["expression"],
            rule.get("action", "warn"),
        ))
    return expectations


def _apply_expectation(fn, name: str, expression: str, action: str):
    """Apply a single SDP expectation decorator to a function."""
    if action == "drop":
        return dp.expect_or_drop(name, expression)(fn)
    elif action == "fail":
        return dp.expect_or_fail(name, expression)(fn)
    else:
        return dp.expect(name, expression)(fn)


def _build_aggregate_query(table_cfg: dict) -> str:
    """
    Build a SQL query from the aggregate table configuration.

    Supports:
      - Single source table with GROUP BY + metrics
      - Joins across multiple silver tables
    """
    source_cfg = table_cfg["source"]
    primary_table = source_cfg["primary_table"]
    joins = source_cfg.get("joins", [])
    group_by = table_cfg.get("group_by", [])
    metrics = table_cfg.get("metrics", [])

    # SELECT clause: group-by columns + metric expressions
    select_parts = []
    for col in group_by:
        alias = col.split(".")[-1] if "." in col else col
        select_parts.append(f"{col} AS {alias}")

    for metric in metrics:
        select_parts.append(f"{metric['expression']} AS {metric['name']}")

    select_clause = ",\n    ".join(select_parts)

    # FROM clause
    from_clause = f"{primary_table} AS orders"

    # JOIN clauses
    join_clauses = []
    for join in joins:
        join_type = join.get("type", "inner").upper()
        join_table = join["table"]
        alias = join.get("alias", join_table.split("_")[-1])
        condition = join["condition"]
        join_clauses.append(
            f"{join_type} JOIN {join_table} AS {alias} ON {condition}"
        )

    join_clause = "\n".join(join_clauses)

    # GROUP BY clause
    group_clause = ", ".join(group_by) if group_by else ""

    # Assemble full query
    query = f"SELECT\n    {select_clause}\nFROM {from_clause}"
    if join_clause:
        query += f"\n{join_clause}"
    if group_clause:
        query += f"\nGROUP BY {group_clause}"

    return query


# ---------------------------------------------------------------------------
# Load configuration
# ---------------------------------------------------------------------------
_config_base = spark.conf.get("config_base_path")  # noqa: F821
_config_path = os.path.join(_config_base, "gold_tables.yml")
with open(_config_path, "r") as _f:
    _config = yaml.safe_load(_f)

_defaults = _config.get("defaults", {})
_default_as_mv = _defaults.get("materialized_view", True)


# ---------------------------------------------------------------------------
# Dynamic table registration
# ---------------------------------------------------------------------------
def _register_table(table_name: str, table_cfg: dict):
    """
    Dynamically create and register a gold table or materialized view.
    """
    table_type = table_cfg.get("table_type", "aggregate")
    description = table_cfg.get("description", "")
    as_mv = table_cfg.get("materialized_view", _default_as_mv)

    # --- Build the query function ---
    if table_type == "aggregate":
        query = _build_aggregate_query(table_cfg)

        def _make_fn(sql_query):
            def _table_fn():
                return spark.sql(sql_query)  # noqa: F821
            return _table_fn

        fn = _make_fn(query)

    elif table_type == "sql":
        raw_sql = table_cfg["sql"]

        def _make_sql_fn(sql_query):
            def _table_fn():
                return spark.sql(sql_query)  # noqa: F821
            return _table_fn

        fn = _make_sql_fn(raw_sql)

    else:
        raise ValueError(f"Unsupported gold table_type: {table_type}")

    gold_name = f"gold_meta_{table_name}"
    fn.__name__ = gold_name
    fn.__qualname__ = gold_name

    # --- Collect all expectations ---
    all_expectations = _table_dq_to_expectations(
        table_cfg.get("data_quality", [])
    )

    # --- Apply expectation decorators (innermost first) ---
    for exp_name, exp_expr, exp_action in all_expectations:
        fn = _apply_expectation(fn, exp_name, exp_expr, exp_action)

    # --- Apply outermost decorator based on materialized_view flag ---
    if as_mv:
        fn = dp.materialized_view(
            name=gold_name,
            comment=description,
        )(fn)
    else:
        fn = dp.table(
            name=gold_name,
            comment=description,
        )(fn)

    globals()[gold_name] = fn


# ---------------------------------------------------------------------------
# Register all tables from config
# ---------------------------------------------------------------------------
for _table_name, _table_cfg in _config.get("tables", {}).items():
    _register_table(_table_name, _table_cfg)
