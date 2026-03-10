# ============================================================================
# METADATA-DRIVEN SILVER TRANSFORMATION ENGINE
# ============================================================================
# DO NOT MODIFY THIS FILE.
# To onboard new silver tables, edit: config/silver_tables.yml
# ============================================================================
#
# Supports two modes per table:
#   1. SCD mode (scd config present):
#      - Creates target via dp.create_streaming_table()
#      - Applies transformations to source stream in a @dp.view()
#      - Merges into target via dp.apply_changes() with SCD Type 1 or 2
#
#   2. Append mode (no scd config):
#      - Creates streaming table via @dp.table() with returned DataFrame
#      - Applies transformations inline
# ============================================================================

import os
import yaml
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    BooleanType,
    DateType,
    TimestampType,
    DecimalType,
    ShortType,
    ByteType,
    BinaryType,
)

# ---------------------------------------------------------------------------
# Type mapping: YAML type string -> PySpark type
# ---------------------------------------------------------------------------
TYPE_MAP = {
    "STRING": StringType(),
    "INT": IntegerType(),
    "INTEGER": IntegerType(),
    "BIGINT": LongType(),
    "LONG": LongType(),
    "DOUBLE": DoubleType(),
    "FLOAT": FloatType(),
    "BOOLEAN": BooleanType(),
    "DATE": DateType(),
    "TIMESTAMP": TimestampType(),
    "SHORT": ShortType(),
    "BYTE": ByteType(),
    "BINARY": BinaryType(),
}


def _resolve_type(type_str: str):
    """Resolve a YAML type string to a PySpark DataType."""
    upper = type_str.upper().strip()
    if upper in TYPE_MAP:
        return TYPE_MAP[upper]
    if upper.startswith("DECIMAL"):
        import re
        m = re.match(r"DECIMAL\((\d+),\s*(\d+)\)", upper)
        if m:
            return DecimalType(int(m.group(1)), int(m.group(2)))
        return DecimalType(10, 2)
    raise ValueError(f"Unsupported column type: {type_str}")


def _column_tests_to_expectations(columns: list) -> list:
    """Convert dbt-style column tests to SDP expectation tuples."""
    expectations = []
    for col in columns:
        col_name = col["name"]
        for test in col.get("tests", []):
            if test == "not_null":
                expectations.append((
                    f"{col_name}_not_null",
                    f"{col_name} IS NOT NULL",
                    "warn",
                ))
            elif isinstance(test, dict) and "accepted_values" in test:
                values = test["accepted_values"]["values"]
                values_str = ", ".join(f"'{v}'" for v in values)
                expectations.append((
                    f"{col_name}_accepted_values",
                    f"{col_name} IN ({values_str})",
                    "warn",
                ))
    return expectations


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


def _apply_transformations(df, transformations: list):
    """
    Apply a chain of transformations to a DataFrame.

    Supported transformation types:
      - deduplicate: window-based dedup using row_number
      - filter: SQL WHERE condition
      - derive: add computed columns via SQL expressions
      - rename: rename columns
      - drop_columns: remove columns
      - cast: change column types
    """
    for t in transformations:
        t_type = t["type"]

        if t_type == "deduplicate":
            partition_cols = t["partition_by"]
            order_expr = t.get("order_by", "_ingested_at DESC")
            w = Window.partitionBy(*partition_cols).orderBy(
                F.expr(order_expr)
            )
            df = (
                df.withColumn("_row_num", F.row_number().over(w))
                .filter(F.col("_row_num") == 1)
                .drop("_row_num")
            )

        elif t_type == "filter":
            df = df.filter(t["condition"])

        elif t_type == "derive":
            for col_def in t["columns"]:
                df = df.withColumn(
                    col_def["name"],
                    F.expr(col_def["expression"])
                )

        elif t_type == "rename":
            for mapping in t["columns"]:
                df = df.withColumnRenamed(
                    mapping["old_name"], mapping["new_name"]
                )

        elif t_type == "drop_columns":
            df = df.drop(*t["columns"])

        elif t_type == "cast":
            for col_name, target_type in t["columns"].items():
                df = df.withColumn(
                    col_name,
                    F.col(col_name).cast(target_type)
                )

    return df


# ---------------------------------------------------------------------------
# Load configuration
# ---------------------------------------------------------------------------
_config_base = spark.conf.get("config_base_path")  # noqa: F821
_config_path = os.path.join(_config_base, "silver_tables.yml")
with open(_config_path, "r") as _f:
    _config = yaml.safe_load(_f)

_defaults = _config.get("defaults", {})


# ---------------------------------------------------------------------------
# SCD table registration (SCD Type 1 / Type 2 via apply_changes)
# ---------------------------------------------------------------------------
def _register_scd_table(table_name: str, table_cfg: dict):
    """
    Register a silver table using SDP's apply_changes for SCD processing.

    SCD Type 1: Overwrites existing rows — no history kept.
    SCD Type 2: Inserts new version rows. SDP auto-manages:
      - __START_AT  : effective start timestamp of this version
      - __END_AT    : effective end timestamp (null for current)
      - __IS_CURRENT: boolean flag for the active version

    Flow:
      1. dp.create_streaming_table() — creates the target table
      2. @dp.view() — reads source stream and applies transformations
      3. dp.apply_changes() — merges transformed stream into target
    """
    source_cfg = table_cfg["source"]
    source_table = source_cfg["table"]
    scd_cfg = table_cfg["scd"]
    transformations = table_cfg.get("transformations", [])
    cluster_by = table_cfg.get("cluster_by")
    description = table_cfg.get("description", "")

    scd_type = scd_cfg["type"]               # 1 or 2
    keys = scd_cfg["keys"]                    # list of business key columns
    sequence_by = scd_cfg["sequence_by"]      # ordering column
    ignore_null_updates = scd_cfg.get("ignore_null_updates", False)

    # SCD2-specific: which columns to track for history
    track_history_columns = scd_cfg.get("track_history_columns")
    except_columns = scd_cfg.get("except_columns")

    silver_name = f"silver_meta_{table_name}"
    view_name = f"_vw_{silver_name}"

    # --- 1. Create the target streaming table ---
    create_kwargs = {
        "name": silver_name,
        "comment": description,
    }
    if cluster_by:
        create_kwargs["cluster_by"] = cluster_by

    dp.create_streaming_table(**create_kwargs)

    # --- 2. Create a view that reads + transforms the source stream ---
    # Data quality expectations are applied on the view (source side)
    all_expectations = []
    all_expectations.extend(
        _column_tests_to_expectations(table_cfg.get("columns", []))
    )
    all_expectations.extend(
        _table_dq_to_expectations(table_cfg.get("data_quality", []))
    )

    def _make_view_fn():
        def _view_fn():
            df = dp.read_stream(source_table)
            df = _apply_transformations(df, transformations)
            return df
        return _view_fn

    view_fn = _make_view_fn()
    view_fn.__name__ = view_name
    view_fn.__qualname__ = view_name

    # Apply expectation decorators on the view (innermost first)
    for exp_name, exp_expr, exp_action in all_expectations:
        view_fn = _apply_expectation(view_fn, exp_name, exp_expr, exp_action)

    view_fn = dp.view(
        name=view_name,
        comment=f"Transformed source stream for {silver_name}",
    )(view_fn)

    globals()[view_name] = view_fn

    # --- 3. Apply changes (SCD merge) from view into target ---
    apply_kwargs = {
        "target": silver_name,
        "source": view_name,
        "keys": keys,
        "sequence_by": F.col(sequence_by),
        "stored_as_scd_type": scd_type,
        "ignore_null_updates": ignore_null_updates,
    }

    # SCD2: specify which columns trigger a new version
    if scd_type == 2:
        if track_history_columns and except_columns:
            raise ValueError(
                f"Table '{table_name}': track_history_columns and "
                "except_columns are mutually exclusive for SCD2."
            )
        if track_history_columns:
            apply_kwargs["track_history_column_list"] = track_history_columns
        elif except_columns:
            apply_kwargs["track_history_except_column_list"] = except_columns

    dp.apply_changes(**apply_kwargs)


# ---------------------------------------------------------------------------
# Append-mode table registration (no SCD — original behavior)
# ---------------------------------------------------------------------------
def _register_append_table(table_name: str, table_cfg: dict):
    """
    Register a silver streaming table in simple append mode (no SCD).
    Reads source stream, applies transformations, returns DataFrame.
    """
    source_cfg = table_cfg["source"]
    source_table = source_cfg["table"]
    transformations = table_cfg.get("transformations", [])
    cluster_by = table_cfg.get("cluster_by")
    description = table_cfg.get("description", "")

    def _make_fn():
        def _table_fn():
            df = dp.read_stream(source_table)
            df = _apply_transformations(df, transformations)
            return df
        return _table_fn

    fn = _make_fn()
    silver_name = f"silver_meta_{table_name}"
    fn.__name__ = silver_name
    fn.__qualname__ = silver_name

    all_expectations = []
    all_expectations.extend(
        _column_tests_to_expectations(table_cfg.get("columns", []))
    )
    all_expectations.extend(
        _table_dq_to_expectations(table_cfg.get("data_quality", []))
    )

    for exp_name, exp_expr, exp_action in all_expectations:
        fn = _apply_expectation(fn, exp_name, exp_expr, exp_action)

    fn = dp.table(
        name=silver_name,
        comment=description,
        cluster_by=cluster_by,
    )(fn)

    globals()[silver_name] = fn


# ---------------------------------------------------------------------------
# Register all tables from config
# ---------------------------------------------------------------------------
for _table_name, _table_cfg in _config.get("tables", {}).items():
    if "scd" in _table_cfg:
        _register_scd_table(_table_name, _table_cfg)
    else:
        _register_append_table(_table_name, _table_cfg)
