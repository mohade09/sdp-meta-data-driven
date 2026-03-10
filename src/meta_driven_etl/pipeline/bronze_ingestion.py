# ============================================================================
# METADATA-DRIVEN BRONZE INGESTION ENGINE
# ============================================================================
# DO NOT MODIFY THIS FILE.
# To onboard new data sources, edit: config/tables.yml
# ============================================================================

import os
import yaml
from pyspark import pipelines as dp
from pyspark.sql import functions as F
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
    # Handle DECIMAL(precision, scale)
    if upper.startswith("DECIMAL"):
        import re
        m = re.match(r"DECIMAL\((\d+),\s*(\d+)\)", upper)
        if m:
            return DecimalType(int(m.group(1)), int(m.group(2)))
        return DecimalType(10, 2)
    raise ValueError(f"Unsupported column type: {type_str}")


def _build_schema(columns: list) -> StructType:
    """Build a PySpark StructType from the YAML column definitions."""
    fields = [
        StructField(col["name"], _resolve_type(col["type"]), True)
        for col in columns
    ]
    return StructType(fields)


def _build_schema_hints(columns: list) -> str:
    """Build a schemaHints string for read_files (SQL) from column definitions."""
    return ", ".join(f"{col['name']} {col['type']}" for col in columns)


def _column_tests_to_expectations(columns: list) -> list:
    """
    Convert dbt-style column tests to SDP expectation tuples.
    Returns: list of (name, sql_expression, action)
    """
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
    """
    Convert table-level data_quality rules to SDP expectation tuples.
    Returns: list of (name, sql_expression, action)
    """
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
    else:  # "warn" or default
        return dp.expect(name, expression)(fn)


# ---------------------------------------------------------------------------
# Load configuration
# ---------------------------------------------------------------------------
# __file__ is not defined in SDP runtime; use pipeline parameter instead
_config_base = spark.conf.get("config_base_path")  # noqa: F821
_config_path = os.path.join(_config_base, "bronze_tables.yml")
with open(_config_path, "r") as _f:
    _config = yaml.safe_load(_f)

_defaults = _config.get("defaults", {})
_source_volume = _defaults.get("source_volume", "")


# ---------------------------------------------------------------------------
# Dynamic table registration
# ---------------------------------------------------------------------------
def _register_table(table_name: str, table_cfg: dict):
    """
    Dynamically create and register a bronze streaming table from config.
    """
    source_cfg = table_cfg["source"]
    source_path = os.path.join(_source_volume, source_cfg["path"])
    file_format = source_cfg["format"]
    reader_options = source_cfg.get("options", {})
    columns = table_cfg.get("columns", [])
    cluster_by = table_cfg.get("cluster_by")
    description = table_cfg.get("description", "")

    schema = _build_schema(columns)

    # --- Build the table function ---
    def _make_fn():
        def _table_fn():
            reader = (
                spark.readStream  # noqa: F821 (spark is available in SDP runtime)
                .format("cloudFiles")
                .option("cloudFiles.format", file_format)
                .schema(schema)
            )
            for opt_key, opt_val in reader_options.items():
                reader = reader.option(opt_key, str(opt_val))

            return (
                reader
                .load(source_path)
                .withColumn("_ingested_at", F.current_timestamp())
                .withColumn("_source_file", F.col("_metadata.file_path"))
            )
        return _table_fn

    fn = _make_fn()
    bronze_name = f"bronze_meta_{table_name}"
    fn.__name__ = bronze_name
    fn.__qualname__ = bronze_name

    # --- Collect all expectations ---
    all_expectations = []
    all_expectations.extend(_column_tests_to_expectations(columns))
    all_expectations.extend(
        _table_dq_to_expectations(table_cfg.get("data_quality", []))
    )

    # --- Apply expectation decorators (innermost first) ---
    for exp_name, exp_expr, exp_action in all_expectations:
        fn = _apply_expectation(fn, exp_name, exp_expr, exp_action)

    # --- Apply @dp.table() decorator (outermost) ---
    fn = dp.table(
        name=bronze_name,
        comment=description,
        cluster_by=cluster_by,
    )(fn)

    # Register in module globals so SDP discovers it
    globals()[bronze_name] = fn


# ---------------------------------------------------------------------------
# Register all tables from config
# ---------------------------------------------------------------------------
for _table_name, _table_cfg in _config.get("tables", {}).items():
    _register_table(_table_name, _table_cfg)
