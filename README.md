# Metadata-Driven Spark Declarative Pipeline (SDP) Framework

A configuration-driven ingestion framework for Databricks Lakeflow pipelines. Developers onboard new data sources by editing a single YAML config file — no pipeline code changes required.

## How It Works

```
tables.yml (config) --> bronze_ingestion.py (engine) --> Streaming Tables in Bronze
```

The engine reads `tables.yml` at pipeline startup and dynamically creates one SDP streaming table per entry. Each table gets:
- Explicit schema enforcement from column definitions
- Data quality expectations from dbt-style tests and custom rules
- Automatic `_ingested_at` and `_source_file` metadata columns
- Liquid Clustering via `cluster_by`

## Project Structure

```
sdp-meta-data-driven/
├── databricks.yml                              # Databricks Asset Bundle (dev/prod)
├── pyproject.toml                              # Python package config
├── resources/
│   └── meta_driven_etl.pipeline.yml            # SDP pipeline resource
└── src/meta_driven_etl/
    ├── config/
    │   └── tables.yml                          # CONFIG FILE (edit this)
    └── pipeline/
        └── bronze_ingestion.py                 # ENGINE (do not edit)
```

## Adding a New Table

Add an entry to `src/meta_driven_etl/config/tables.yml`:

```yaml
tables:
  orders:
    description: "Order transactions"
    source:
      path: "orders/"              # relative to defaults.source_volume
      format: csv
      options:
        header: "true"
    columns:
      - name: order_id
        type: STRING
        tests:
          - not_null
      - name: order_date
        type: DATE
      - name: total_amount
        type: "DECIMAL(10,2)"
    cluster_by:
      - order_date
    data_quality:
      - name: positive_amount
        expression: "total_amount > 0"
        action: drop
```

Then deploy:

```bash
databricks bundle deploy
databricks bundle run meta_driven_etl
```

A new streaming table `bronze_meta_orders` is created automatically.

## Config Reference (`tables.yml`)

### Defaults

```yaml
defaults:
  source_volume: "/Volumes/catalog/schema/volume"   # base path for all sources
  target_layer: bronze                                # layer prefix
```

### Table Entry

| Field | Required | Description |
|-------|----------|-------------|
| `description` | No | Table comment in Unity Catalog |
| `source.path` | Yes | Folder path relative to `source_volume` |
| `source.format` | Yes | File format: `csv`, `json`, `parquet`, `avro` |
| `source.options` | No | Reader options (e.g., `header`, `delimiter`, `mode`) |
| `columns` | Yes | List of column definitions |
| `cluster_by` | No | Columns for Liquid Clustering |
| `data_quality` | No | Table-level DQ rules |

### Column Definition

```yaml
columns:
  - name: customer_id        # column name
    type: STRING              # PySpark type (see supported types below)
    description: "..."        # optional documentation
    tests:                    # dbt-style column tests
      - not_null
      - accepted_values:
          values: ["A", "B", "C"]
```

**Supported types:** `STRING`, `INT`, `INTEGER`, `BIGINT`, `LONG`, `DOUBLE`, `FLOAT`, `BOOLEAN`, `DATE`, `TIMESTAMP`, `SHORT`, `BYTE`, `BINARY`, `DECIMAL(p,s)`

### Column Tests (dbt-style)

| Test | Maps To | Behavior |
|------|---------|----------|
| `not_null` | `dp.expect("col_not_null", "col IS NOT NULL")` | Warns on null values |
| `accepted_values` | `dp.expect("col_accepted_values", "col IN (...)")` | Warns on invalid values |

> Note: `unique` is not supported for streaming tables.

### Data Quality Rules

Custom SQL expressions with configurable actions:

```yaml
data_quality:
  - name: valid_email          # rule name (must be unique)
    expression: "email LIKE '%@%.%'"   # SQL boolean expression
    action: drop               # warn | drop | fail
    description: "..."         # optional documentation
```

| Action | SDP Expectation | Behavior |
|--------|----------------|----------|
| `warn` | `dp.expect()` | Log violation, keep the row |
| `drop` | `dp.expect_or_drop()` | Silently remove violating rows |
| `fail` | `dp.expect_or_fail()` | Halt the pipeline on any violation |

## Environments

| Target | Schema | Pipeline Name | Mode |
|--------|--------|---------------|------|
| `dev` | `bronze_dev` | `[dev username] meta_driven_etl` | Development |
| `prod` | `bronze` | `meta_driven_etl` | Production |

Tables are named `bronze_meta_<table_name>` in both environments.

## Commands

```bash
# Validate bundle config
databricks bundle validate

# Deploy to dev (default)
databricks bundle deploy

# Deploy to prod
databricks bundle deploy --target prod

# Run dev pipeline
databricks bundle run meta_driven_etl

# Run prod pipeline
databricks bundle run meta_driven_etl --target prod
```

## Architecture

### Engine (`bronze_ingestion.py`)

The engine performs these steps at pipeline startup:

1. Reads `tables.yml` via pipeline configuration parameter `config_base_path`
2. For each table entry:
   - Builds a PySpark `StructType` schema from column definitions
   - Creates a function that reads from the source using Auto Loader (`cloudFiles`)
   - Converts column tests and DQ rules to SDP expectations
   - Applies `@dp.expect()`, `@dp.expect_or_drop()`, or `@dp.expect_or_fail()` decorators
   - Applies `@dp.table()` decorator with name, comment, and clustering
   - Registers the function in module globals for SDP discovery
3. Each registered function becomes a streaming table in the pipeline

### Metadata Columns

Every table automatically includes:

| Column | Description |
|--------|-------------|
| `_ingested_at` | Timestamp when the record was ingested |
| `_source_file` | Full path of the source file |

## Currently Onboarded Tables

| Table | Source | Format | Rows | DQ Rules |
|-------|--------|--------|------|----------|
| `bronze_meta_customers` | `customers/` | CSV | 15 | 3 column tests + 3 table rules |
| `bronze_meta_orders` | `orders/` | CSV | 25 | 4 column tests + 4 table rules |

## Available Source Data

The volume `/Volumes/vdm_classic_aerfvt_catalog/dbdemos_ai_query/raw/` contains:

| Folder | Status |
|--------|--------|
| `customers/` | Onboarded |
| `orders/` | Onboarded |
| `events/` | Available for next iteration |
| `taxi/` | Available for next iteration |
