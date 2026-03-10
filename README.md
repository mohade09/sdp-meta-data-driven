# Metadata-Driven Spark Declarative Pipeline (SDP) Framework

A configuration-driven lakehouse framework for Databricks Lakeflow pipelines. Developers onboard new data sources and transformations by editing YAML config files — no pipeline code changes required.

## End-to-End Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (bronze_tables.yml → bronze_ingestion.py)                        │
│                                                                                 │
│  Cloud Storage (Volumes)                                                        │
│       │                                                                         │
│       ▼                                                                         │
│  ┌────────────────────┐   Reads bronze_tables.yml at pipeline startup.          │
│  │  Auto Loader       │   Creates one streaming table per YAML entry.           │
│  │  (cloudFiles)      │   Enforces explicit schema from column definitions.     │
│  └────────────────────┘   Adds _ingested_at and _source_file metadata.          │
│       │                                                                         │
│       ▼                                                                         │
│  ┌────────────────────┐   Applies dbt-style column tests (not_null,             │
│  │  Data Quality      │   accepted_values) and custom SQL rules.                │
│  │  Expectations      │   Actions: warn | drop | fail per rule.                 │
│  └────────────────────┘                                                         │
│       │                                                                         │
│       ▼                                                                         │
│  ┌────────────────────┐                                                         │
│  │  bronze_meta_*     │   Streaming tables with Liquid Clustering.              │
│  │  (Streaming Tables)│   e.g. bronze_meta_customers, bronze_meta_orders        │
│  └────────────────────┘                                                         │
└─────────────────────────────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (silver_tables.yml → silver_transformation.py)                   │
│                                                                                 │
│  bronze_meta_* tables                                                           │
│       │                                                                         │
│       ▼                                                                         │
│  ┌────────────────────┐   Reads source bronze stream via dp.read_stream().      │
│  │  Transformations   │   Applies a chain of transforms from YAML config:       │
│  │  Engine            │   deduplicate, filter, derive, rename, drop, cast.      │
│  └────────────────────┘                                                         │
│       │                                                                         │
│       ├── SCD Type 1 ──────────────────────────────────────────────┐            │
│       │   Overwrites existing rows on key match.                   │            │
│       │   No history kept. Uses dp.apply_changes().                │            │
│       │   e.g. silver_meta_clean_orders (keyed on order_id)        │            │
│       │                                                            │            │
│       ├── SCD Type 2 ──────────────────────────────────────────────┤            │
│       │   Inserts new version row when tracked columns change.     │            │
│       │   SDP auto-manages __START_AT, __END_AT, __IS_CURRENT.     │            │
│       │   Configurable: track_history_columns or except_columns.   │            │
│       │   e.g. silver_meta_clean_customers (tracks email, phone…)  │            │
│       │                                                            │            │
│       └── Append Mode (no SCD) ────────────────────────────────────┘            │
│           Simple streaming append via @dp.table().                              │
│           Used when no scd block is present in config.                          │
│       │                                                                         │
│       ▼                                                                         │
│  ┌────────────────────┐   Same DQ framework as bronze: column tests             │
│  │  Data Quality      │   and table-level rules applied on the                  │
│  │  Expectations      │   source view before merge into target.                 │
│  └────────────────────┘                                                         │
│       │                                                                         │
│       ▼                                                                         │
│  ┌────────────────────┐                                                         │
│  │  silver_meta_*     │   Cleaned, deduplicated, SCD-tracked tables.            │
│  │  (Streaming Tables)│   e.g. silver_meta_clean_customers (SCD2)               │
│  └────────────────────┘        silver_meta_clean_orders (SCD1)                  │
└─────────────────────────────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  (gold_tables.yml → gold_aggregation.py)                            │
│                                                                                 │
│  silver_meta_* tables                                                           │
│       │                                                                         │
│       ├── table_type: aggregate ───────────────────────────────────┐            │
│       │   Auto-builds SQL from YAML config:                        │            │
│       │   source + joins + group_by + metrics.                     │            │
│       │   e.g. gold_meta_customer_order_summary                    │            │
│       │        gold_meta_monthly_revenue                           │            │
│       │                                                            │            │
│       └── table_type: sql ─────────────────────────────────────────┘            │
│           Raw SQL for full flexibility. Write any SELECT:                        │
│           CTEs, window functions, RANK, NTILE, CASE,                            │
│           UNION, PIVOT — whatever you need.                                     │
│           e.g. gold_meta_high_value_customers                                   │
│                gold_meta_payment_method_revenue                                 │
│       │                                                                         │
│       ▼                                                                         │
│  ┌────────────────────┐   Per-table toggle:                                     │
│  │  Output Mode       │   materialized_view: true  → dp.materialized_view()    │
│  │                    │     Auto-refreshes when upstream silver data changes.   │
│  │                    │   materialized_view: false → dp.table()                 │
│  └────────────────────┘     Standard batch table.                               │
│       │                                                                         │
│       ▼                                                                         │
│  ┌────────────────────┐                                                         │
│  │  gold_meta_*       │   Business-ready tables for BI dashboards,              │
│  │  (MVs / Tables)    │   reporting, and downstream consumption.                │
│  └────────────────────┘                                                         │
└─────────────────────────────────────────────────────────────────────────────────┘
       │
       ▼
  Databricks AI/BI Dashboards, Genie Spaces, SQL Analytics, dbt, etc.
```

## Project Structure

```
sdp-meta-data-driven/
├── databricks.yml                              # Databricks Asset Bundle (dev/prod)
├── pyproject.toml                              # Python package config
├── resources/
│   └── meta_driven_etl.pipeline.yml            # SDP pipeline resource
└── src/meta_driven_etl/
    ├── config/
    │   ├── bronze_tables.yml                   # Bronze layer config
    │   ├── silver_tables.yml                   # Silver layer config (SCD1/SCD2)
    │   └── gold_tables.yml                     # Gold layer config (MVs + SQL)
    └── pipeline/
        ├── bronze_ingestion.py                 # Bronze engine (do not edit)
        ├── silver_transformation.py            # Silver engine (do not edit)
        └── gold_aggregation.py                 # Gold engine (do not edit)
```

---

## Bronze Layer

### How It Works

The engine reads `bronze_tables.yml` and dynamically creates one streaming table per entry using Auto Loader (`cloudFiles`). Each table gets:
- Explicit schema enforcement from column definitions
- Data quality expectations from dbt-style tests and custom rules
- Automatic `_ingested_at` and `_source_file` metadata columns
- Liquid Clustering via `cluster_by`

### Adding a Bronze Table

```yaml
# src/meta_driven_etl/config/bronze_tables.yml
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
      - name: total_amount
        type: "DECIMAL(10,2)"
    cluster_by:
      - order_date
    data_quality:
      - name: positive_amount
        expression: "total_amount > 0"
        action: drop
```

Output: `bronze_meta_orders` streaming table.

---

## Silver Layer

### How It Works

The engine reads `silver_tables.yml` and creates silver tables in one of two modes:

| Mode | When | SDP Pattern |
|------|------|-------------|
| **SCD** (Type 1 or 2) | `scd` block present | `create_streaming_table()` + `@dp.view()` + `apply_changes()` |
| **Append** | No `scd` block | `@dp.table()` with returned DataFrame |

### Transformation Types

| Type | Description |
|------|-------------|
| `deduplicate` | Window-based dedup using `ROW_NUMBER()` |
| `filter` | SQL WHERE condition |
| `derive` | Add computed columns via SQL expressions |
| `rename` | Rename columns |
| `drop_columns` | Remove columns from output |
| `cast` | Change column data types |

### SCD Configuration

```yaml
# src/meta_driven_etl/config/silver_tables.yml
tables:
  clean_customers:
    description: "Customer dimension with SCD2 history"
    source:
      table: "bronze_meta_customers"

    scd:
      type: 2                          # 1 = overwrite, 2 = versioned history
      keys:
        - customer_id                  # business key(s)
      sequence_by: "_ingested_at"      # ordering column
      track_history_columns:           # SCD2 only: columns that trigger new version
        - email
        - phone
        - loyalty_tier
      ignore_null_updates: true        # skip nulls in incoming data

    transformations:
      - type: derive
        columns:
          - name: full_name
            expression: "CONCAT(first_name, ' ', last_name)"
            type: STRING
```

**SCD Type 1**: Overwrites existing row on key match. No history.
**SCD Type 2**: Creates new version row. SDP auto-manages `__START_AT`, `__END_AT`, `__IS_CURRENT` columns.

### SCD Parameters Reference

| Parameter | Required | Description |
|-----------|----------|-------------|
| `scd.type` | Yes | `1` (overwrite) or `2` (versioned history) |
| `scd.keys` | Yes | Business key column(s) for matching |
| `scd.sequence_by` | Yes | Column to determine record ordering |
| `scd.track_history_columns` | No | SCD2 only: columns that trigger a new version |
| `scd.except_columns` | No | SCD2 only: columns to exclude from tracking (mutually exclusive with above) |
| `scd.ignore_null_updates` | No | If true, null values won't overwrite existing non-null values (default: false) |

---

## Gold Layer

### How It Works

The engine reads `gold_tables.yml` and creates either materialized views or standard tables. Two table types are supported:

| `table_type` | Description |
|--------------|-------------|
| `aggregate` | Auto-builds SQL from `group_by` + `metrics` + optional `joins` |
| `sql` | Raw SQL query for full flexibility (CTEs, window functions, UNION, PIVOT, etc.) |

### Output Mode

```yaml
defaults:
  materialized_view: true    # default for all gold tables

tables:
  monthly_revenue:
    materialized_view: true  # dp.materialized_view() — auto-refreshes on upstream changes
  loyalty_tier:
    materialized_view: false # dp.table() — standard batch table
```

### Aggregate Example

```yaml
tables:
  customer_order_summary:
    table_type: aggregate
    materialized_view: true
    source:
      primary_table: "silver_meta_clean_orders"
      joins:
        - table: "silver_meta_clean_customers"
          alias: "c"
          type: inner
          condition: "orders.customer_id = c.customer_id"
    group_by:
      - "orders.customer_id"
      - "c.full_name"
    metrics:
      - name: total_orders
        expression: "COUNT(orders.order_id)"
      - name: total_spend
        expression: "SUM(orders.total_amount)"
```

### SQL Example

```yaml
tables:
  high_value_customers:
    table_type: sql
    materialized_view: true
    sql: |
      WITH customer_metrics AS (
        SELECT
          o.customer_id,
          c.full_name,
          SUM(o.net_amount) AS lifetime_spend
        FROM silver_meta_clean_orders o
        INNER JOIN silver_meta_clean_customers c
          ON o.customer_id = c.customer_id
        GROUP BY o.customer_id, c.full_name
      )
      SELECT *,
        RANK() OVER (ORDER BY lifetime_spend DESC) AS spend_rank,
        CASE
          WHEN lifetime_spend >= 10000 THEN 'VIP'
          WHEN lifetime_spend >= 5000  THEN 'High'
          ELSE 'Standard'
        END AS value_segment
      FROM customer_metrics
    data_quality:
      - name: valid_spend
        expression: "lifetime_spend > 0"
        action: warn
```

---

## Data Quality (All Layers)

### Column Tests (dbt-style)

```yaml
columns:
  - name: status
    type: STRING
    tests:
      - not_null
      - accepted_values:
          values: ["active", "inactive"]
```

| Test | Maps To | Behavior |
|------|---------|----------|
| `not_null` | `dp.expect("col_not_null", "col IS NOT NULL")` | Warns on null |
| `accepted_values` | `dp.expect("col_accepted_values", "col IN (...)")` | Warns on invalid |

### Table-Level Rules

```yaml
data_quality:
  - name: valid_email
    expression: "email LIKE '%@%.%'"
    action: drop
```

| Action | SDP Expectation | Behavior |
|--------|----------------|----------|
| `warn` | `dp.expect()` | Log violation, keep the row |
| `drop` | `dp.expect_or_drop()` | Silently remove violating rows |
| `fail` | `dp.expect_or_fail()` | Halt the pipeline on any violation |

---

## Supported Column Types

`STRING`, `INT`, `INTEGER`, `BIGINT`, `LONG`, `DOUBLE`, `FLOAT`, `BOOLEAN`, `DATE`, `TIMESTAMP`, `SHORT`, `BYTE`, `BINARY`, `DECIMAL(p,s)`

---

## Currently Onboarded Tables

### Bronze

| Table | Source | Format | DQ Rules |
|-------|--------|--------|----------|
| `bronze_meta_customers` | `customers/` | CSV | 3 column tests + 3 table rules |
| `bronze_meta_orders` | `orders/` | CSV | 4 column tests + 4 table rules |

### Silver

| Table | Source | SCD | Transformations |
|-------|--------|-----|-----------------|
| `silver_meta_clean_customers` | `bronze_meta_customers` | Type 2 (tracks email, phone, address, loyalty_tier) | derive (full_name, signup_year), drop_columns |
| `silver_meta_clean_orders` | `bronze_meta_orders` | Type 1 (overwrite) | derive (net_amount, order_year, order_month), filter, drop_columns |

### Gold

| Table | Type | MV | Description |
|-------|------|----|-------------|
| `gold_meta_customer_order_summary` | aggregate + join | Yes | Per-customer order metrics |
| `gold_meta_monthly_revenue` | aggregate | Yes | Monthly revenue and order metrics |
| `gold_meta_loyalty_tier_analysis` | aggregate + join | No | Metrics by loyalty tier |
| `gold_meta_high_value_customers` | sql | Yes | Top customers with ranking and segments |
| `gold_meta_payment_method_revenue` | sql | No | Revenue by payment method and month |

---

## Environments

| Target | Schema | Pipeline Name | Mode |
|--------|--------|---------------|------|
| `dev` | `bronze_dev` | `[dev username] meta_driven_etl` | Development |
| `prod` | `bronze` | `meta_driven_etl` | Production |

---

## Commands

```bash
# Validate bundle config
databricks bundle validate

# Deploy to dev (default)
databricks bundle deploy

# Deploy to prod
databricks bundle deploy --target prod

# Run pipeline
databricks bundle run meta_driven_etl

# Run with full refresh (recreate all tables)
databricks bundle run meta_driven_etl --refresh-all
```
