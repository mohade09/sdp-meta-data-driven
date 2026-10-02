# Gold Layer Data Contracts

Open Data Contract Standard (ODCS v3.0.1) contracts for the gold-layer tables
produced by the `meta_driven_etl` pipeline.

## Source of truth

Each contract mirrors an entry in
[`src/meta_driven_etl/config/gold_tables.yml`](../src/meta_driven_etl/config/gold_tables.yml).
The physical tables are generated dynamically by
[`src/meta_driven_etl/pipeline/gold_aggregation.py`](../src/meta_driven_etl/pipeline/gold_aggregation.py)
into Unity Catalog:

- Catalog: `vdm_classic_aerfvt_catalog`
- Schema: `gold` (prod) / `gold_dev` (dev)
- Naming: `gold_meta_<table_name>`

When you change `gold_tables.yml`, update the matching contract and bump its
`version`.

## Contracts

| Contract file | Table | Type | Grain |
|---|---|---|---|
| `gold_customer_order_summary.yml` | `gold_meta_customer_order_summary` | materialized view | `customer_id` |
| `gold_monthly_revenue.yml` | `gold_meta_monthly_revenue` | materialized view | `order_year`, `order_month` |
| `gold_loyalty_tier_analysis.yml` | `gold_meta_loyalty_tier_analysis` | table | `loyalty_tier` |
| `gold_high_value_customers.yml` | `gold_meta_high_value_customers` | materialized view | `customer_id` |
| `gold_payment_method_revenue.yml` | `gold_meta_payment_method_revenue` | table | `order_year`, `order_month`, `payment_method` |
| `gold_daily_event_summary.yml` | `gold_meta_daily_event_summary` | materialized view | `event_date`, `event_type`, `device_type` |
| `gold_customer_engagement.yml` | `gold_meta_customer_engagement` | materialized view | `customer_id` |

## Notes

- **SQL-mode tables** (`high_value_customers`, `payment_method_revenue`,
  `customer_engagement`) are defined via raw SQL. Their physical types in the
  contracts are **inferred** from the SQL expressions — verify against the
  deployed schema (`DESCRIBE`) and adjust if Spark resolves them differently.
- Data-quality rules mirror the `data_quality` expectations in
  `gold_tables.yml`, plus an added uniqueness rule on each table's grain.
- These contracts are specification documents. They are **not** currently
  enforced by the pipeline or CI — wiring enforcement (e.g. SDP expectations
  generated from the contract, or a CI schema-drift check) would be a separate
  code change.
