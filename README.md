# BlueAlpha Data Pipeline

## Overview
A production-style data pipeline that ingests advertising data from three sources — Google Ads (JSON), Facebook Ads (CSV), and CRM (CSV) — into a PostgreSQL warehouse. Raw data is cleaned and transformed through a layered dbt architecture (staging, intermediate, marts) to produce an analytics-ready `fact_campaign_daily` table that unifies cross-platform campaign metrics with CRM-attributed revenue. Orchestrated by Airflow and fully containerized with Docker Compose.

## Architecture

```
                    +-------------+
                    |   Airflow   |
                    +------+------+
             +-------------+-------------+
             v             v             v
       Google Ads     Facebook       CRM CSV
       (JSON)         (CSV)          (CSV)
             |             |             |
             v             v             v
       raw.google     raw.facebook   raw.crm          <- Raw (all text)
             |             |             |
             v             v             v
       stg_google     stg_facebook   stg_crm          <- Staging (dbt views)
             |             |             |
             +------+------+        +----+
                    v                v
       int_campaign_daily    int_crm_orders           <- Intermediate (incremental)
                    |                |
                    +-------+--------+
                            v
                  fact_campaign_daily                  <- Marts (dbt table)
```

## Tech Stack
- **Python 3.11 + pandas** — Ingestion
- **PostgreSQL 16** — Data warehouse
- **dbt 1.11** — SQL transformations + testing
- **Apache Airflow 2.9** — Orchestration
- **Docker Compose** — Local environment

## Quick Start

```bash
docker-compose up --build

# Wait ~60s for Airflow to initialize, then:
# Airflow UI:  http://localhost:8081  (admin / admin)
# Adminer UI:  http://localhost:8080  (postgres / postgres / warehouse)
# Trigger DAG: bluealpha_data_pipeline
```

The DAG executes 6 tasks in sequence:
1. **create_run_context** — Generates a shared `run_id` (UUID) for the entire pipeline run, passed to all tasks via XCom
2. **ingest_crm** — Loads CRM revenue CSV into `raw.crm_revenue_raw`
3. **ingest_api** — Loads Google Ads JSON into `raw.google_ads_raw`
4. **ingest_export** — Loads Facebook CSV into `raw.facebook_export_raw`
5. **dbt_run** — Builds all 6 dbt models (staging -> intermediate -> marts)
6. **dbt_test** — Runs 13 data quality tests

## Project Structure

```
data-engineer-coding-task/
|-- airflow/dags/pipeline_dag.py        # Airflow DAG (6 tasks, XCom run_id)
|-- data/                               # Source files (JSON + 2 CSVs)
|-- dbt_project/
|   |-- models/
|   |   |-- staging/                    # Type casting, date parsing, dedup, flags
|   |   |-- intermediate/              # Union campaigns, filter valid CRM orders
|   |   |-- marts/                     # Join campaigns with CRM revenue
|   |   |-- schema.yml                 # Source definitions + 13 dbt tests
|   |-- dbt_project.yml
|   |-- profiles.yml
|-- docker/initdb/create_schema.sql     # DDL for schemas + raw tables
|-- pipeline/                           # Python ingestion modules
|   |-- config.py                       # Env-based settings
|   |-- db.py                           # Postgres connection helper
|   |-- run_context.py                  # Generates run_id per execution
|   |-- load.py                         # Atomic delete + insert (sql.Identifier)
|   |-- ingest_api_google_ads.py        # JSON -> raw
|   |-- ingest_export_facebook.py       # CSV -> raw
|   |-- ingest_crm_revenue.py           # CSV -> raw (with date comma fix)
|-- tests/                              # Python unit tests
|-- docker-compose.yml
|-- requirements.txt
|-- DESIGN.md                           # Architecture decisions and trade-offs
```

## Data Quality Handling

- Date values arrive in multiple formats across all three sources (e.g., `YYYY-MM-DD`, `MM/DD/YYYY`, `Month DD, YYYY`); each is normalized to a standard date type via regex pattern matching and CASE-based parsing in the staging layer.
- The CRM source contains duplicate `order_id` rows, which are deduplicated using `ROW_NUMBER()` partitioned by `order_id`, keeping the first occurrence.
- CRM orders with revenue exceeding $10,000 are flagged as `is_outlier` and excluded from the intermediate layer, but preserved in staging for investigation since they could be legitimate enterprise deals.
- Negative revenue values in CRM are flagged as `is_refund` and kept in the pipeline as real business events, with refund amounts separated from attributed revenue in the marts layer.
- CRM rows missing required fields (order_id, customer_id, revenue, or order_date) are marked `is_valid = FALSE` and excluded from downstream analytics and attribution.
- Missing purchase counts in Facebook data are coalesced to zero, treating no recorded purchase as zero purchases rather than unknown.
- CRM channel names are lowercased in staging to ensure consistent grouping across variations like `Google`, `google`, and `GOOGLE`.
- The CRM source contains dates in both MM/DD/YYYY and DD/MM/YYYY formats; when the first segment exceeds 12 (e.g., `15/01/2024`) we can unambiguously parse as DD/MM, but for ambiguous cases like `08/01/2024` we default to MM/DD as a US convention — in production, the correct fix is to confirm and enforce a single date format at the source.

## Assumptions

1. **Google `conversions` = Facebook `purchases`** — Both represent the same business event; unified as `purchases`/`purchase_value` in the intermediate layer.
2. **CRM `campaign_source` maps to ad `campaign_id`** — Enables the left join in `fact_campaign_daily` to attribute CRM revenue to campaigns.
3. **Revenue > $10,000 is an outlier** — Flagged and excluded from downstream aggregation; preserved in staging for investigation.
4. **First occurrence wins for duplicate orders** — Lowest `row_num` kept when multiple rows share the same `order_id`.
5. **Ambiguous dates default to MM/DD/YYYY** — Only parsed as DD/MM/YYYY when first segment > 12.
6. **All raw columns stored as text** — Type casting deferred to dbt staging layer for lossless raw storage.

## Idempotency

- A single `run_id` (UUID) is created per DAG execution and shared across all ingestion tasks via Airflow XCom
- Each ingestion task deletes-then-inserts within a single atomic transaction, keyed on `run_id`
- On retry, Airflow reuses the same XCom value, so the previous attempt's data is properly replaced
- dbt incremental models use `unique_key` and `load_ts` filtering to merge only new rows
- Airflow DAG: `catchup=False`, `schedule=None` (manual trigger)

## Data Validation Results

Pipeline output verified against source files (85 CRM rows, 45 Google rows, 45 Facebook rows):

| Test | Result |
|------|--------|
| CRM dedup: 0 duplicate order_ids in staging | PASS |
| CRM valid(80) + invalid(2) = staging total(82) | PASS |
| int_crm = valid(80) - outlier(1) = 79 | PASS |
| Outlier flagged: ORD-10081 at $9,999,999.99 | PASS |
| Refund flagged: ORD-10076 at -$50.00 | PASS |
| Invalid rows: null revenue (ORD-10008), null customer_id (ORD-10030) | PASS |
| Google spend = cost_micros / 1,000,000 | PASS |
| Channel names all lowercase | PASS |
| int_campaign = google(45) + facebook(45) = 90 | PASS |
| Shared run_id across all 3 sources (XCom) | PASS |
| Date range: both platforms Jan 1-15, 2024 | PASS |
| Revenue reconciliation: $89.75 gap explained by 1 unmatched campaign date | PASS |
| Attribution count: 79 - 1 refund = 78 in fact | PASS |
