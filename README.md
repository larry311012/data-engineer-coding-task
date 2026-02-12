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