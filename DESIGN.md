Design Document — BlueAlpha Data Pipeline 

Schema Design

- Layer Architecture:
- raw: Unmodified source data as text.
- staging: Cleaned, typed, and validated views with data quality flags (is_valid, is_outlier, is_refund).
- intermediate: Business logic layer; unifies ad platforms and filters CRM data.
- marts: Analytics-ready fact tables joining campaign metrics with attributed revenue.
- Grain: fact_campaign_daily (Platform + Campaign + Date). CRM revenue is pre-aggregated by source and date.

Rationale
I used a layered dbt structure to ensure clear responsibilities, preserve raw data for auditing, and provide high-performance tables for analysis.

Data Quality Strategy

- Date values arrive in multiple formats across all three sources (e.g., YYYY-MM-DD, MM/DD/YYYY, Month DD, YYYY); each is normalized to a standard date type via regex pattern matching and CASE-based parsing in the staging layer.
- The CRM source contains duplicate order_id rows, which are deduplicated using ROW_NUMBER() partitioned by order_id, keeping the first occurrence.
- CRM orders with revenue exceeding $10,000 are flagged as is_outlier and excluded from the intermediate layer, but preserved in staging for investigation since they could be legitimate enterprise deals.
- Negative revenue values in CRM are flagged as is_refund and kept in the pipeline as real business events, with refund amounts separated from attributed revenue in the marts layer.
- CRM rows missing required fields (order_id, customer_id, revenue, or order_date) are marked is_valid = FALSE and excluded from downstream analytics and attribution.
- Missing purchase counts in Facebook data are coalesced to zero, treating no recorded purchase as zero purchases rather than unknown.
- CRM channel names are lowercased in staging to ensure consistent grouping across variations like Google, google, and GOOGLE.

Architecture Decisions (ADRs)

- ADR 1 (Orchestration): Airflow 2.9 for robust DAG management, retries, and monitoring.
- ADR 2 (Transformations): dbt-postgres 1.8 for declarative SQL, testing, and lineage.
- ADR 3 (Outliers): Flag instead of drop to preserve data for investigation.
- ADR 4 (Idempotency): Delete-by-run_id + insert using UUIDs for safe re-runs.
- ADR 5 (Date Ambiguit): The CRM source contains dates in both MM/DD/YYYY and DD/MM/YYYY formats; when the first segment exceeds 12 (e.g., 15/01/2024)we can unambiguously parse as DD/MM, but for ambiguous cases like 08/01/2024 we default to MM/DD as a US convention — in production, the correct fix is to confirm and enforce a single date format at the source.

Trade-offs and Future Improvements

- Current Constraints: Raw tables use text columns; no automated alerting or lineage visualization beyond dbt.
- Roadmap: Implement proper DDL types, Airflow alerts, dbt freshness checks, dimension tables, CI/CD testing, and a dedicated "rejects" table for observability.
