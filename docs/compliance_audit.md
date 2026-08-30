# Compliance Audit — Self-Healing Data Pipeline Agent

**Date:** August 30, 2026  
**Auditor:** Automated Data Engineering & Agentic AI Specialist  
**Status:** Audit Complete  

---

## Executive Summary

An audit of the repository against the official **Self-Healing Data Pipeline Agent** assignment specification was conducted. While the local Python/Pandas data pipeline is operational and passes 8 unit tests, the current implementation has several gaps relative to the official production requirements (specifically regarding Airflow task lineage structure, BigQuery integration operators, BigQuery validation tasks, and SLA mechanisms).

This document serves as the compliance baseline prior to making structural updates to the pipeline.

---

## Compliance Matrix

| Requirement | Current Status | Evidence | Required Change |
| :--- | :--- | :--- | :--- |
| **1. Real Apache Airflow DAG** | **PASS** | `airflow/dags/pipeline_dag_starter.py` defines `self_healing_pipeline` DAG. `docker-compose.yml` runs Airflow 2.9.3 containerized with Postgres. | Ensure DAG compiles cleanly and matches official specification. |
| **2. Explicit task dependencies** | **PARTIAL** | Lineage is currently `[ingest_orders, ingest_events] >> validate_schema >> validate_quality >> transform_data >> verify_final`. Dimension ingestion (`customers`, `products`) is nested inside `ingest_orders()` function rather than being explicit tasks. | Split ingestion into 4 distinct explicit tasks: `ingest_customers`, `ingest_products`, `ingest_orders`, `ingest_events`. |
| **3. Retries** | **PASS** | `default_args` in `pipeline_dag_starter.py` defines `"retries": 2`. | Maintain retry configuration across all tasks in DAG. |
| **4. retry_delay** | **PASS** | `default_args` in `pipeline_dag_starter.py` defines `"retry_delay": timedelta(minutes=2)`. | Maintain retry_delay configuration across all tasks in DAG. |
| **5. on_failure_callback on every task** | **PASS** | `default_args` defines `"on_failure_callback": on_task_failure`. Callback writes structured JSON & MD incident reports. | Ensure callback extracts full context (including BigQuery/Airflow details) for future agent consumption. |
| **6. Task/DAG-level SLA for freshness** | **PARTIAL** | `default_args` defines `"sla": timedelta(hours=26)`. However, freshness checks are also partially hardcoded in `validate_quality()`. | Retain Airflow's native SLA parameter while integrating SLA breach callback logging. |
| **7. Ingestion → validation → transform → load → agent-monitoring lineage** | **PARTIAL** | Missing explicit `load_bigquery`, `bigquery_validation`, and `agent_monitor` tasks in Airflow DAG. | Restructure lineage to: `ingest_*` (4 tasks) → `schema_validation` → `quality_validation` → `transform` → `load_bigquery` → `bigquery_validation` → `agent_monitor`. |
| **8. Final analytics-ready data MUST be loaded into Google BigQuery** | **MISSING** | `pipeline_dag_starter.py` docstring states "NO external BigQuery or GCP dependencies." Data is only written to local `data/processed/`. | Implement BigQuery target tables loading (`dim_customers`, `dim_products`, `fct_orders`, `fct_events`) with partitioning and clustering via Airflow operators. |
| **9. BigQuery operators should be used through Airflow** | **MISSING** | No Airflow BigQuery operators (`BigQueryInsertJobOperator`, `BigQueryCheckOperator`, `LocalFilesystemToBigQueryOperator`, etc.) are imported or used. | Use official Airflow BigQuery operators to execute BigQuery DDL/DML and loads. |
| **10. Validation should be executable against BigQuery** | **MISSING** | Data validation is only performed in Pandas in `validate_schema` and `validate_quality`. | Add a `bigquery_validation` task running SQL assertions (row count, null rate, duplicates, referential integrity) directly against BigQuery tables. |
| **11. Future AI Agent tools (BigQuery, Airflow, Remediation)** | **PARTIAL** | System design doc outlines agent tools. Incident callback outputs structured incident JSON/MD. Agent execution itself is out-of-scope for this phase per architecture boundary. | Ensure incident output payloads supply all metadata required by future BigQuery query and Airflow status tools. |
| **12. Agent reasoning schema (OBSERVED → EVIDENCE → HYPOTHESIS → CONFIDENCE → BLAST RADIUS → ACTION)** | **PARTIAL** | Designed in `docs/design_document.md`. Markdown failure callback output partially maps to this schema. | Standardize failure callback and incident report templates to strictly follow the 6-stage schema. |
| **13. Agent must safely FIX or ESCALATE** | **PARTIAL** | Remediation policy defined in `pipeline_config.yaml`. Agent execution deferred to Phase 4. | Ensure DAG failure hooks support both escalation and remediation hooks cleanly. |
| **14. Every incident needs an evidence-backed incident report** | **PASS** | `on_task_failure` callback automatically generates `.json` and `.md` incident reports upon task failure. | Expand evidence collection details written by callback. |
| **15. At least 5 fault scenario tests** | **PARTIAL** | `tests/test_local_pipeline.py` contains 8 tests for data errors using Pandas, but Airflow/BigQuery fault injection tests in `tests/fault-injection/` are missing. | Add dedicated fault scenario tests covering Schema Drift, Volume Anomaly, Null Spike, Duplicate Ingestion, Referential Breakage, and Staleness. |
| **16. 3 real auto-generated incident reports** | **PARTIAL** | Failure callback code exists, but no auto-generated incident reports are saved in `incidents/reports/`. | Run fault tests/simulations to generate real incident report files in `incidents/reports/`. |
| **17. Final project must be demonstrable through Airflow UI** | **PARTIAL** | Airflow runs via Docker Compose, but the DAG graph view is missing BigQuery loading and monitoring nodes. | Update DAG structure to render full end-to-end lineage cleanly in Airflow UI. |

---

## Key Findings

1. **Local Pipeline Strengths:** The existing local Pandas ETL logic (`ingest_orders`, `ingest_events`, `validate_schema`, `validate_quality`, `transform_data`, `verify_final`) works reliably for local dev and has 8 passing unit tests. This MUST be preserved for local offline testing.
2. **Missing Production Lineage:** BigQuery loading and BigQuery SQL validation operators are missing from the DAG.
3. **Airflow Task Lineage Refactoring Needed:** Ingest tasks for dimensions (`customers` and `products`) need to be explicitly exposed as DAG nodes rather than embedded inside `ingest_orders()`.
4. **Environment Separation Required:** Local Pandas pipeline execution must be clearly separated from the official GCP BigQuery production pipeline path.
