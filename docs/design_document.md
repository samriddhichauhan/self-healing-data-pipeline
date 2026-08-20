# Engineering Design Document: Self-Healing Data Pipeline Agent

**Author:** Data Engineering & Agentic AI Intern  
**Status:** Proposed  
**Date:** August 19, 2026  
**Project Location:** [design_document.md](file:///c:/Users/samri/OneDrive/Documents/Internship%201/design_document.md)

---

## 1. Executive Summary

In modern enterprise environments, data pipelines are the lifeblood of decision-making. However, production pipelines frequently experience operational issues and silent data-quality failures (e.g., partial batch ingestions, schema drifts, null spikes, and referential breakages) that escape basic syntax checks. Standard alerting systems notify engineers of explicit task crashes but are blind to semantic data issues. Triage and recovery remain heavily manual processes, contributing to high Mean Time to Resolution (MTTR), operational overhead, and alert fatigue.

This document proposes a **Self-Healing Data Pipeline Agent** designed to automate level-1 monitoring and recovery. It combines deterministic data engineering controls with an agentic diagnostic layer. 

* **Orchestration & Data Flow:** Apache Airflow manages execution, retries, and task dependencies. Google BigQuery serves as the target cloud data warehouse, hosting raw, staging, and analytics-ready production datasets.
* **Agentic Diagnostics:** Rather than relying on simple, blind alert routing or generic LLM explanations, the system triggers a specialized AI Agent when validation checks fail or SLA breaches occur. The agent acts like an on-call data engineer: it uses secure tools to gather concrete evidence from BigQuery schemas/logs, Airflow task statuses, and historical runs.
* **Controlled Decision Framework:** A dedicated policy engine validates the agent's proposed action. The agent does not execute arbitrary code or write free-form SQL directly to production tables. If the root cause is deterministic and the remediation is approved, safe, and reversible (e.g., task reruns or deduplication merges), the system auto-remediates the issue. Otherwise, it escalates to a human engineer with an evidence-backed incident report.
* **Traceability & Safety:** Safety is baked into the design through strict write-privilege boundaries, absolute audit logging, and post-remediation validation loops.

---

## 2. Problem Statement

Production data pipelines are vulnerable to complex data-quality failures that do not necessarily crash the orchestrator, leading to silent data corruption or downstream reporting errors. The system must address the following six critical fault categories:

1. **Schema Drift:** Upstream APIs or databases rename fields, drop columns, or modify data types (e.g., changing a price from float to string). If staging schemas are static, downstream views and joins fail.
2. **Volume Anomaly:** A daily batch ingestion succeeds but contains significantly fewer rows than expected (e.g., due to an upstream extraction job failure) or a massive spike in rows due to upstream replication anomalies.
3. **Null / Quality Spike:** A column's data type remains correct, but a critical field (e.g., `order_total`) suddenly experiences a surge in NULL values or zeroes (e.g., 20% null rate compared to a historical average of <0.5%).
4. **Duplicate Ingestion:** Due to network retries, manual backfills, or lack of source-system deduplication, the same transaction batch is ingested multiple times, causing inflated business metrics.
5. **Referential Breakage:** Daily transactional orders are loaded referencing `customer_id` or `product_id` values that do not exist in the corresponding dimension tables, breaking analytical joins.
6. **Staleness:** The ingestion tasks complete successfully, but the source data itself is static or contains old dates. The maximum timestamp in the loaded data falls behind the configured Service Level Agreement (SLA).

To solve these problems without risking production stability, this project establishes a design for:
* **Automated Detection:** Identifying anomalies instantly at ingestion, staging, or post-load.
* **Evidence Collection:** Querying structural metadata and logs to document the scope of the problem.
* **Root-Cause Analysis:** Synthesizing evidence to generate a logical hypothesis.
* **Safe Remediation:** Applying deterministic, idempotent operations under strict validation.
* **Human Escalation:** Providing high-context reports for manual triage when confidence is low or risk is high.
* **Audit Trail:** Saving every observation, diagnostic step, and remediation action for post-mortem review.

---

## 3. Goals and Non-Goals

### Goals
* **Real Orchestration:** Implement a functional Apache Airflow DAG demonstrating data lineage.
* **Multi-Source Ingestion:** Ingest customers, products, daily orders, and daily clickstream events.
* **Multi-Stage Validation:** Implement validation checks at staging (pre-transform) and production (post-load).
* **Target Analytical Storage:** Load cleansed, typed, and normalized datasets into Google BigQuery.
* **Fault Detection:** Design the pipeline to actively identify the six target fault categories.
* **Evidence-Based Diagnosis:** Ensure the Agent bases decisions on factual log and metadata evidence, not LLM assumptions.
* **Safe Auto-Remediation:** Automate recovery for low-risk, deterministic faults (e.g., retrying transient tasks or running deduplication MERGEs).
* **Controlled Escalation:** Generate complete markdown incident reports for human engineers.
* **Comprehensive Audit Trail:** Track all operational and diagnostic logs in BigQuery/GCS.
* **Reproducible Testing:** Build a scenario-based test harness that injects faults and verifies the end-to-end self-healing flow.

### Non-Goals
* **No Blind SQL Execution:** The Agent will not write arbitrary SQL strings and execute them directly against production tables.
* **No Data Imputation/Guessing:** The Agent will never guess or generate fake replacement data for missing or NULL fields (e.g., substituting median revenue for NULLs).
* **No Silent Bypasses:** Validation checks cannot be skipped or bypassed without an auditable configuration change.
* **No Total Autonomy:** The system is not designed to operate entirely without human supervision; it acts as a co-pilot and automated level-1 triage mechanism.
* **No Unrestricted Remediation:** The Agent is restricted from running destructive operations, dropping tables, or altering production schemas directly.

---

## 4. High-Level Architecture

The architecture separates the core data engineering pipeline (orchestration, validation, and loading) from the diagnostic agent. This ensures that even if the agentic layer experiences a failure (e.g., API limits or LLM outages), the data pipeline remains functional and safe.

```mermaid
graph TD
    subgraph Source Layer
        C[customers.csv]
        P[products.csv]
        O[orders_date.csv]
        E[events_date.jsonl]
    end

    subgraph Orchestration Layer (Apache Airflow)
        Ingest[Staging Ingestion]
        Val1[Pre-Transform Validation]
        Trans[Transformation Layer]
        Load[Load Layer: BigQuery]
        Val2[Post-Load Validation]
    end

    C & P & O & E --> Ingest
    Ingest --> Val1
    Val1 --> Trans
    Trans --> Load
    Load --> Val2

    subgraph Agentic Diagnosis & Healing System
        Trigger{Trigger: Failure / SLA / Callback}
        Evidence[Evidence Collection Engine]
        BQTool[BigQuery Metadata Tool]
        AFTool[Airflow Log/State Tool]
        Diag[Diagnosis Engine]
        Decision{Decision Engine}
        Policy[Safety Policy Layer]
        Remed[Safe Auto-Remediation]
        Escal[Human Escalation]
        Report[Incident Report & Audit Trail]
    end

    Val1 -- Failure / Alert --> Trigger
    Val2 -- Failure / Alert --> Trigger
    Orchestration -- on_failure_callback --> Trigger

    Trigger --> Evidence
    Evidence --> BQTool
    Evidence --> AFTool
    BQTool & AFTool --> Diag
    Diag --> Decision
    Policy --> Decision
    Decision -- Approved, Safe --> Remed
    Decision -- Unsafe, Uncertain --> Escal
    Remed --> Report
    Escal --> Report
```

### DAG Lineage Structure

The pipeline is organized in parallel branches to prevent isolated failures in one data source from unnecessarily blocking other independent sources, while maintaining structural dependencies:

```
ingest_customers --> validate_customers --> transform_customers --> load_customers
ingest_products  --> validate_products  --> transform_products  --> load_products
daily_orders     --> validate_orders     --> transform_orders     --> load_orders
daily_events     --> validate_events     --> transform_events     --> load_events
                                  \             /
                                   \           /
                                  agent_monitoring
```

The tasks converge on a final `agent_monitoring` task that acts as a collector. Additionally, Airflow `on_failure_callback` hooks are configured at the task level to invoke the agent immediately upon any individual failure.

---

## 5. Technology Choices and Justification

### 5.1 Apache Airflow
* **Role:** Orchestration and lineage tracking.
* **Justification:** Airflow provides native DAG scheduling, task isolation, and automatic retries. Its task structure maps dependencies cleanly.
* **Evidence Hooks:** The task history, run duration, state (success/failed/upstream_failed), and stdout logs serve as core inputs for the diagnostic agent. 
* **Callbacks:** The `on_failure_callback` is leveraged to capture failures instantly, extracting the failing task's context (DAG ID, Task ID, Execution Date, Try Number) and passing it to the agent.
* **SLA Configuration:** Utilizing `sla` parameters on tasks and DAGs allows the scheduler to flag slow or hung tasks before they cause data staleness downstream.
* **Sensors:** Deferrable sensors (e.g., `GCSObjectExistenceSensor`) are used to wait for incoming daily files without occupying active worker slots.

### 5.2 Google BigQuery
* **Role:** Staging, transformation, analytical storage, and metadata logging.
* **Justification:** BigQuery is a highly scalable, serverless data warehouse optimized for analytical queries. It provides comprehensive `INFORMATION_SCHEMA` metadata for auditing and schema inspection.
* **Standard Operators:** All BigQuery interactions within the Airflow DAG are executed via standardized operators (e.g., `GCSToBigQueryOperator`, `BigQueryInsertJobOperator`, and `BigQueryCheckOperator`) rather than ad-hoc python client calls, ensuring clean task logs and credential isolation.
* **Performance Optimizations:** 
  * **Partitioning:** The `orders` and `events` tables are partitioned by day (using transaction date/event date) to limit query costs and optimize lookup performance during validation checks.
  * **Clustering:** Tables are clustered by common query keys (e.g., `customer_id` and `product_id`) to accelerate join queries.

### 5.3 AI Agent
* **Role:** Intelligent diagnostic synthesis, evidence correlation, and structured routing.
* **Justification:** Traditional data validation tells us *that* a table is empty, but it cannot correlate a drop in rows with an upstream Airflow API timeout. An AI agent is uniquely suited to read unstructured logs, trace dependencies, evaluate the business context, and formulate a detailed hypothesis.
* **Deterministic Execution:** The agent is designed as a tool-using executor. It is blocked from generating code or executing arbitrary database mutations directly. It communicates with Airflow and BigQuery solely via structured, read-only tools, reporting its findings to a deterministic policy engine that selects the corrective action.
* **Framework:** A custom function-calling state machine is proposed. It provides high execution safety, explicit state transitions, and avoids the complexity of complex, multi-agent frameworks while maintaining 100% deterministic guardrails.

---

## 6. Data Flow

The project processes four primary data sources, whose schemas, expectations, and SLAs are defined in `pipeline_config.yaml`. This configuration file serves as the single source of truth (the data contract) for both the pipeline validation tasks and the agent's diagnostic tool.

### Ingestion Lifecycle

```
[Source GCS Files] -> [Staging Tables (Raw)] -> [Pre-Transform Checks] -> [Transformation] -> [Target Production Tables] -> [Post-Load Checks]
```

1. **Source Discovery:** Daily files (`orders_YYYY-MM-DD.csv` and `events_YYYY-MM-DD.jsonl`) arrive in a GCS landing bucket.
2. **Ingestion & Staging:** Airflow tasks read files and load them into raw staging tables in BigQuery.
3. **Pre-Transform Validation:** Structural checks compare staging tables against the schema definitions in `pipeline_config.yaml`.
4. **Transformation:** Staging tables are joined, cleansed (standardizing date formats, formatting strings), and structured.
5. **Loading Target Tables:** Cleaned data is appended or merged into the production analytics tables.
6. **Post-Load Validation:** Referential integrity, null rates, and duplication queries run against the production tables.
7. **Signal Capture:** If any validation check returns a non-zero count of failures, a failure signal is dispatched to the monitoring agent.

---

## 7. Validation and Monitoring Architecture

Data validation is implemented as dedicated tasks using BigQuery SQL queries. Below is the validation contract mapped to the six required fault categories:

| Fault Category | Detection Signal | Evidence Gathered | Possible Response |
| :--- | :--- | :--- | :--- |
| **7.1 Schema Drift** | `validate_schema` task fails | `INFORMATION_SCHEMA.COLUMNS` definitions, file headers, and `pipeline_config.yaml` values | **Escalate.** The agent collects the mismatch details. Structural changes are blocked from auto-remediation to prevent downstream query breakages. |
| **7.2 Volume Anomaly** | `validate_row_count` task fails | Current partition count vs historical median; `pipeline_config.yaml` minimum bounds | **Auto-Remediate / Escalate.** If the source file was partially loaded due to a transient extraction cutoff, the agent triggers a rerun of the GCS ingestion task. If the source file is actually truncated, it escalates. |
| **7.3 Null / Quality Spike** | `validate_null_rate` task fails | SQL query calculating percentage of NULLs in critical columns | **Escalate.** Auto-remediation is blocked. The agent documents the percentage spike and locates the specific source records containing NULL values. |
| **7.4 Duplicate Ingestion** | `validate_duplicates` task fails | `COUNT(*) - COUNT(DISTINCT business_key)` query in BigQuery | **Auto-Remediate.** The agent executes a structured `MERGE` query that uses partition-level transaction deduplication based on transaction ID and updated timestamps. |
| **7.5 Referential Breakage** | `validate_joins` task fails | Outer join query looking for orphaned keys (e.g., `orders.customer_id IS NULL`) | **Escalate.** Creating dummy dimensions is prohibited. The agent reports the count of orphaned rows and a sample of the missing keys. |
| **7.6 Staleness** | `validate_freshness` task fails | `MAX(timestamp)` query in BigQuery compared to Current Time and SLA configs | **Auto-Remediate / Escalate.** If the upstream source file is delayed, the agent checks if the GCS landing sensor is still active. If the sensor has timed out, it attempts one restart. If the file is missing, it escalates. |

---

## 8. Agent Architecture

The agent is designed as a state-based diagnostic runner separated into logical phases.

```
+--------------------+      +-----------------------+      +------------------------+      +-------------------+
| Stage 1: Detection | ---> | Stage 2: Evidence Col | ---> | Stage 3: Policy Check | ---> | Stage 4: Execution|
| (Signals/Callback) |      | (Query / Log Tools)   |      | (Safety Gate)          |      | (Remed. or Escal.)|
+--------------------+      +-----------------------+      +------------------------+      +-------------------+
```

### Stage 1: Incident Detection
When a failure occurs, the trigger system instantiates a structured `Incident` state object:
```json
{
  "incident_id": "INC-20260819-9831",
  "timestamp": "2026-08-19T16:04:35Z",
  "dag_id": "daily_data_pipeline",
  "task_id": "validate_orders_null_rate",
  "execution_date": "2026-08-18",
  "try_number": 1,
  "failure_category": "NULL_SPIKE",
  "error_message": "Post-load check failed: NULL count on 'order_total' exceeded SLA threshold (0.5%). Found 18.2%."
}
```

### Stage 2: Evidence Collection
The agent receives the `Incident` object and utilizes a set of restricted API tools to populate a diagnostic context block. It is strictly prohibited from executing DDL/DML statements during this phase.

* **BigQuery Tool (`execute_diagnostic_query`):** A read-only database tool that executes pre-compiled SQL queries to count rows, inspect columns, compare historical statistics, and query the `INFORMATION_SCHEMA` metadata.
* **Airflow Tool (`fetch_task_metadata`):** An API integration that retrieves task state history, reads stdout/stderr logs for execution errors, checks retry parameters, and traces the state of upstream dependencies.

---

## 9. Diagnosis Logic

To ensure explainability, the agent's reasoning engine must structure its analysis into six core components. Hallucinations or unstructured summaries are prevented by requiring each component to reference specific data items returned by the tools:

* **OBSERVED:** A precise description of the error event.
  * *Example:* "Task `validate_orders_null_rate` failed on execution date 2026-08-18."
* **EVIDENCE:** The quantitative or qualitative findings returned by the tools.
  * *Example:* "BigQuery query `SELECT COUNT(*), COUNT(order_total) FROM staging.orders` returned 10,000 total rows and 1,820 NULLs. Airflow task logs show no ingestion errors."
* **HYPOTHESIS:** The deduced technical cause of the anomaly.
  * *Example:* "The upstream transaction exporter sent data containing unpopulated prices, possibly due to a database schema change at the source."
* **CONFIDENCE:** An assessment of confidence (High, Medium, Low) based on the evidence completeness.
  * *Example:* "High (100% of affected rows contain null values in the same column, staging table schema matches config, indicating source data anomaly)."
* **BLAST RADIUS:** The downstream downstream files, tables, views, and dashboards impacted by this issue.
  * *Example:* "`analytics.daily_sales_summary` will reflect deflated revenue metrics. Downstream forecasting DAG `weekly_sales_forecast` will ingest corrupted input data."
* **ACTION:** The determined path forward.
  * *Example:* "Escalate to on-call engineer. Auto-remediation is blocked by safety policy (NULL_SPIKE is classified as unsafe)."

---

## 10. Fix vs Escalate Decision Framework

The agent is blocked from selecting remediation paths by default. An action is only executed if it satisfies a strict security checklist. If any of the following checks evaluate to `False`, the agent aborts remediation and escalates immediately:

```
                  +-----------------------------------------+
                  | Agent Proposes Remediation Action       |
                  +-----------------------------------------+
                                       |
                                       v
            [Check 1: Is root cause backed by high evidence?]  -- No -->  [Escalate]
                                       | Yes
                                       v
            [Check 2: Is the proposed action deterministic?]   -- No -->  [Escalate]
                                       | Yes
                                       v
            [Check 3: Could action cause data corruption?]     -- Yes --> [Escalate]
                                       | No
                                       v
            [Check 4: Is action on the approved whitelist?]    -- No -->  [Escalate]
                                       | Yes
                                       v
            [Check 5: Is the operation auditable/reversible?]  -- No -->  [Escalate]
                                       | Yes
                                       v
                         +---------------------------+
                         | Execute Auto-Remediation  |
                         +---------------------------+
```

1. **Evidentiary Support:** Is the root cause validated by matching logs or database metrics? (Confidence must be **High**).
2. **Determinism:** Is the correction predictable? (e.g., rerunning a task produces a repeatable output, whereas generating data is non-deterministic).
3. **Loss Prevention:** Does the remediation risk dropping tables, wiping valid rows, or altering schemas? (Must be **No**).
4. **Approval Whitelist:** Is the action explicitly registered in `pipeline_config.yaml` as an approved auto-heal playbook? (Must be **Yes**).
5. **Auditability & Reversibility:** Can the operation be undone? Is there a rollback path (such as rewriting a daily partition from a backup staging table)? (Must be **Yes**).

---

## 11. Examples of Safe vs Unsafe Actions

| Potentially Safe (Auto-Remediation Allowed) | Unsafe / Prohibited (Human Escalation Required) |
| :--- | :--- |
| **Rerunning Ingestion Task:** Rerunning a staging task when the log displays a transient connection timeout. | **Schema Coercion:** Silently altering database column types or dropping columns that are not in the configuration. |
| **Deduplication Merge:** Deduplicating staging data into partition tables using standard, pre-approved primary key merges. | **Data Imputation:** Guessing, generating, or averaging metrics to replace NULL values or missing entries. |
| **Downstream Restart:** Rerunning a downstream view recalculation task after the parent staging table is verified as resolved. | **Ad-hoc SQL Generation:** Executing arbitrary LLM-generated `UPDATE` or `DELETE` statements on tables. |
| **SLA Sensor Restart:** Restarting a GCS sensor that timed out before the file was uploaded, if the file has now appeared. | **Silent Deletions:** Deleting non-matching records in referential integrity failures instead of escalating. |

---

## 12. Auto-Remediation Flow

When an approved, safe remediation is identified, the agent follows a strict transactional flow to execute the correction and verify success:

```
[Trigger] 
   |
   v
[Identify Safe Action]
   |
   v
[Write Audit Log: Pending Execution]
   |
   v
[Run Remediation Action] 
   |
   v
[Execute Post-Remediation Validation Task]
   |
   +---> [Success] ---> [Write Audit Log: Success] ---> [Close Incident]
   |
   +---> [Failure] ---> [Trigger Rollback] ---> [Write Audit Log: Failed] ---> [Escalate to Human]
```

1. **Audit Initiation:** Write an entry to the `incident_audit_trail` table in BigQuery, marking the state as `Remediation_Pending`.
2. **Execution:** Call the restricted execution tool (e.g., calling Airflow API to trigger a task instance rerun).
3. **Verification Loop:** Monitor the Airflow task state. Once complete, run the specific post-load validation check query that originally flagged the failure.
4. **Finalization:** If the validation check passes, update the audit log to `Remediation_Success` and close the incident.
5. **Fallback Escalation:** If the validation check fails or the task crashes again, run the rollback query (e.g., restoring the partition from a backup staging partition), log the result as `Remediation_Failed`, and escalate immediately to a human operator.

---

## 13. Escalation Flow

For unsafe faults, low-confidence diagnoses, or failed auto-remediations, the system initiates the escalation protocol:

1. **Incident Serialization:** The agent outputs a structured JSON payload containing the diagnosis path.
2. **Report Generation:** A Python helper compiles the JSON into a clean, markdown-formatted **Incident Escalation Report**.
3. **Notification:** The report is saved to `gcs://<bucket>/incidents/INC-XXXX.md` and registered in the `incident_reporting` table. In a production environment, this event triggers a webhook to alerting systems (e.g., Slack or PagerDuty).
4. **Handover:** The Airflow DAG execution halts, ensuring no corrupt data propagates downstream. The human on-call engineer inherits a pre-triaged incident report containing all the diagnostic evidence needed to begin debugging.

---

## 14. Incident Reporting Template

Every incident, whether auto-remediated or escalated, generates a standardized Markdown report saved in GCS. The template is structured as follows:

```markdown
# Incident Report: [Incident ID]

## Status: [RESOLVED / ESCALATED]
* **Detection Timestamp:** YYYY-MM-DD HH:MM:SS UTC
* **Source DAG:** [DAG ID]
* **Source Task:** [Task ID]
* **Affected Execution Date:** YYYY-MM-DD
* **Failure Category:** [SCHEMA_DRIFT / VOLUME_ANOMALY / NULL_SPIKE / DUPLICATE / REFERENTIAL / STALENESS]

## 1. Executive Summary
[A plain-English summary of what went wrong and what the current status is.]

## 2. Diagnosis Details
* **Observed Problem:** [Description of the failing task and error messages.]
* **Evidence Gathered:**
  * *Airflow Logs:* `[Log Snippet]`
  * *BigQuery Metrics:* `[SQL outputs, row counts, null rates]`
* **Hypothesis:** [Likely root cause]
* **Confidence Level:** [High / Medium / Low] (With reasoning)

## 3. Impact Analysis (Blast Radius)
* **Target Table:** [Table Name]
* **Downstream Tasks Affected:** [List of dependent tasks]
* **Downstream Reports Impacted:** [Affected downstream tables/dashboards]

## 4. Actions & Resolution
* **Remediation Attempted:** [None / Detail of action executed]
* **Post-Remediation Verification:** [Pass / Fail / N/A]
* **Audit Trail Link:** [Link to BigQuery Job ID or Airflow Run ID]

## 5. Escalation & Rollback (If applicable)
* **Escalation Reason:** [Why auto-remediation was not approved or failed]
* **Rollback Plan:** [Step-by-step instructions to restore data state]
* **Recommended Human Action:** [Specific debugging starting points]
```

---

## 15. Reliability and Safety Principles

1. **Evidence Before Action:** The agent is forbidden from forming hypotheses or acting without verifying logs and running metadata queries.
2. **Least Privilege:** The agent's service credentials have read-only access to source/staging data and restricted, procedure-based write access for remediation. It cannot delete or modify historical tables.
3. **No Silent Data Corruption:** A failure must halt the DAG branch rather than allowing corrupt data to merge into production tables.
4. **Deterministic Remediation Only:** Only explicitly whitelisted, idempotent operations can be automated.
5. **Human Escalation when Uncertain:** If confidence is below "High," or if the issue violates safety policies, immediate escalation is mandatory.
6. **Every Action is Auditable:** Every tool execution, diagnosis step, and API call must be logged to a central database.
7. **Idempotent Pipeline Design:** Retrying any task in the pipeline must yield the exact same end state without duplicating data.
8. **Validation Before and After Remediation:** Remediation actions are wrapped in strict pre-checks and post-checks to guarantee correctness.
9. **Retry Transient Failures:** Operational retries are handled natively by Airflow (e.g., transient network drops), keeping the agent focused on data-quality anomalies.
10. **Separate Diagnosis from Execution:** The diagnostic layer runs as a read-only analysis phase. The execution layer is a separate, highly constrained module that validates the action against the safety whitelist before running it.

---

## 16. Idempotency

Duplicate records are a common issue in ETL pipelines. To prevent duplication during reruns or auto-remediations, the pipeline implements the following controls:

* **Write-Truncate on Staging:** Airflow ingestion tasks write to staging tables using a write-truncate mechanism or temporary table overwrite. This guarantees that if an ingestion task is rerun, the staging table is reset.
* **Partition Overwrites:** For historical fact tables, the pipeline utilizes partition-based loading. Rerunning a specific execution date completely replaces that day's partition in BigQuery, preventing row duplication:
  ```sql
  -- MERGE query example enforcing idempotency
  MERGE INTO production.orders TARGET
  USING staging.orders SOURCE
  ON TARGET.order_id = SOURCE.order_id AND TARGET.order_date = SOURCE.order_date
  WHEN MATCHED THEN
    UPDATE SET order_total = SOURCE.order_total, customer_id = SOURCE.customer_id, updated_at = SOURCE.updated_at
  WHEN NOT MATCHED THEN
    INSERT (order_id, order_date, order_total, customer_id, created_at, updated_at)
    VALUES (SOURCE.order_id, SOURCE.order_date, SOURCE.order_total, SOURCE.customer_id, SOURCE.created_at, SOURCE.updated_at)
  ```
* **Unique Transaction Keys:** Orders and clickstream events include a composite unique business key (e.g., `order_id` + `order_date` or `event_id` + `event_timestamp`) which is checked by the deduplication tasks before target table insertion.

---

## 17. Error Handling

Error handling distinguishes between operational errors and data-quality anomalies:

```
                  +-----------------------------------------+
                  |              Task Failure               |
                  +-----------------------------------------+
                                       |
                                       v
                    [Is it a transient connection error?]
                                  /         \
                            Yes  /           \ No (Data Quality / Validation Fail)
                                v             v
                    [Airflow Native Retry]   [Trigger Agent Monitoring]
                            /       \                 |
                   Success /         \ Fail           v
                          v           v       [Agent Diagnosis Loop]
                     [Resume]     [Agent]     /                    \
                                             v                      v
                                     [Auto-Remediate]          [Escalate]
```

* **Airflow Native Retries:** Tasks are configured with `retries=3` and `retry_delay=5` minutes. This handles temporary network drops, database lock timeouts, or API rate limits. The agent is not triggered for these transient retries.
* **Permanent Task Failures:** If a task exhausts its native retries and still fails, it triggers the Airflow `on_failure_callback`, which starts the Agent monitoring flow.
* **Validation Failures:** Validation tasks (which run checking queries) do not fail silently. If validation fails, they raise an explicit Airflow exception, triggering the callback.
* **Agent Self-Failure:** If the Agent itself crashes (e.g., LLM API timeout), a fail-safe exception block triggers, causing the `agent_monitoring` task to fail and raise a high-priority system alert. The data pipeline is halted safely.

---

## 18. Security

* **Service Account Hardening:** The pipeline runs under a dedicated GCP Service Account. It is granted the minimum IAM roles required: `roles/bigquery.dataEditor` for staging/production tables and `roles/storage.objectViewer` for source buckets.
* **Credential Isolation:** Credentials are managed strictly via Airflow Connections or Google Secret Manager. Service account JSON keys are never committed to the git repository.
* **Agent Permission Limits:** The Agent executes under a secondary service account with read-only access (`roles/bigquery.metadataViewer` and `roles/bigquery.jobUser`) for running checks. Write permissions are restricted to a specific dataset containing the audit logs.
* **Budget Alerts:** A strict billing budget of $10.00 is set on the GCP project, sending immediate notifications to prevent runaway cost loops from recursive query executions or stuck sensors.

---

## 19. Testing Strategy

The pipeline and self-healing agent must undergo robust unit and integration testing.

### 19.1 Unit Testing
* **Transformations:** Python-based schema parser and date standardization functions will be unit tested using mock data structures in a local pytest environment.
* **Agent Diagnosis Logic:** The classification models and reasoning state machine will be tested by feeding mock `Incident` objects and mock query results, asserting that the correct `Hypothesis`, `Confidence`, and `Action` are generated.

### 19.2 Scenario Testing
A scenario test suite will inject the five specific faults into the source data to verify end-to-end detection, diagnosis, and action execution:

```
[Inject Fault File] -> [Run DAG] -> [Verify Action & Report Output]
```

1. **Schema Drift Scenario:**
   * *Injection:* Modify `customers.csv` by renaming `customer_id` to `cust_id`.
   * *Expected Output:* Ingestion task fails validation, Agent collects schema evidence, asserts a schema drift hypothesis, and escalates to a human.
2. **Volume Anomaly Scenario:**
   * *Injection:* Truncate `orders_YYYY-MM-DD.csv` from the typical ~10,000 rows to exactly 12 rows.
   * *Expected Output:* Validation task detects a drop outside the 3-sigma historical threshold, Agent verifies the file is completely ingested (no partial failures), classifies it as an upstream exporter volume anomaly, and escalates.
3. **Null Spike Scenario:**
   * *Injection:* Set the `order_total` field to NULL for 25% of records in `orders_YYYY-MM-DD.csv`.
   * *Expected Output:* Post-load validation flags the null spike, Agent gathers NULL rate evidence, classifies it as a data-quality failure, and escalates (no auto-imputation occurs).
4. **Duplicate Ingestion Scenario:**
   * *Injection:* Append 500 duplicate transaction rows to `orders_YYYY-MM-DD.csv`.
   * *Expected Output:* Validation task flags duplicate business keys, Agent executes a safe deduplication `MERGE` query, verifies the final table contains zero duplicate keys, and closes the incident.
5. **Referential Breakage Scenario:**
   * *Injection:* Edit `orders_YYYY-MM-DD.csv` to replace the `customer_id` values of 100 records with a non-existent identifier (`99999`).
   * *Expected Output:* Post-load validation flags the foreign key violation, Agent collects sample invalid keys, maps the blast radius, and escalates.

---

## 20. Observability

To ensure complete operational visibility, every step of the pipeline execution and self-healing loop is recorded:

* **Operational Dashboards:** BigQuery houses a central table `monitoring.pipeline_executions` recording DAG execution times, rows processed, and validation check outcomes.
* **Agent Decisions:** Every agent activation writes to `monitoring.incident_audit_trail`:
  * `incident_id`, `dag_run_id`, `state`, `evidence_collected`, `selected_action`, `remediation_job_id`.
* **Traceable Logs:** Remediation actions executed via BigQuery record the resulting BigQuery Job IDs in the audit log, allowing developers to trace the exact physical query executed by the engine.

---

## 21. Trade-offs

During the design phase, several architecture trade-offs were evaluated:

### 21.1 Apache Airflow vs. Custom Cron Scheduler
* *Custom Cron:* Lightweight to set up, requires no server hosting.
* *Airflow (Selected):* Introduces local Docker/server setup complexity, but provides native task lineage, automatic retry policies, credential management, SLA monitoring, and structured log archiving which are essential for a production-grade pipeline.

### 21.2 Google BigQuery vs. Local SQLite Database
* *SQLite:* Extremely simple to test locally, zero hosting cost.
* *BigQuery (Selected):* Requires GCP subscription and credential setup, but models real-world enterprise architectures, provides partitioned tables, has native GCS ingestion connectors, and supports scale-out analytics operations.

### 21.3 Rule-Based Engine vs. LLM Agent
* *Rule-Based:* 100% deterministic and easy to code.
* *LLM Agent (Selected for Diagnosis):* Rule-based alerting tells us *that* something failed, but struggles to read and extract meaning from unstructured task logs or trace complex multi-system failures. The LLM acts as the *diagnosis engine* (generating explainable hypotheses), while a deterministic *policy framework* serves as the executor gate to guarantee safety.

### 21.4 Auto-Remediation vs. Complete Escalation
* *Complete Escalation:* Safest option; no risk of automated corruption. However, it results in high operational overhead for routine, repeating errors.
* *Hybrid (Selected):* Automates only a narrow, whitelisted set of low-risk, idempotent tasks (e.g., deduplication or transient task retries) while routing complex data logic anomalies to humans.

---

## 22. Failure Modes of the Agent Itself

What happens if the monitoring agent itself fails? The system is designed to "fail safe" under all conditions:

* **BigQuery Unavailability:** If the agent cannot access BigQuery schemas during an incident check, it defaults to a state of low-confidence and escalates via the local file system (saving the report directly to the host disk) without attempting remediation.
* **Airflow API Downtime:** If the agent cannot query the Airflow metadata database or trigger a task, it reports an execution block and escalates.
* **Contradictory Evidence:** If BigQuery shows zero duplicate keys but the validation task logs duplicate errors, the agent registers a mismatch, sets confidence to "Low," and escalates.
* **Malformed LLM Recommendation:** If the LLM generates an action string that is not in the hardcoded safety whitelist (e.g., proposing `DROP TABLE`), the deterministic parser rejects the command and raises a high-priority escalation alert.
* **Data Write Blunders:** If a safe auto-remediation (like a deduplication merge) fails or triggers another validation error, the post-check flags the failure, initiates rollback of the partition, and escalates.

---

## 23. Future Improvements

With additional timeline allocations, the system could be enhanced with the following production features:

* **Historical Anomaly Models:** Replace basic static thresholds in `pipeline_config.yaml` with simple statistical machine learning algorithms (e.g., Double Exponential Smoothing) to detect seasonal volume anomalies.
* **Interactive Approval Flow:** Integrate a Slack or MS Teams bot that allows engineers to click "Approve Remediation" directly from their chat window, introducing a human-in-the-loop validation step for medium-risk actions.
* **Dependency Blast-Radius Mapping:** Build a complete graph representation of the warehouse, allowing the agent to automatically flag downstream BI dashboards and views that must be paused when a parent table goes offline.
* **Automated Regression Audits:** Automate evaluation of the Agent's diagnostic accuracy by recording human feedback on the correctness of its hypotheses.

---

## 24. Final Architecture Summary

The proposed **Self-Healing Data Pipeline Agent** blends deterministic data engineering guardrails with modern AI diagnostics. 

```
+----------------------------------------------------------------------------------------------------+
|  [Orchestrator: Airflow]   ===>  [Storage: BigQuery]   ===>  [Validation Tasks: Schema/Data Checks] |
|                                                                                ||                  |
|                                                                                \/                  |
|  [Incident Report Created] <=== [Safety Policy Gate]   <===  [Agentic Diagnosis: Tools/Reasoning]  |
|            ||                                                                                      |
|            \/                                                                                      |
|   /------------------\                                                                             |
|  | Remediation/Escal. |                                                                            |
|   \------------------/                                                                             |
+----------------------------------------------------------------------------------------------------+
```

By ensuring that the agent does not write code, operates under least-privilege permissions, and acts only on concrete evidence, the design establishes a trustworthy system suitable for enterprise operations.
