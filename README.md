# Self-Healing Data Pipeline

A containerized, resilient data ingestion and diagnostics pipeline demonstrating self-healing behaviors for common data quality and operational issues.

## Project Structure

```
├── apps/                        # Application components
│   ├── frontend/                # React dashboard (Planned)
│   ├── backend/                 # API server (Planned)
│   └── agent/                   # AI diagnostics agent (Planned)
├── airflow/                     # Apache Airflow orchestrator (LocalExecutor)
│   ├── dags/                    # DAG definitions (pipeline_dag_starter.py)
│   ├── config/                  # Pipeline schemas and metadata (pipeline_config.yaml)
│   ├── logs/                    # Execution logs
│   └── plugins/                 # Custom Airflow operators/hooks
├── data/                        # Persistent analytical storage
│   ├── raw/                     # Raw daily ingestion CSV/JSONL datasets
│   ├── processed/               # Cleansed analytical data output
│   └── test-fixtures/           # Mock data inputs for fault injection testing
├── docs/                        # Specifications and design documentation
│   ├── design_document.md       # Technical specs & recovery policies
│   ├── architecture.md          # System topology & domain details
│   ├── api.md                   # Backend REST endpoints spec
│   └── runbook.md               # Operations & troubleshooting handbook
├── infrastructure/              # Infrastructure-as-code
│   ├── docker/                  # Custom service Dockerfiles
│   └── gcp/                     # GCP resource setup scripts
├── scripts/                     # Local developer scripts
│   ├── data/                    # Dataset helpers (generate_data.py)
│   └── setup/                   # Host environment setup
└── tests/                       # Scoped test suites
    ├── integration/             # GCP/BigQuery integration tests
    ├── e2e/                     # End-to-end user flow tests
    └── fault-injection/         # Diagnostics & remediation tests
```

## Getting Started

1. Set up your environment variables by copying `.env.example`:
   ```bash
   cp .env.example .env
   ```
2. Provision GCP resources using the script in `infrastructure/gcp/setup_intern.sh`.
3. Save your service account credentials key file to `airflow/config/gcp-service-account.json`.
4. Start the local Airflow orchestrator:
   ```bash
   docker compose up airflow-init
   docker compose up -d
   ```
5. Open the Airflow Webserver UI at [http://localhost:8080](http://localhost:8080) (Default user/password: `airflow`/`airflow`).
