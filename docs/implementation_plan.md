# Implementation Plan — Phase 2B: Monorepo Restructuring

This document records the design decisions and operations performed to restructure the project into a clean monorepo architecture.

---

## Restructuring Blueprint

```
self-healing-data-pipeline/
│
├── apps/                         # Domain-specific applications
│   ├── frontend/                 # React UI
│   ├── backend/                  # REST API
│   └── agent/                    # Diagnostic AI Agent
│
├── airflow/                      # Airflow Orchestrator
│   ├── dags/                     # starter_dag.py
│   ├── plugins/                  # Custom components
│   ├── config/                   # pipeline_config.yaml
│   └── logs/                     # Task execution logs
│
├── data/                         # Persistent analytical storage
│   ├── raw/                      # raw csv/jsonl datasets
│   ├── processed/                # transformed outputs
│   └── test-fixtures/            # mock datasets for tests
│
├── infrastructure/               # Setup and environment automation
│   ├── docker/                   # service dockerfiles
│   └── gcp/                      # gcloud setup scripts
│
├── docs/                         # Technical docs
│   ├── architecture.md           # component maps
│   ├── design_document.md        # engineering design docs
│   ├── api.md                    # api specs
│   └── runbook.md                # operations guide
│
├── scripts/                      # pipeline utility scripts
│   └── data/                     # generate_data.py
│
├── .env.example
├── .gitignore
└── docker-compose.yml
```

---

## Done Operations

1. **Docker Compose Pathing**:
   - Volume mounts for the webserver and scheduler updated to point to subdirectories under `./airflow/`.
   - `airflow-init` container volume path changed to `./airflow:/sources` to contain system initialization files.
2. **Git Exclusions**:
   - Initialized `git init` repo.
   - Configured `.gitignore` to prevent leaking `gcp-service-account.json`, `.env` files, `node_modules/`, build folders, and Python caches.
3. **Eliminated Data Redundancy**:
   - Moved all raw dataset CSV/JSONL files into `data/raw/`.
   - Cleared duplicate files inside `airflow/data/` to keep storage slim.
4. **Starter Files Integration**:
   - Relocated config to `airflow/config/pipeline_config.yaml`.
   - Relocated DAG code to `airflow/dags/pipeline_dag_starter.py`.
5. **Auxiliary Relocations**:
   - Relocated design documents to `docs/`.
   - Relocated data generator scripts to `scripts/data/generate_data.py`.
   - Relocated setup script to `infrastructure/gcp/setup_intern.sh`.
