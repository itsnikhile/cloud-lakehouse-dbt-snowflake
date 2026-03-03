# Cloud Lakehouse — dbt + Snowflake + Airflow

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)
![Status](https://img.shields.io/badge/Status-Production--Ready-brightgreen?style=flat-square)
![License](https://img.shields.io/badge/License-MIT-blue?style=flat-square)

> S3 → Snowpipe → Bronze/Silver/Gold → BI Tools

## Architecture

![Architecture Diagram](./architecture.svg)

## Project Structure

```
cloud-lakehouse-dbt-snowflake/
    ├── .env.example
    ├── .gitignore
    ├── Makefile
    ├── main.py
    ├── requirements.txt
    ├── airflow/
        ├── dags/
            ├── lakehouse_dag.py
    ├── config/
        ├── config.yaml
    ├── dbt_project/
        ├── macros/
        ├── models/
            ├── marts/
                ├── __init__.py
                ├── dim_users.sql
                ├── fct_daily_revenue.sql
            ├── staging/
                ├── __init__.py
                ├── stg_events.sql
                ├── stg_transactions.sql
        ├── tests/
    ├── docker/
    ├── docs/
    ├── src/
        ├── __init__.py
        ├── ingestion/
            ├── __init__.py
            ├── schema_manager.py
```

## Quick Start

```bash
# 1. Clone
git clone https://github.com/itsnikhile/cloud-lakehouse-dbt-snowflake
cd cloud-lakehouse-dbt-snowflake

# 2. Install
pip install -r requirements.txt

# 3. Configure
cp .env.example .env
# Edit .env with your credentials

# 4. Run demo (no external services needed)
python main.py demo
```

## Local Development with Docker

```bash
# Start all infrastructure (Kafka, Redis, etc.)
docker-compose up -d

# Run the full pipeline
make run

# Run tests
make test
```

## Running Tests

```bash
pytest tests/ -v --cov=src --cov-report=term-missing
```

## Configuration

All config is in `config/config.yaml`. Override with environment variables.
Copy `.env.example` to `.env` and fill in your credentials.

## Key Features

- ✅ Production-grade error handling and retry logic
- ✅ Comprehensive test suite with mocks
- ✅ Docker Compose for local development
- ✅ Makefile for common commands
- ✅ Structured logging with metrics
- ✅ CI/CD ready (GitHub Actions workflow)

---

> Built by [Nikhil E](https://github.com/itsnikhile) — Senior Data Engineer
