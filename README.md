# Secure Cloud Data Pipeline on AWS (S3 · Lambda · Glue · Athena · IAM)

Event-driven, serverless pipeline: CSV files land in S3, Lambda validates them and starts a Glue PySpark job,
Glue writes partitioned Parquet and updates the Data Catalog, and analysts query the result with Athena.
Everything is deployed with one CloudFormation template.

![architecture](docs/architecture.png)

## What each service does
| Service | Role |
|---|---|
| **S3** | Data lake with `raw/`, `processed/`, `quarantine/`, `athena-results/` prefixes. SSE-S3 encryption, versioning, Block Public Access, TLS-only policy, lifecycle rules |
| **Lambda** (`lambda/handler.py`) | Triggered by `raw/*.csv`. Checks size, header, row shape. Valid -> starts Glue. Invalid -> moved to `quarantine/` with the reason. Retries (2x) + SQS dead-letter queue |
| **Glue job** (`glue/etl_job.py`) | Parses and validates every row, quarantines bad rows with reasons, de-duplicates to the latest version per order, derives `revenue`, writes Snappy Parquet partitioned by `ingest_date/batch_id` (idempotent per file). Fails the run if >30% rows are bad |
| **Glue Crawler / Data Catalog** | Registers the `sales` table and new partitions |
| **Athena** | Dedicated workgroup: enforced encrypted results location, 1 GB per-query scan limit, saved queries |
| **IAM** | Separate least-privilege roles for Lambda and Glue; an analyst managed policy that can read `processed/` only |
| **CloudWatch / SNS / EventBridge** | Alarm on Lambda errors, rule on Glue job failure/timeout, e-mail alerts |

## Deploy
Prerequisites: AWS account, AWS CLI v2 (`aws configure`), bash (Git Bash / WSL on Windows).
```bash
PROJECT=salesdl ALERT_EMAIL=you@example.com ./scripts/deploy.sh
./scripts/demo.sh        # uploads a good file and a bad-header file
```
Then open **Athena -> workgroup `salesdl-wg` -> Saved queries -> daily-revenue**.
(Allow 1-2 minutes after the Glue job finishes for the crawler to register the partition.)

## Tests (no AWS needed)
```bash
python tests/test_handler.py
```

## Clean up (avoid charges)
```bash
python scripts/cleanup.py salesdl
```

## Layout
```
infra/template.yaml     CloudFormation (S3, IAM, Lambda, Glue, Athena, alerts)
lambda/handler.py       ingestion + validation
glue/etl_job.py         PySpark ETL
athena/queries.sql      example analytics queries
scripts/                deploy.sh, demo.sh, cleanup.py
data/sample/            sample inputs
tests/                  unit tests for the Lambda
```
