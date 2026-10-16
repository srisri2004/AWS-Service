#!/usr/bin/env bash
# Uploads sample files (one good, one with a bad header) and shows the pipeline reacting.
set -euo pipefail
cd "$(dirname "$0")/.."
PROJECT="${PROJECT:-salesdl}"
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-$(aws configure get region)}}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
B="${PROJECT}-data-${ACCOUNT}-${REGION}"

aws s3 cp data/sample/sales_20261015.csv "s3://$B/raw/sales_20261015.csv"
aws s3 cp data/sample/bad_header.csv     "s3://$B/raw/bad_header.csv"
echo "Waiting 90s for Lambda + Glue..."; sleep 90
echo "--- Glue runs"; aws glue get-job-runs --job-name "${PROJECT}-etl-job" --max-items 3 \
  --query 'JobRuns[].[Id,JobRunState,StartedOn]' --output table
echo "--- processed/"; aws s3 ls "s3://$B/processed/sales/" --recursive | head
echo "--- quarantine/"; aws s3 ls "s3://$B/quarantine/" --recursive
echo "Now open Athena > workgroup ${PROJECT}-wg > Saved queries > daily-revenue (give the crawler ~1-2 min after the job)."
