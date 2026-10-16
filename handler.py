"""Ingest Lambda: triggered by S3 ObjectCreated on raw/*.csv.

1. Lightweight validation (size, extension, header, sample rows).
2. Valid file  -> start the Glue ETL job for that object.
3. Invalid file -> move to quarantine/ with the reason stored as object metadata.
Unhandled errors propagate so Lambda's async retries / DLQ / alarm take over.
"""
import csv
import io
import json
import logging
import os
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REQUIRED_COLUMNS = [c.strip() for c in os.environ.get(
    "REQUIRED_COLUMNS",
    "order_id,customer_id,product,quantity,unit_price,status,order_date,updated_at").split(",")]
MAX_BYTES = int(os.environ.get("MAX_FILE_BYTES", str(500 * 1024 * 1024)))
SAMPLE_BYTES = 64 * 1024
GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME", "")

_clients = {}


def _client(name):
    if name not in _clients:
        import boto3  # available in the Lambda runtime; imported lazily for unit tests
        _clients[name] = boto3.client(name)
    return _clients[name]


class InvalidFile(Exception):
    """File must be quarantined (not retried)."""


def validate_sample(sample_text, required=None):
    """Check the header and that the first rows have the right number of fields."""
    required = required or REQUIRED_COLUMNS
    rows = list(csv.reader(io.StringIO(sample_text)))
    if not rows:
        raise InvalidFile("file is empty")
    header = [h.strip().lower() for h in rows[0]]
    missing = [c for c in required if c not in header]
    if missing:
        raise InvalidFile(f"missing columns: {missing}")
    if len(rows) < 2:
        raise InvalidFile("file has a header but no data rows")
    # the last sampled row may be cut off by the byte-range read, so skip it
    for i, r in enumerate(rows[1:-1], start=2):
        if r and len(r) != len(header):
            raise InvalidFile(f"row {i} has {len(r)} fields, expected {len(header)}")
    return True


def quarantine(bucket, key, reason):
    s3 = _client("s3")
    dest = "quarantine/" + key.split("/", 1)[-1]
    s3.copy_object(Bucket=bucket, Key=dest, CopySource={"Bucket": bucket, "Key": key},
                   Metadata={"quarantine-reason": reason[:200]},
                   MetadataDirective="REPLACE", ServerSideEncryption="AES256")
    s3.delete_object(Bucket=bucket, Key=key)
    logger.warning("Quarantined s3://%s/%s -> %s (%s)", bucket, key, dest, reason)
    return dest


def process_object(bucket, key):
    if not key.startswith("raw/") or not key.lower().endswith(".csv"):
        logger.info("Ignoring %s", key)
        return {"key": key, "action": "ignored"}
    s3 = _client("s3")
    head = s3.head_object(Bucket=bucket, Key=key)
    try:
        size = head["ContentLength"]
        if size == 0:
            raise InvalidFile("file is empty")
        if size > MAX_BYTES:
            raise InvalidFile(f"file too large ({size} bytes)")
        body = s3.get_object(Bucket=bucket, Key=key,
                             Range=f"bytes=0-{SAMPLE_BYTES - 1}")["Body"].read()
        validate_sample(body.decode("utf-8-sig", errors="replace"))
    except InvalidFile as exc:
        return {"key": key, "action": "quarantined", "reason": str(exc),
                "moved_to": quarantine(bucket, key, str(exc))}
    run = _client("glue").start_job_run(
        JobName=GLUE_JOB_NAME, Arguments={"--SOURCE_PATH": f"s3://{bucket}/{key}"})
    logger.info("Started Glue run %s for s3://%s/%s", run["JobRunId"], bucket, key)
    return {"key": key, "action": "glue_started", "job_run_id": run["JobRunId"]}


def lambda_handler(event, context=None):
    results = []
    for rec in event.get("Records", []):
        bucket = rec["s3"]["bucket"]["name"]
        key = unquote_plus(rec["s3"]["object"]["key"])
        results.append(process_object(bucket, key))
    logger.info(json.dumps(results))
    return results
