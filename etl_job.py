"""AWS Glue (PySpark) ETL: raw CSV -> validated, de-duplicated, partitioned Parquet.

Job arguments: SOURCE_PATH (runtime), TARGET_PATH, QUARANTINE_PATH, CRAWLER_NAME
"""
import re
import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F

REJECT_THRESHOLD = 0.30
VALID_STATUS = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]

args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_PATH", "TARGET_PATH",
                                     "QUARANTINE_PATH", "CRAWLER_NAME"])
sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

source = args["SOURCE_PATH"]
batch_id = re.sub(r"[^A-Za-z0-9_-]", "_", source.rsplit("/", 1)[-1].rsplit(".", 1)[0])
ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# 1. Extract: read everything as string so bad values never break the read
raw = (spark.read.option("header", "true").option("mode", "PERMISSIVE")
       .option("multiLine", "false").csv(source))
raw = raw.toDF(*[c.strip().lower() for c in raw.columns])
total = raw.count()
if total == 0:
    raise ValueError(f"No rows in {source}")

# 2. Validate: a typed column is NULL when its value cannot be parsed
both = raw.select(
    "*",
    F.trim("order_id").cast("long").alias("t_order_id"),
    F.trim("customer_id").alias("t_customer_id"),
    F.trim("product").alias("t_product"),
    F.trim("quantity").cast("int").alias("t_quantity"),
    F.trim("unit_price").cast("double").alias("t_unit_price"),
    F.upper(F.trim("status")).alias("t_status"),
    F.to_date(F.trim("order_date"), "yyyy-MM-dd").alias("t_order_date"),
    F.to_timestamp(F.trim("updated_at")).alias("t_updated_at"))

reason = F.concat_ws("; ",
    F.when(F.col("t_order_id").isNull(), F.lit("invalid order_id")),
    F.when(F.col("t_customer_id").isNull() | (F.col("t_customer_id") == ""), F.lit("missing customer_id")),
    F.when(F.col("t_quantity").isNull() | (F.col("t_quantity") <= 0), F.lit("invalid quantity")),
    F.when(F.col("t_unit_price").isNull() | (F.col("t_unit_price") < 0), F.lit("invalid unit_price")),
    F.when(F.col("t_order_date").isNull(), F.lit("invalid order_date")),
    F.when(F.col("t_updated_at").isNull(), F.lit("invalid updated_at")),
    F.when(~F.col("t_status").isin(VALID_STATUS), F.lit("invalid status")))
both = both.withColumn("reject_reason", reason).cache()

bad = both.filter(F.col("reject_reason") != "")
good = both.filter(F.col("reject_reason") == "")
rejected = bad.count()

# 3. Quarantine rejected rows (original values + reason) for investigation
if rejected:
    (bad.select(*raw.columns, "reject_reason").coalesce(1)
        .write.mode("overwrite").option("header", "true")
        .csv(f"{args['QUARANTINE_PATH'].rstrip('/')}/{ingest_date}/{batch_id}/"))

if rejected / total > REJECT_THRESHOLD:
    raise ValueError(f"Reject ratio {rejected}/{total} exceeds {REJECT_THRESHOLD:.0%}")

# 4. Transform: latest version per order, derive revenue, drop cancelled for analytics
w = Window.partitionBy("t_order_id").orderBy(F.col("t_updated_at").desc())
clean = (good.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
         .select(F.col("t_order_id").alias("order_id"),
                 F.col("t_customer_id").alias("customer_id"),
                 F.col("t_product").alias("product"),
                 F.col("t_quantity").alias("quantity"),
                 F.col("t_unit_price").alias("unit_price"),
                 F.col("t_status").alias("status"),
                 F.col("t_order_date").alias("order_date"),
                 F.col("t_updated_at").alias("updated_at"),
                 F.round(F.col("t_quantity") * F.col("t_unit_price"), 2).alias("revenue")))
loaded = clean.count()

# 5. Load: Parquet (columnar, compressed), one folder per source file => idempotent re-runs
target = f"{args['TARGET_PATH'].rstrip('/')}/ingest_date={ingest_date}/batch_id={batch_id}/"
clean.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(target)
print(f"AUDIT source={source} rows_in={total} rejected={rejected} loaded={loaded} target={target}")

# 6. Refresh the Glue Data Catalog so Athena sees the new partition
glue_client = boto3.client("glue")
try:
    glue_client.start_crawler(Name=args["CRAWLER_NAME"])
except glue_client.exceptions.CrawlerRunningException:
    print("Crawler already running; it will pick up the new partition")

job.commit()
