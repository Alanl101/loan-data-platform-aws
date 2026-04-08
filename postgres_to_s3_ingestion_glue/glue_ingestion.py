import sys
import boto3
import json
from datetime import datetime, timedelta
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# -- Init 
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info(f"Job started: {args['JOB_NAME']}")
print(f"[OUTPUT] Job started: {args['JOB_NAME']}")

# -- Secrets
try:
    secret = json.loads(
        boto3.client('secretsmanager', region_name='us-east-1')
        .get_secret_value(SecretId='poc/postgres/credentials')['SecretString']
    )
    logger.info("Secrets retrieved successfully")
    print("[OUTPUT] Secrets retrieved successfully")
except Exception as e:
    logger.error(f"Failed to retrieve secret: {e}")
    print(f"[OUTPUT] ERROR - Failed to retrieve secret: {e}")
    raise

jdbc_url = f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}"
jdbc_props = {
    "user":     secret['username'],
    "password": secret['password'],
    "driver":   "org.postgresql.Driver"
}

logger.info(f"JDBC URL built: {jdbc_url}")
print(f"[OUTPUT] JDBC URL built: {jdbc_url}")

# -- 3-Day Window 
now = datetime.utcnow()
window_start = (now - timedelta(days=2)).strftime('%Y-%m-%d')

logger.info(f"Ingesting records updated since: {window_start}")
print(f"[OUTPUT] Ingesting records updated since: {window_start}")

# -- Extract
try:
    loans_df = spark.read.jdbc(
        url=jdbc_url,
        table=f"""(
            SELECT * FROM origination.loans
            WHERE updated_at >= '{window_start}'
        ) AS loans_window""",
        properties=jdbc_props
    )
    row_count = loans_df.count()
    logger.info(f"Rows read from Postgres: {row_count}")
    print(f"[OUTPUT] Rows read from Postgres: {row_count}")
except Exception as e:
    logger.error(f"Failed to read from Postgres: {e}")
    print(f"[OUTPUT] ERROR - Failed to read from Postgres: {e}")
    raise

if row_count == 0:
    logger.info("No records in window. Exiting.")
    print("[OUTPUT] No records in window. Exiting.")
    job.commit()
    sys.exit(0)

# -- Add Partition Columns 
loans_df = loans_df \
    .withColumn("year",  F.lit(now.year)) \
    .withColumn("month", F.lit(now.month)) \
    .withColumn("day",   F.lit(now.day))

# -- Write to Bronze 
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

try:
    loans_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet("s3://poc1-alan-s3-us-east-1/poc1-bronze-alan-s3-us-east-1/loans/")
    logger.info("Write to S3 completed successfully")
    print("[OUTPUT] Write to S3 completed successfully")
except Exception as e:
    logger.error(f"Failed to write to S3, routing to dead-letter: {e}")
    print(f"[OUTPUT] ERROR - Failed to write to S3, routing to dead-letter: {e}")
    loans_df.write.mode("append").parquet(
        f"s3://poc1-alan-s3-us-east-1/poc1-bronze-alan-s3-us-east-1/dead-letter/loans/{now.strftime('%Y/%m/%d')}/"
    )
    job.commit()
    sys.exit(1)

job.commit()
logger.info(f"Job completed successfully: {args['JOB_NAME']}")
print(f"[OUTPUT] Job completed successfully: {args['JOB_NAME']}")
