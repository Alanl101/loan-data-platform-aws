import sys
import logging
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info(f"Job started: {args['JOB_NAME']}")

# Fetch credentials from Secrets Manager
try:
    client = boto3.client('secretsmanager', region_name='us-east-1')
    secret = json.loads(
        client.get_secret_value(
            SecretId='poc/postgres/credentials'
        )['SecretString']
    )
    logger.info("Successfully retrieved credentials from Secrets Manager")
except Exception as e:
    logger.error(f"Failed to retrieve secret: {str(e)}")
    raise

# Build JDBC URL from secret
jdbc_url = f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}"

# Read from PostgreSQL with bookmark
try:
    loans_df = glueContext.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "url": jdbc_url,
            "dbtable": "origination.loans",
            "user": secret['username'],
            "password": secret['password'],
            "jobBookmarkKeys": ["created_at"],
            "jobBookmarkKeysSortOrder": "asc"
        },
        transformation_ctx="loans_postgres_node"
    )
    logger.info(f"Rows read from Postgres: {loans_df.count()}")
except Exception as e:
    logger.error(f"Failed to read from Postgres: {str(e)}")
    raise

# Write to S3 as parquet
try:
    glueContext.write_dynamic_frame.from_options(
        frame=loans_df,
        connection_type="s3",
        format="parquet",
        connection_options={
            "path": "s3://poc1-alan-s3-us-east-1/poc1-bronze-alan-s3-us-east-1/poc1-bronze-loans-alan-s3-us-east-1/"
        },
        transformation_ctx="loans_s3_node"
    )
    logger.info("Write to S3 completed successfully")
except Exception as e:
    logger.error(f"Failed to write to S3: {str(e)}")
    raise

job.commit()
logger.info(f"Job completed successfully: {args['JOB_NAME']}")
