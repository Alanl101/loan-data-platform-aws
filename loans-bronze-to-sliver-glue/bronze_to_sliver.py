import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from bronze layer
loans_df = spark.read.parquet(
    "s3://poc1-alan-s3-us-east-1/poc1-bronze-alan-s3-us-east-1/poc1-bronze-loans-alan-s3-us-east-1/"
)

# Step 1 - Critical Data Quality Checks
total_rows = loans_df.count()
null_loan_ids = loans_df.filter(F.col("loan_id").isNull()).count()
duplicate_loan_ids = total_rows - loans_df.dropDuplicates(["loan_id"]).count()
negative_amounts = loans_df.filter(F.col("loan_amount") < 0).count()

print(f"Total rows: {total_rows}")
print(f"Null loan_ids: {null_loan_ids}")
print(f"Duplicate loan_ids: {duplicate_loan_ids}")
print(f"Negative loan amounts: {negative_amounts}")

# Step 2 - Fail the job if critical checks fail
if null_loan_ids > 0:
    raise Exception(f"Data quality failed: {null_loan_ids} null loan_ids found. Stopping job.")

if duplicate_loan_ids > 0:
    raise Exception(f"Data quality failed: {duplicate_loan_ids} duplicate loan_ids found. Stopping job.")

if negative_amounts > 0:
    raise Exception(f"Data quality failed: {negative_amounts} negative loan amounts found. Stopping job.")

print("Critical data quality checks passed. Proceeding to silver layer.")

# Step 3 - Clean and Transform
silver_df = loans_df \
    # Data Validation Rules - removing invalid records
    .dropDuplicates(["loan_id"]) \
    .filter(F.col("loan_amount") > 0) \
    .filter(F.col("loan_id").isNotNull()) \
    # Type casting, standardization and adding data information
    .withColumn("origination_date", F.to_date(F.col("origination_date"))) \
    .withColumn("loan_amount", F.col("loan_amount").cast("double")) \
    .withColumn("interest_rate", F.col("interest_rate").cast("double")) \
    .withColumn("loan_status", F.lower(F.trim(F.col("loan_status")))) \
    .withColumn("processed_date", F.current_timestamp()) \
    .withColumn("source_system", F.lit("postgresql_ec2")) \
    .withColumn("pipeline_run_id", F.lit(args['JOB_NAME']))

# Step 4 - Write to silver layer
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(silver_df, glueContext, "silver_loans"),
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://poc1-alan-s3-us-east-1/poc1-silver-alan-s3-us-east-1/poc1-silver-loans-alan-s3-us-east-1/",
        "partitionKeys": ["origination_date"]
    }
)

job.commit()
