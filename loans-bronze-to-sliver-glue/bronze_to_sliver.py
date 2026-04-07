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

loans_df.cache()

# Step 1 - Audit raw data (informational only)
total_rows = loans_df.count()
null_loan_ids = loans_df.filter(F.col("loan_id").isNull()).count()
duplicate_loan_ids = total_rows - loans_df.select("loan_id").dropDuplicates().count()
negative_amounts = loans_df.filter(F.col("loan_amount") < 0).count()

print(f"Total rows: {total_rows}")
print(f"Null loan_ids: {null_loan_ids}")
print(f"Duplicate loan_ids: {duplicate_loan_ids} (will be deduplicated in cleaning step)")
print(f"Negative loan amounts: {negative_amounts}")

loans_df.unpersist()

# Step 2 - Clean and Transform
silver_df = (
    loans_df
    # Data Validation Rules - removing invalid records
    .dropDuplicates(["loan_id"])
    .filter(F.col("loan_amount") > 0)
    .filter(F.col("loan_id").isNotNull())
    # Type casting, standardization and adding metadata
    .withColumn("origination_date", F.to_date(F.col("origination_date")))
    .withColumn("loan_amount", F.col("loan_amount").cast("double"))
    .withColumn("interest_rate", F.col("interest_rate").cast("double"))
    .withColumn("loan_status", F.lower(F.trim(F.col("loan_status"))))
    .withColumn("processed_date", F.current_timestamp())
    .withColumn("source_system", F.lit("postgresql_ec2"))
    .withColumn("pipeline_run_id", F.lit(args['JOB_NAME']))
)

silver_df.cache()

# Step 3 - Validate cleaned data, fail if anything still wrong
clean_total = silver_df.count()
clean_duplicates = clean_total - silver_df.select("loan_id").dropDuplicates().count()
clean_nulls = silver_df.filter(F.col("loan_id").isNull()).count()
clean_negatives = silver_df.filter(F.col("loan_amount") < 0).count()

print(f"Clean total rows: {clean_total}")
print(f"Remaining duplicate loan_ids: {clean_duplicates}")
print(f"Remaining null loan_ids: {clean_nulls}")
print(f"Remaining negative loan amounts: {clean_negatives}")

if clean_duplicates > 0:
    raise Exception(f"Data quality failed: {clean_duplicates} duplicate loan_ids remain after cleaning.")
if clean_nulls > 0:
    raise Exception(f"Data quality failed: {clean_nulls} null loan_ids remain after cleaning.")
if clean_negatives > 0:
    raise Exception(f"Data quality failed: {clean_negatives} negative loan amounts remain after cleaning.")

print("Post-clean data quality checks passed. Proceeding to write silver layer.")

silver_df.unpersist()

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
