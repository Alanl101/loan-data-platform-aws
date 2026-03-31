# loan-data-platform-aws

PostgreSQL (origination.loans on EC2)
   |
   |  AWS Glue JDBC read (incremental via bookmarks or full scan)
   v
Raw Extract (S3 / Bronze Layer - Parquet)



PostgreSQL (OLTP - origination.loans on EC2)
        |
        |  CDC capture (row-level changes)
        |  - inserts / updates / deletes
        |
        v
AWS DMS (CDC replication) OR Debezium (logical decoding)
        |
        |  continuous event stream (JSON change events)
        |
        v
Kafka (MSK) OR Kinesis Data Streams
        |
        |  real-time buffering + ordering
        |
        v
Kinesis Firehose (optional but common in AWS setups)
        |
        |  batching, compression, file rollover
        |
        v
S3 Bronze (Streaming Zone)
- append-only event log
- partitioned by event_date / event_hour
- stored as Parquet or JSON (raw)






S3 - Bronze Layer
   |
   |  partitioned by ingestion_date
   v
Cleaned Dataset (Glue ETL job)
   - type casting
   - null handling
   - schema standardization
   v
Curated Layer (Silver - S3 Parquet)
   |
   |  SCD2 / CDC logic applied (merge + history tracking)
   v
Gold Layer (Analytics-ready tables)
   - loan status history
   - customer payment timelines
   - BI / Athena queries
