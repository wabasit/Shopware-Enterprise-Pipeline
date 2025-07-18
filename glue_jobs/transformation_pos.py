# Import libraries: AWS, Spark, Glue, and utility modules
import sys
import json
import boto3
from datetime import datetime, timedelta, date
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import re
from pyspark.sql.window import Window #

# Parse job arguments, initialize configuration constants and paths
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BUCKET_NAME',
    'DATABASE_NAME',
    'start_processing_date', # Optional: for backfill range (format YYYY-MM-DD)
    'end_processing_date'    # Optional: for backfill range (format YYYY-MM-DD)
])

# Configuration parameters
BUCKET_NAME = args['BUCKET_NAME']
DATABASE_NAME = args['DATABASE_NAME']

# --- AUTOMATIC GENERATION OF RUN_TIMESTAMP ---
# Use UTC for consistency across AWS services
current_utc_time = datetime.utcnow()
# RUN_TIMESTAMP: Format: YYYYMMDD_HHMMSS (for unique job run identification)
RUN_TIMESTAMP = current_utc_time.strftime('%Y%m%d_%H%M%S')
# --- END AUTOMATIC GENERATION ---

# S3 paths configuration (solely for POS)
S3_PATHS = {
    'pos_raw': f's3://{BUCKET_NAME}/raw/batch/pos-raw/', # For POS raw data
    'silver_pos': f's3://{BUCKET_NAME}/processed/pos/', # For POS processed data
    'validation_errors': f's3://{BUCKET_NAME}/errors/validation/', # Generic path for errors
    'archive': f's3://{BUCKET_NAME}/archive/', # Generic path for archive
    'logs': f's3://{BUCKET_NAME}/logs/validation/' # Generic path for logs
}

# Initialize job
job.init(args['JOB_NAME'], args)

# Initialize S3 client for archival operations
s3_client = boto3.client('s3')

# PROCESSING_DATE is now a global variable that will be set dynamically in main()
# It needs an initial placeholder value for `setup_logging` when it's first called,
# but it will be updated per iteration in `main()`.
PROCESSING_DATE = current_utc_time.strftime('%Y-%m-%d')