import sys
import json
import boto3
from datetime import datetime, timedelta, date
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame

# Initialize Spark and Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
# Define all possible arguments. Those not provided will raise KeyError if accessed directly.
all_expected_args = [
    'JOB_NAME',
    'BUCKET_NAME',
    'DATABASE_NAME',
    'REDSHIFT_CONNECTION',
    'REDSHIFT_SCHEMA',
    'REDSHIFT_PROCESSED_SCHEMA',
    #'PROCESSING_DATE', # This will be the *current* date for daily runs, or the specific date for single-day backfill
    'start_processing_date', # Optional: for backfill range (format YYYY-MM-DD)
    'end_processing_date',   # Optional: for backfill range (format YYYY-MM-DD)
    'TempDir' # Required for Redshift writes
]

args = getResolvedOptions(sys.argv, all_expected_args)

# Configuration parameters
BUCKET_NAME = args['BUCKET_NAME']
DATABASE_NAME = args['DATABASE_NAME']
REDSHIFT_CONNECTION = args['REDSHIFT_CONNECTION']
REDSHIFT_KPI_SCHEMA = args['REDSHIFT_SCHEMA']
REDSHIFT_PROCESSED_SCHEMA = args['REDSHIFT_PROCESSED_SCHEMA']

# --- AUTOMATIC GENERATION OF RUN_TIMESTAMP ---
# Use UTC for consistency across AWS services
current_utc_time = datetime.utcnow()
# RUN_TIMESTAMP: Format: YYYYMMDD_HHMMSS (for unique job run identification)
RUN_TIMESTAMP = current_utc_time.strftime('%Y%m%d_%H%M%S')
# --- END AUTOMATIC GENERATION ---

# S3 paths configuration
S3_PATHS = {
    'silver_inventory': f's3://{BUCKET_NAME}/processed/inventory/',
    'silver_pos': f's3://{BUCKET_NAME}/processed/pos/',
    'kpi_errors': f's3://{BUCKET_NAME}/errors/kpi/',
    'logs': f's3://{BUCKET_NAME}/logs/kpi/'
}

# Redshift table names
REDSHIFT_KPI_TABLES = {
    'sales_kpi': f'{REDSHIFT_KPI_SCHEMA}.sales_kpi_daily',
    'inventory_kpi': f'{REDSHIFT_KPI_SCHEMA}.inventory_kpi_daily',
    'regional_kpi': f'{REDSHIFT_KPI_SCHEMA}.regional_kpi_daily'
}

# Redshift processed source data table names (for Option 2, these are daily appends/upserts)
REDSHIFT_PROCESSED_SOURCE_TABLES = {
    'inventory': f'{REDSHIFT_PROCESSED_SCHEMA}.inventory_daily',
    'pos': f'{REDSHIFT_PROCESSED_SCHEMA}.pos_daily'
}

# Initialize job
job.init(args['JOB_NAME'], args)

# Initialize S3 client
s3_client = boto3.client('s3')