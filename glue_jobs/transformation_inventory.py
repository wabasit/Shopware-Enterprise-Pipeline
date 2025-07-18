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