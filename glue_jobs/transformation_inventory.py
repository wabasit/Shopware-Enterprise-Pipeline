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

def setup_logging():
    """
    Setup structured logging for the job
    """
    log_data = {
        'job_name': args['JOB_NAME'],
        'run_timestamp': RUN_TIMESTAMP,
        'processing_date': args.get('PROCESSING_DATE'), # Initial processing_date for the run
        'logs': []
    }
    return log_data

def log_message(log_data, level, message, details=None):
    """
    Add structured log message to log data
    
    Args:
        log_data: Dictionary containing log information
        level: Log level (INFO, WARN, ERROR)
        message: Log message
        details: Optional additional details
    """
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'level': level,
        'message': message
    }
    if details:
        log_entry['details'] = details
    
    log_data['logs'].append(log_entry)
    print(f"[{level}] {message}")

def save_logs_to_s3(log_data):
    """
    Save structured logs to S3
    
    Args:
        log_data: Dictionary containing all log information
    """
    try:
        log_path = f"{S3_PATHS['logs']}{RUN_TIMESTAMP}/kpi_logs.json"
        log_json = json.dumps(log_data, indent=2)
        
        # Write to S3
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=log_path.replace(f's3://{BUCKET_NAME}/', ''),
            Body=log_json,
            ContentType='application/json'
        )
        print(f"Logs saved to: {log_path}")
    except Exception as e:
        print(f"ERROR: Failed to save logs to S3: {str(e)}")

def read_silver_data(source_type, current_processing_date_str, log_data):
    """
    Read data for a specific processing_date from Silver layer.
    
    Args:
        source_type: 'inventory' or 'pos'
        current_processing_date_str: The specific date string (YYYY-MM-DD) to read for.
        log_data: Logging data structure
        
    Returns:
        Spark DataFrame or None if failed
    """
    try:
        if source_type == 'inventory':
            silver_path = S3_PATHS['silver_inventory']
        elif source_type == 'pos':
            silver_path = S3_PATHS['silver_pos']
        else:
            raise ValueError(f"Unsupported source_type: {source_type}")

        log_message(log_data, "INFO", f"Reading {source_type} data for PROCESSING_DATE={current_processing_date_str} from Silver layer: {silver_path}")
        
        # Read only the partition for the current_processing_date_str
        # Assumes S3 path is partitioned by processing_date like: s3://bucket/silver/inventory/processing_date=YYYY-MM-DD/
        df = spark.read.parquet(f"{silver_path}processing_date={current_processing_date_str}/")
        
        row_count = df.count()
        log_message(log_data, "INFO", f"Successfully read {row_count} rows from {source_type} Silver layer for processing_date={current_processing_date_str}")
        
        return df
        
    except Exception as e:
        log_message(log_data, "ERROR", f"Failed to read {source_type} Silver data for {current_processing_date_str}", str(e))
        return None
