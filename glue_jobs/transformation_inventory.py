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
from pyspark.sql.window import Window # Import for window function

# Initialize Spark and Glue contexts
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

# S3 paths configuration (only for inventory)
S3_PATHS = {
    'inventory_raw': f's3://{BUCKET_NAME}/raw/batch/inventory-raw/',
    'silver_inventory': f's3://{BUCKET_NAME}/processed/inventory/',
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


def setup_logging():
    """
    Setup structured logging for the job.
    Note: 'processing_date' in log_data will be updated dynamically in main()
          when processing multiple dates. This initial value is a placeholder.
    """
    log_data = {
        'job_name': args['JOB_NAME'],
        'run_timestamp': RUN_TIMESTAMP,
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
        'timestamp': datetime.now().isoformat(), # Use local time for log timestamp for readability in console
        'level': level,
        'message': message
    }
    if details:
        log_entry['details'] = details

    # Add PROCESSING_DATE to each log entry for multi-day runs if it's set globally
    if 'PROCESSING_DATE' in globals():
        log_entry['processing_date'] = globals()['PROCESSING_DATE']
    
    log_data['logs'].append(log_entry)
    print(f"[{level}] {message}") # Also print to console for real-time monitoring

def save_logs_to_s3(log_data):
    """
    Save structured logs to S3

    Args:
        log_data: Dictionary containing all log information
    """
    try:
        log_path = f"{S3_PATHS['logs']}{RUN_TIMESTAMP}/validation_logs.json"
        log_json = json.dumps(log_data, indent=2, default=str) # Use default=str for datetime objects
        
        # Write to S3
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=log_path.replace(f's3://{BUCKET_NAME}/', ''), # Remove s3://bucket-name/ prefix
            Body=log_json,
            ContentType='application/json'
        )
        print(f"Logs saved to: {log_path}")
    except Exception as e:
        print(f"ERROR: Failed to save logs to S3: {str(e)}")

def get_schema_for_source(source_type):
    """
    Define expected schemas for 'inventory' data source based on the provided schema.
    Raises ValueError if source_type is not 'inventory'.

    Args:
        source_type: 'inventory'
        
    Returns:
        Dictionary with schema definition and validation rules
    """
    if source_type == 'inventory':
        return {
            'required_fields': ['inventory_id', 'product_id', 'warehouse_id', 'stock_level', 'last_updated'],
            'schema': StructType([
                StructField('inventory_id', IntegerType(), False),
                StructField('product_id', IntegerType(), False),
                StructField('warehouse_id', IntegerType(), False),
                StructField('stock_level', IntegerType(), False),
                StructField('restock_threshold', IntegerType(), True), # Nullable
                StructField('last_updated', LongType(), False) # Changed to LongType for epoch seconds
            ]),
            'validation_rules': {
                'stock_level': lambda col: col >= 0, # Stock level must be non-negative
                'restock_threshold': lambda col: (col.isNull()) | (col >= 0), # Restock threshold can be null or non-negative
                'last_updated': lambda col: (col.isNotNull()) & (col > 0) # last_updated must not be null and be a positive epoch
            }
        }
    else:
        # This branch should ideally not be reached as only 'inventory' is processed
        raise ValueError(f"Unsupported source type: {source_type}. This job only processes 'inventory' data.")
