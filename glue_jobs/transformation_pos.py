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

    def get_pos_schema():
    """
    Define expected schema and validation rules for 'pos' data.
        
    Returns:
        Dictionary with schema definition and validation rules
    """
    return {
        'required_fields': ['transaction_id', 'store_id', 'product_id', 'quantity', 'revenue', 'timestamp'],
        'schema': StructType([
            StructField('transaction_id', StringType(), False),
            StructField('store_id', IntegerType(), False),
            StructField('product_id', IntegerType(), False),
            StructField('quantity', IntegerType(), False),
            StructField('revenue', FloatType(), False),
            StructField('discount_applied', FloatType(), True), # Nullable
            StructField('timestamp', LongType(), False) # Changed to LongType for epoch seconds
        ]),
        'validation_rules': {
            'quantity': lambda col: col >= 0, # Quantity must be non-negative
            'revenue': lambda col: col >= 0, # Revenue must be non-negative
            'timestamp': lambda col: (col.isNotNull()) & (col > 0) # timestamp must not be null and be a positive epoch
        },
        'deduplication_keys': ['transaction_id'], # Key for deduplication
        'deduplication_order_by': 'timestamp' # Column to order by for deduplication
    }

def read_pos_data_from_catalog(log_data):
    """
    Read POS data from Glue Data Catalog.

    Args:
        log_data: Logging data structure
        
    Returns:
        Spark DataFrame or None if failed
    """
    try:
        table_name = "pos_raw"
        
        log_message(log_data, "INFO", f"Reading POS data from catalog table: {table_name}")
        
        # Create dynamic frame from catalog
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=DATABASE_NAME,
            table_name=table_name,
            transformation_ctx=f"read_pos"
        )
        
        # Convert to Spark DataFrame
        df = dynamic_frame.toDF()
        
        # Filter for current processing date partitions if the raw data is partitioned by 'partition_date'
        if 'partition_date' in df.columns:
            log_message(log_data, "INFO", f"Filtering POS data for partition_date = {PROCESSING_DATE}")
            df = df.filter(col('partition_date') == PROCESSING_DATE)
        else:
            log_message(log_data, "WARN", f"Table {table_name} does not have 'partition_date' column. Reading all data available via catalog.")
        
        row_count = df.count()
        log_message(log_data, "INFO", f"Successfully read {row_count} rows from POS table")
        
        return df
        
    except Exception as e:
        log_message(log_data, "ERROR", f"Failed to read POS data from catalog", str(e))
        return None