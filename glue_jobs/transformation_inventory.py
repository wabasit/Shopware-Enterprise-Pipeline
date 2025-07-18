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

def read_data_from_catalog(source_type, log_data):
    """
    Read data from Glue Data Catalog for the 'inventory' source.

    Args:
        source_type: 'inventory'
        log_data: Logging data structure
        
    Returns:
        Spark DataFrame or None if failed
    """
    try:
        table_name = f"{source_type}_raw" # This will be 'inventory_raw'
        
        log_message(log_data, "INFO", f"Reading {source_type} data from catalog table: {table_name}")
        
        # Create dynamic frame from catalog
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=DATABASE_NAME,
            table_name=table_name,
            transformation_ctx=f"read_{source_type}"
        )
        
        # Convert to Spark DataFrame
        df = dynamic_frame.toDF()
        
        # Filter for current processing date partitions if the raw data is partitioned by 'partition_date'
        # Important: This assumes raw data is partitioned. If not, the 'archive_processed_files'
        # logic will still attempt to filter by filename date for archival.
        if 'partition_date' in df.columns:
            log_message(log_data, "INFO", f"Filtering {source_type} data for partition_date = {PROCESSING_DATE}")
            df = df.filter(col('partition_date') == PROCESSING_DATE)
        else:
            log_message(log_data, "WARN", f"Table {table_name} does not have 'partition_date' column. Reading all data.")
        
        row_count = df.count()
        log_message(log_data, "INFO", f"Successfully read {row_count} rows from {source_type} table")
        
        return df
        
    except Exception as e:
        log_message(log_data, "ERROR", f"Failed to read {source_type} data from catalog", str(e))
        return None

def validate_data(df, source_type, log_data):
    """
    Validate data against defined schema and rules for 'inventory' data.

    Args:
        df: Spark DataFrame
        source_type: 'inventory'
        log_data: Logging data structure
        
    Returns:
        Tuple of (valid_df, invalid_df, validation_summary)
    """
    try:
        schema_config = get_schema_for_source(source_type) # This will get inventory schema
        required_fields = schema_config['required_fields']
        validation_rules = schema_config['validation_rules']
        expected_schema = schema_config['schema']
        
        log_message(log_data, "INFO", f"Starting validation for {source_type} data")
        
        # 1. Check for required fields existence in the DataFrame
        # Important: Check against the original DF before casting, as casting might drop columns
        missing_fields = [field for field in required_fields if field not in df.columns]
        if missing_fields:
            error_msg = f"Missing critical required fields in input DataFrame: {missing_fields}"
            log_message(log_data, "ERROR", error_msg)
            # If critical fields are missing, the entire DF is considered unprocessable
            return None, df.withColumn('error_reason', lit(error_msg)), {'status': 'failed', 'reason': error_msg}
        
        # 2. Cast to expected schema (this handles type mismatches by turning them into nulls if incompatible)
        # It's important to select and reorder columns to match the target schema structure
        selected_cols = [col(field.name).cast(field.dataType).alias(field.name) for field in expected_schema.fields]
        df_casted = df.select(*selected_cols)
        
        # 3. Build validation conditions and error reasons
        invalid_conditions = []
        error_reason_expr = lit(None) # Default to None, will be set for invalid rows
        
        # Check for null values in required fields *after* casting
        for field in required_fields:
            if field in df_casted.columns: # Ensure field exists after casting/selection
                null_check = col(field).isNull()
                invalid_conditions.append(null_check)
                error_reason_expr = when(null_check, f"Null in required field: {field}").otherwise(error_reason_expr)
        
        # Apply validation rules
        for field, rule in validation_rules.items():
            if field in df_casted.columns: # Ensure field exists after casting/selection
                rule_violation_check = ~rule(col(field))
                invalid_conditions.append(rule_violation_check)
                error_reason_expr = when(rule_violation_check, f"Rule violation for {field}").otherwise(error_reason_expr)

        # Combine all invalid conditions using OR
        combined_invalid_condition = lit(False)
        if invalid_conditions:
            for condition in invalid_conditions:
                combined_invalid_condition = combined_invalid_condition | condition

        df_with_validation = df_casted.withColumn('is_valid', ~combined_invalid_condition) \
                                     .withColumn('error_reason', error_reason_expr) # This column will have reason for invalid rows
        
        # Split into valid and invalid DataFrames
        valid_df = df_with_validation.filter(col('is_valid') == True).drop('error_reason', 'is_valid')
        invalid_df = df_with_validation.filter(col('is_valid') == False).drop('is_valid')
        
        # Get validation summary
        total_rows = df.count() # Use count from original DF for total
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        
        validation_summary = {
            'total_rows': total_rows,
            'valid_rows': valid_count,
            'invalid_rows': invalid_count,
            'validation_rate': (valid_count / total_rows) * 100 if total_rows > 0 else 0,
            'status': 'success' if invalid_count == 0 else 'partial_success'
        }
        
        log_message(log_data, "INFO", f"Validation completed for {source_type}", validation_summary)
        
        return valid_df, invalid_df, validation_summary
        
    except Exception as e:
        log_message(log_data, "ERROR", f"Validation failed for {source_type}", str(e))
        # Return original DF with a generic error reason if validation setup itself fails
        return None, df.withColumn('error_reason', lit(f"Validation setup error: {str(e)}")), {'status': 'error', 'reason': str(e)}
