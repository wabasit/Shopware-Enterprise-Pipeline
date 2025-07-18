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
    
    def validate_pos_data(df, log_data):
    """
    Validate POS data against defined schema and rules.

    Args:
        df: Spark DataFrame
        log_data: Logging data structure
        
    Returns:
        Tuple of (valid_df, invalid_df, validation_summary)
    """
    try:
        schema_config = get_pos_schema()
        required_fields = schema_config['required_fields']
        validation_rules = schema_config['validation_rules']
        expected_schema = schema_config['schema']
        
        log_message(log_data, "INFO", f"Starting validation for POS data")
        
        # 1. Check for required fields existence in the DataFrame
        missing_fields = [field for field in required_fields if field not in df.columns]
        if missing_fields:
            error_msg = f"Missing critical required fields in input DataFrame: {missing_fields}"
            log_message(log_data, "ERROR", error_msg)
            # If critical fields are missing, the entire DF is considered unprocessable
            return None, df.withColumn('error_reason', lit(error_msg)), {'status': 'failed', 'reason': error_msg}
        
        # 2. Cast to expected schema (this handles type mismatches by turning them into nulls if incompatible)
        selected_cols = [col(field.name).cast(field.dataType).alias(field.name) for field in expected_schema.fields if field.name in df.columns]
        
        # Add columns from expected_schema that are not in df but are nullable
        for field in expected_schema.fields:
            if field.name not in df.columns and field.nullable:
                selected_cols.append(lit(None).cast(field.dataType).alias(field.name))
        
        df_casted = df.select(*selected_cols)
        
        # 3. Build validation conditions and error reasons
        invalid_conditions = []
        error_reason_expr = lit(None) # Default to None, will be set for invalid rows
        
        # Check for null values in required fields *after* casting
        for field in required_fields:
            if field in df_casted.columns:
                null_check = col(field).isNull()
                invalid_conditions.append(null_check)
                error_reason_expr = when(null_check, f"Null in required field: {field}").otherwise(error_reason_expr)
        
        # Apply validation rules
        for field, rule in validation_rules.items():
            if field in df_casted.columns:
                rule_violation_check = ~rule(col(field))
                invalid_conditions.append(rule_violation_check)
                error_reason_expr = when(rule_violation_check, f"Rule violation for {field}").otherwise(error_reason_expr)

        # Combine all invalid conditions using OR
        combined_invalid_condition = lit(False)
        if invalid_conditions:
            for condition in invalid_conditions:
                combined_invalid_condition = combined_invalid_condition | condition

        df_with_validation = df_casted.withColumn('is_valid', ~combined_invalid_condition) \
                                     .withColumn('error_reason', error_reason_expr)
        
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
        
        log_message(log_data, "INFO", f"Validation completed for POS data", validation_summary)
        
        return valid_df, invalid_df, validation_summary
        
    except Exception as e:
        log_message(log_data, "ERROR", f"Validation failed for POS data", str(e))
        # Return original DF with a generic error reason if validation setup itself fails
        return None, df.withColumn('error_reason', lit(f"Validation setup error: {str(e)}")), {'status': 'error', 'reason': str(e)}
    
    def transform_pos_data(df, log_data):
    """
    Apply transformations to validated POS data.
    
    Args:
        df: Spark DataFrame with valid data
        log_data: Logging data structure
        
    Returns:
        Transformed DataFrame
    """
    try:
        log_message(log_data, "INFO", f"Starting transformation for POS data")
        
        df_transformed = df
        
        # Convert epoch timestamp to TimestampType
        timestamp_col_name = get_pos_schema()['deduplication_order_by']
        df_transformed = df_transformed.withColumn(f'{timestamp_col_name}_timestamp',
                                                    from_unixtime(col(timestamp_col_name)).cast(TimestampType()))
        
        # Add derived columns specific to POS
        df_transformed = df_transformed \
            .withColumn('sale_category',
                        when(col('revenue') >= 100, 'HIGH_VALUE')
                        .when((col('revenue') >= 50) & (col('revenue') < 100), 'MEDIUM_VALUE')
                        .otherwise('LOW_VALUE'))
        
        # Add processing_date for partitioning
        df_transformed = df_transformed.withColumn('processing_date', lit(PROCESSING_DATE))
        
        # Add audit columns (using UTC for consistency)
        df_transformed = df_transformed \
            .withColumn('processed_timestamp_utc', lit(datetime.utcnow())) \
            .withColumn('job_run_id', lit(RUN_TIMESTAMP))
        
        row_count = df_transformed.count()
        log_message(log_data, "INFO", f"Transformation completed for POS data: {row_count} rows")
        
        return df_transformed
        
    except Exception as e:
        log_message(log_data, "ERROR", f"Transformation failed for POS data", str(e))
        return None
    
    def deduplicate_pos_data(df, log_data):
    """
    De-duplicates the POS data, keeping the latest record based on transaction_id and timestamp.

    Args:
        df: Spark DataFrame with transformed data.
        log_data: Logging data structure

    Returns:
        Deduplicated Spark DataFrame.
    """
    try:
        schema_config = get_pos_schema()
        deduplication_keys = schema_config['deduplication_keys']
        deduplication_order_by = schema_config['deduplication_order_by'] + '_timestamp' # Use the transformed timestamp column

        log_message(log_data, "INFO", f"Starting de-duplication for POS data based on '{deduplication_keys}' and '{deduplication_order_by}'.")

        # Define window specification: partition by deduplication_keys, order by deduplication_order_by descending
        window_spec = Window.partitionBy(*deduplication_keys).orderBy(col(deduplication_order_by).desc())

        # Apply window function to assign row numbers, then filter to keep only the latest record
        deduplicated_df = df.withColumn("rn", row_number().over(window_spec)) \
                            .filter(col("rn") == 1) \
                            .drop("rn")

        original_count = df.count()
        deduplicated_count = deduplicated_df.count()

        if original_count > deduplicated_count:
            log_message(log_data, "INFO", f"De-duplication completed: {original_count - deduplicated_count} duplicates removed. Remaining records: {deduplicated_count}")
        else:
            log_message(log_data, "INFO", f"De-duplication completed: No duplicates found.")
        
        return deduplicated_df

    except Exception as e:
        log_message(log_data, "ERROR", f"De-duplication failed for POS data", str(e))
        raise

def write_pos_to_silver(df, log_data):
    """
    Write transformed POS data to Silver layer.
    Uses 'append' mode to add new data to existing partitions.
    
    Args:
        df: Transformed and deduplicated DataFrame
        log_data: Logging data structure
    """
    try:
        output_path = S3_PATHS['silver_pos']
        
        log_message(log_data, "INFO", f"Writing POS data to Silver layer base path: {output_path}")
        
        df.write \
          .mode('append') \
          .option('path', output_path) \
          .partitionBy('processing_date') \
          .format('parquet') \
          .save()
        
        log_message(log_data, "INFO", f"Successfully wrote POS data to Silver layer for PROCESSING_DATE={PROCESSING_DATE}")
        
    except Exception as e:
        log_message(log_data, "ERROR", f"Failed to write POS data to Silver layer", str(e))
        raise

def write_pos_errors_to_s3(df, log_data):
    """
    Write invalid POS rows to errors location.
    
    Args:
        df: DataFrame with invalid rows (should contain 'error_reason' column)
        log_data: Logging data structure
    """
    try:
        if df.count() == 0:
            log_message(log_data, "INFO", f"No POS errors to write.")
            return
        
        # Errors are stored under pos/run_timestamp for traceability
        error_path = f"{S3_PATHS['validation_errors']}pos/{RUN_TIMESTAMP}/"
        
        log_message(log_data, "INFO", f"Writing {df.count()} error records for POS data to: {error_path}")
        
        # Add metadata columns for error analysis (using UTC for consistency)
        df_with_metadata = df \
            .withColumn('error_timestamp_utc', lit(datetime.utcnow())) \
            .withColumn('job_run_id', lit(RUN_TIMESTAMP)) \
            .withColumn('processing_date_of_error', lit(PROCESSING_DATE)) \
            .withColumn('source_type', lit("pos"))
        
        # Write as JSON for easier error analysis and readability
        df_with_metadata.write \
            .mode('overwrite') \
            .option('path', error_path) \
            .format('json') \
            .save()
        
        log_message(log_data, "INFO", f"Successfully wrote POS error records.")
        
    except Exception as e:
        log_message(log_data, "ERROR", f"Failed to write POS error records", str(e))

def archive_processed_pos_files(log_data):
    """
    Move processed POS files from raw to archive location for the current processing date.
    Assumes raw files contain the date in YYYYMMDD format in their filename and are CSV.
    
    Args:
        log_data: Logging data structure
    """
    try:
        log_message(log_data, "INFO", f"Starting archival process for POS data for date {PROCESSING_DATE}")
        
        source_prefix = f"raw/batch/pos-raw/" # Raw path for POS
        archive_prefix = f"archive/pos/{PROCESSING_DATE}/" # Archive path for POS
        
        # Convert PROCESSING_DATE to YYYYMMDD for filename matching
        processing_date_yyyymmdd = PROCESSING_DATE.replace('-', '')
        
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=source_prefix)
        
        archived_count = 0
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    source_key = obj['Key'] # e.g., 'raw/batch/pos-raw/data_20240715_100000.csv'
                    
                    # Ensure it's a CSV file and not a directory or other type
                    if source_key.endswith('/') or not source_key.lower().endswith('.csv'):
                        continue
                    
                    filename = source_key.split('/')[-1]
                    
                    # Heuristic to find YYYYMMDD in filename to match with PROCESSING_DATE
                    file_date_match = re.search(r'(\d{8})', filename)
                    
                    if not file_date_match:
                        log_message(log_data, "INFO", f"Skipping {filename}: No YYYYMMDD date found in filename for archival.")
                        continue
                    
                    file_date_yyyymmdd = file_date_match.group(1)
                    
                    if file_date_yyyymmdd == processing_date_yyyymmdd:
                        # Move to archive
                        archive_key = archive_prefix + filename
                        
                        log_message(log_data, "INFO", f"Archiving {source_key} to {archive_key}")
                        s3_client.copy_object(
                            Bucket=BUCKET_NAME,
                            CopySource={'Bucket': BUCKET_NAME, 'Key': source_key},
                            Key=archive_key
                        )
                        
                        s3_client.delete_object(Bucket=BUCKET_NAME, Key=source_key)
                        archived_count += 1
                    else:
                        log_message(log_data, "INFO", f"Skipping {filename}: File date {file_date_yyyymmdd} does not match PROCESSING_DATE {processing_date_yyyymmdd}.")
        
        log_message(log_data, "INFO", f"Archived {archived_count} files for POS data for date {PROCESSING_DATE}")
        
    except Exception as e:
        log_message(log_data, "ERROR", f"Archival failed for POS data", str(e))
        # Do not raise here, as archival is often a best-effort operation and shouldn't fail the whole job.
