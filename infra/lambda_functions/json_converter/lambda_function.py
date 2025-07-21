
import json
import boto3
import logging
import os
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Determine if this is an S3 event or Step Functions invocation
        if 'Records' in event:
            # Direct S3 trigger
            return handle_s3_event(event)
        else:
            # Step Functions invocation
            return handle_step_functions_event(event)
            
    except Exception as e:
        logger.error(f"Lambda failed: {e}")
        raise

def handle_s3_event(event):
    """Handle direct S3 event triggers"""
    processed_files = []
    
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        result = process_file(bucket, key, "raw/batch/inventory-raw/")
        if result:
            processed_files.append(result)
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Processed {len(processed_files)} files'),
        'processed_files': processed_files
    }

def handle_step_functions_event(event):
    """Handle Step Functions invocation"""
    bucket = event['bucket']
    key = event['key']
    target_prefix = event.get('target_prefix', 'raw/batch/inventory-raw/')
    processing_date = event.get('processing_date')
    execution_name = event.get('execution_name')
    
    logger.info(f"Step Functions invocation - Processing: s3://{bucket}/{key}")
    logger.info(f"Execution: {execution_name}, Date: {processing_date}")
    
    result = process_file(bucket, key, target_prefix, processing_date)
    
    if result:
        return {
            'status': 'SUCCESS',
            'output_location': result['output_location'],
            'records_converted': result['records_converted'],
            'processing_date': processing_date,
            'run_timestamp': datetime.utcnow().isoformat(),
            'execution_name': execution_name
        }
    else:
        raise Exception("File processing failed")

def process_file(bucket, key, target_prefix, processing_date=None):
    """Core file processing logic"""
    logger.info(f"Processing file: s3://{bucket}/{key}")

    # Skip files that are already in the target path
    if target_prefix.rstrip('/') in key:
        logger.info("File is already in target folder. Skipping.")
        return None

    try:
        # Read original file content
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')

        # Parse JSON content - handle different formats
        data = []
        try:
            # First, try to parse as a single JSON array
            parsed = json.loads(content)
            if isinstance(parsed, list):
                data = parsed
            elif isinstance(parsed, dict):
                data = [parsed]  # Single object, wrap in array
            else:
                raise ValueError("Unexpected JSON structure")
                
        except json.JSONDecodeError as e:
            # If that fails, try parsing as JSON Lines (multiple JSON objects)
            logger.info(f"Standard JSON parsing failed, trying JSON Lines format: {e}")
            try:
                lines = content.strip().split('\n')
                data = []
                for i, line in enumerate(lines, 1):
                    if line.strip():  # Skip empty lines
                        try:
                            obj = json.loads(line.strip())
                            data.append(obj)
                        except json.JSONDecodeError as line_error:
                            logger.error(f"Failed to parse line {i}: {line_error}")
                            logger.error(f"Problematic line: {line[:100]}...")
                            raise Exception(f"JSON Lines parsing failed at line {i}: {line_error}")
                
                if not data:
                    raise Exception("No valid JSON objects found in file")
                    
                logger.info(f"Successfully parsed {len(data)} objects from JSON Lines format")
                
            except Exception as jsonl_error:
                logger.error(f"Both JSON and JSON Lines parsing failed. JSON error: {e}, JSONL error: {jsonl_error}")
                raise Exception(f"Unable to parse file as JSON or JSON Lines. Last error: {jsonl_error}")

        if not data:
            error_msg = f"File {key} contains no data"
            logger.error(error_msg)
            raise Exception(error_msg)

        # Prepare output path
        filename = os.path.basename(key)
        
        # Include processing date in filename if provided
        if processing_date:
            name, ext = os.path.splitext(filename)
            filename = f"{processing_date}_{filename}"
        
        output_key = f"{target_prefix.rstrip('/')}/{filename}"

        # Convert to JSON Lines
        jsonl_lines = [json.dumps(record) for record in data]
        jsonl_content = "\n".join(jsonl_lines)

        # Write converted file to target S3 location
        s3.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=jsonl_content.encode('utf-8'),
            ContentType='application/jsonl'
        )

        logger.info(f"Converted and saved: s3://{bucket}/{output_key}")
        
        return {
            'source_key': key,
            'output_location': f"s3://{bucket}/{output_key}",
            'records_converted': len(data),
            'processing_date': processing_date
        }

    except Exception as e:
        logger.error(f"Failed to process file {key}: {e}")
        raise