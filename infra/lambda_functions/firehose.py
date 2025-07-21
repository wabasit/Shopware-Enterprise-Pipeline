import base64
import json
import traceback
import logging # Import logging module
from datetime import datetime

# Configure the logger for CloudWatch Logs
logger = logging.getLogger()
logger.setLevel(logging.INFO) # Set logging level

def lambda_handler(event, context):
    """
    Main Lambda handler for Firehose data transformation.
    Processes records from Firehose, cleans them, and re-encodes for delivery.
    """
    logger.info("Transformer Lambda triggered by Firehose.")
    output = []

    for record in event.get('records', []):
        try:
            # Decode base64 data and parse JSON payload
            raw_data = base64.b64decode(record['data']).decode('utf-8')
            data = json.loads(raw_data)
            logger.info(f"Received Record: {data}")

            # Clean and transform the record using the helper function
            clean_data = sanitize_record(data)
            logger.info(f"Cleaned Record: {clean_data}")

            # Re-encode the cleaned JSON for Firehose delivery
            # Using separators=(',', ':') for compact JSON output
            cleaned_json = json.dumps(clean_data, separators=(',', ':'))
            encoded_data = base64.b64encode(cleaned_json.encode('utf-8')).decode('utf-8')

            # Append the transformed record to the output list
            output.append({
                'recordId': record['recordId'],
                'result': 'Ok', # Mark as 'Ok' even if data was modified
                'data': encoded_data
            })

        except Exception as e:
            # Log detailed error for failed record processing
            logger.error(f"Record processing failed for recordId {record.get('recordId', 'N/A')}: {e}")
            logger.error(traceback.format_exc()) # Log full traceback

            # Return original record data with 'ProcessingFailed' status for Firehose
            # This allows Firehose to send the failed record to its error output destination (e.g., S3)
            output.append({
                'recordId': record['recordId'],
                'result': 'ProcessingFailed',
                'data': record['data'] # Keep original data for debugging
            })

    return {'records': output}


def sanitize_record(data):
    """
    Cleans and normalizes a single CRM interaction record.
    Handles type conversions and adds a processing timestamp.
    Raises an exception if critical data cannot be sanitized.
    """
    logger.info(f"Sanitizing record for customer_id: {data.get('customer_id', 'N/A')}")
    try:
        # Return a dictionary with cleaned and mapped fields
        return {
            'customer_id': safe_int(data.get('customer_id')),
            'interaction_type': str(data.get('interaction_type', '')),
            'channel': str(data.get('channel', '')),
            'rating': safe_rating(data.get('rating')),
            'message_excerpt': str(data.get('message_excerpt', '')),
            'event_timestamp': convert_ts(data.get('event_timestamp')),
            'ingestion_timestamp': data.get('ingestion_timestamp'), # Assumed already ISO format from poller
            'processed_at': datetime.utcnow().isoformat() # Timestamp when this Lambda processed the record
        }
    except Exception as e:
        # Log and re-raise if sanitization fails for a record
        logger.error(f"Sanitization failed for record: {e}\n{traceback.format_exc()}")
        raise # Re-raise to be caught by the main handler's try-except


def safe_int(val):
    """Safely converts value to integer, returns None on error."""
    try:
        return int(float(val))
    except Exception as e:
        logger.warning(f"Failed to convert '{val}' to int: {e}")
        return None

def safe_rating(val):
    """Safely converts rating to int (1-5), defaults to 3 on error."""
    try:
        rating = int(float(val))
        return max(1, min(5, rating)) # Clamp rating to 1-5 range
    except Exception as e:
        logger.warning(f"Invalid rating '{val}' – defaulting to 3. Error: {e}")
        return 3

def convert_ts(val):
    """Converts timestamp to ISO format, returns None on error."""
    try:
        if isinstance(val, str) and "T" in val:
            return val # Already ISO format
        ts = float(val) # Assume epoch float if not ISO string
        return datetime.utcfromtimestamp(ts).isoformat()
    except Exception as e:
        logger.warning(f"Invalid timestamp. '{val}' – defaulting to None. Error: {e}")
        return None