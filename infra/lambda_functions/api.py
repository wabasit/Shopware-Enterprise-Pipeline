import requests
import json
import boto3
import datetime
import time
import random
import os
import logging
import traceback

# Logger Setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration: Firehose stream names from environment variables or defaults
FIREHOSE_STREAMS = {
    'customer-interaction': os.getenv('CRM_FIREHOSE_STREAM', 'shopware-firehose'),
    'web-logs': os.getenv('WEBLOG_FIREHOSE_STREAM', 'shopware-web-firehose')
}
S3_BUCKET = os.getenv('ARCHIVE_BUCKET', 'shopware-bucket-g3')

# AWS Clients
firehose = boto3.client('firehose')
s3 = boto3.client('s3')

# === HELPER FUNCTIONS ===

def safe_int(val):
    """Safely converts value to integer, logs warning on failure."""
    try:
        return int(float(val))
    except Exception as e:
        logger.warning(f"Failed to convert '{val}' to integer: {e}\n{traceback.format_exc()}")
        return None

def fetch_data(api_url, retries=3):
    """Fetches JSON data from API with exponential backoff retries."""
    logger.info(f"Fetching data from API: {api_url} with {retries} retries.")
    for attempt in range(retries):
        try:
            response = requests.get(api_url, timeout=5)
            if response.status_code >= 500:
                raise requests.exceptions.HTTPError(f"Server error {response.status_code}: {response.text}")
            response.raise_for_status()
            result = response.json()
            
            if isinstance(result, list) and result:
                logger.info(f"Fetched list from {api_url}. Returning first record.")
                return result[0]
            
            logger.info(f"Fetched single record from {api_url}.")
            return result
        
        except requests.exceptions.Timeout:
            logger.warning(f"Attempt {attempt+1}/{retries} timed out for {api_url}.")
        except requests.exceptions.HTTPError as e:
            logger.warning(f"Attempt {attempt+1}/{retries} failed with HTTP error for {api_url}: {e.response.status_code} - {e.response.text}")
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Attempt {attempt+1}/{retries} failed with connection error for {api_url}: {e}")
        except json.JSONDecodeError as e:
            logger.warning(f"Attempt {attempt+1}/{retries} failed with JSON decode error for {api_url}: {e} - Response text: {response.text}")
        except Exception as e:
            logger.warning(f"Attempt {attempt+1}/{retries} failed for {api_url}: {e}\n{traceback.format_exc()}")
        
        if attempt < retries - 1:
            wait_time = (2 ** attempt) + random.random()
            logger.info(f"Retrying {api_url} in {wait_time:.2f} seconds...")
            time.sleep(wait_time)
            
    logger.error(f"All {retries} retries failed for API: {api_url}. Returning None.")
    return None

def archive_raw(data, api_type):
    """Archives raw API response to S3."""
    timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')
    key = f"raw/{api_type}/{timestamp}.json"
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(data))
        logger.info(f"Archived raw {api_type} data to s3://{S3_BUCKET}/{key}")
    except Exception as e:
        logger.error(f"Failed to archive raw {api_type} data to s3://{S3_BUCKET}/{key}: {e}\n{traceback.format_exc()}")

def send_to_firehose(stream_name, data):
    """Sends cleaned record to Firehose, adds newline for Redshift."""
    try:
        firehose.put_record(
            DeliveryStreamName=stream_name,
            Record={'Data': json.dumps(data) + '\n'}
        )
        logger.info(f"Sent record to Firehose stream: {stream_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to send record to Firehose stream '{stream_name}': {e}\n{traceback.format_exc()}")
        return False

# === CLEANERS ===

def clean_crm(payload):
    """Cleans and transforms CRM interaction payload."""
    logger.info(f"Cleaning CRM payload: Customer ID - {payload.get('customer_id', 'N/A')}")
    
    try:
        rating = int(float(payload.get('rating', 3)))
        payload['rating'] = max(1, min(5, rating))
    except Exception as e:
        logger.warning(f"Failed to process CRM 'rating' '{payload.get('rating')}'. Defaulting to 3. Error: {e}")
        payload['rating'] = 3

    try:
        ts = float(payload.pop('timestamp'))
        payload['event_timestamp'] = datetime.datetime.utcfromtimestamp(ts).isoformat()
    except Exception as e:
        logger.warning(f"Failed to convert CRM 'timestamp' '{payload.get('timestamp')}' to event_timestamp. Setting to None. Error: {e}")
        payload['event_timestamp'] = None

    payload['ingestion_timestamp'] = datetime.datetime.utcnow().isoformat()

    cleaned_record = {
        'customer_id': safe_int(payload.get('customer_id')),
        'interaction_type': str(payload.get('interaction_type', '')),
        'channel': str(payload.get('channel', '')),
        'rating': payload.get('rating'),
        'message_excerpt': str(payload.get('message_excerpt', '')),
        'event_timestamp': payload.get('event_timestamp'),
        'ingestion_timestamp': payload['ingestion_timestamp']
    }
    logger.info(f"Cleaned CRM record for customer_id: {cleaned_record['customer_id']}")
    return cleaned_record

def clean_weblog(payload):
    """Cleans and transforms web log payload."""
    logger.info(f"Cleaning weblog payload: Session ID - {payload.get('session_id', 'N/A')}")

    try:
        ts = float(payload.pop('timestamp'))
        event_timestamp = datetime.datetime.utcfromtimestamp(ts).isoformat()
    except Exception as e:
        logger.warning(f"Failed to convert weblog 'timestamp' '{payload.get('timestamp')}' to event_timestamp. Setting to None. Error: {e}")
        event_timestamp = None

    ingestion_timestamp = datetime.datetime.utcnow().isoformat()

    cleaned_record = {
        'session_id': payload.get('session_id'),
        'user_id': str(payload.get('user_id') or ''),
        'page': payload.get('page'),
        'device_type': payload.get('device_type'),
        'browser': payload.get('browser'),
        'event_type': payload.get('event_type'),
        'event_timestamp': event_timestamp,
        'ingestion_timestamp': ingestion_timestamp
    }
    logger.info(f"Cleaned weblog record for session_id: {cleaned_record['session_id']}")
    return cleaned_record

# === MAIN LAMBDA ENTRYPOINT ===

def lambda_handler(event, context):
    """
    Main Lambda function. Polls APIs, archives, cleans, and sends to Firehose.
    """
    logger.info("Lambda execution started for unified API poller.")

    # CRM Data Polling and Processing
    crm_api_url = "http://3.248.199.26:8000/api/customer-interaction/"
    logger.info(f"Initiating CRM data polling from: {crm_api_url}")
    crm_data = fetch_data(crm_api_url)
    
    if crm_data:
        logger.info("CRM data fetched.. Archiving and cleaning.")
        archive_raw(crm_data, 'crm')
        clean_crm_data = clean_crm(crm_data)
        if clean_crm_data:
            logger.info("CRM data cleaned. Sending to Firehose.")
            send_to_firehose(FIREHOSE_STREAMS['customer-interaction'], clean_crm_data)
        else:
            logger.error("CRM data cleaning resulted in invalid record. Skipping Firehose delivery for CRM.")
    else:
        logger.warning(f"No CRM data fetched from {crm_api_url}. Skipping CRM data processing.")

    # Web Logs Data Polling and Processing
    weblog_api_url = "http://3.248.199.26:8000/api/web-traffic/"
    logger.info(f"Initiating web log data polling, from: {weblog_api_url}")
    weblog_data = fetch_data(weblog_api_url)
    
    if weblog_data:
        logger.info("Web log data fetched. Archiving and cleaning.")
        archive_raw(weblog_data, 'web-logs')
        clean_weblog_data = clean_weblog(weblog_data)
        if clean_weblog_data:
            logger.info("Web log data cleaned. Sending to Firehose.")
            send_to_firehose(FIREHOSE_STREAMS['web-logs'], clean_weblog_data)
        else:
            logger.error("Web log data cleaning resulted in invalid record. Skipping Firehose delivery for web logs.")
    else:
        logger.warning(f"No web log data fetched from {weblog_api_url}. Skipping web logs data processing.")

    logger.info("Lambda execution completed for unified API poller.")
    
    return {
        'statusCode': 200,
        'message': 'API data polling and Firehose delivery attempt completed.'
    }