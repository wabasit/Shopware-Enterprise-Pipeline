import requests
import json
import boto3
import datetime
import time

# AWS clients
firehose = boto3.client('firehose')

# Firehose stream names
FIREHOSE_MAP = {
    'customer-interaction': 'shopware-firehose',
    'web-logs': 'weblogs-firehose'
}

# === HELPERS ===

def safe_int(val):
    try:
        return int(float(val))
    except:
        return None

def fetch_data(api_url):
    try:
        response = requests.get(api_url, timeout=5)
        if response.status_code >= 500:
            print(f"🚨 Server error from {api_url}: {response.status_code}")
            return None
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"⚠️ Failed to fetch from {api_url}: {e}")
        return None

def send_to_firehose(stream_name, data):
    try:
        firehose.put_record(
            DeliveryStreamName=stream_name,
            Record={'Data': json.dumps(data) + '\n'}
        )
        print(f"✅ Sent to Firehose: {stream_name}")
    except Exception as e:
        print(f"❌ Firehose failed ({stream_name}): {e}")

# === CLEANERS ===

def clean_crm(payload):
    try:
        rating = int(float(payload.get('rating', 3)))
        payload['rating'] = max(1, min(5, rating))
    except:
        payload['rating'] = 3

    try:
        ts = float(payload.pop('timestamp'))
        payload['event_timestamp'] = datetime.datetime.utcfromtimestamp(ts).isoformat()
    except:
        payload['event_timestamp'] = None

    payload['ingestion_timestamp'] = datetime.datetime.utcnow().isoformat()

    return {
        'customer_id': safe_int(payload.get('customer_id')),
        'interaction_type': str(payload.get('interaction_type', '')),
        'channel': str(payload.get('channel', '')),
        'rating': payload.get('rating'),
        'message_excerpt': str(payload.get('message_excerpt', '')),
        'event_timestamp': payload.get('event_timestamp'),
        'ingestion_timestamp': payload['ingestion_timestamp']
    }

def clean_weblog(payload):
    try:
        ts = float(payload.pop('timestamp'))
        payload['event_timestamp'] = datetime.datetime.utcfromtimestamp(ts).isoformat()
    except:
        payload['event_timestamp'] = None

    payload['ingestion_timestamp'] = datetime.datetime.utcnow().isoformat()

    return {
        'session_id': payload.get('session_id'),
        'user_id': payload.get('user_id'),
        'page': payload.get('page'),
        'device_type': payload.get('device_type'),
        'browser': payload.get('browser'),
        'event_type': payload.get('event_type'),
        'event_timestamp': payload.get('event_timestamp'),
        'ingestion_timestamp': payload['ingestion_timestamp']
    }

# === MAIN ===

def lambda_handler(event, context):
    print(" Starting unified API poller")

    # === CRM ===
    crm_data = fetch_data("http://3.248.199.26:8000/api/customer-interaction/")
    if crm_data:
        clean_crm_data = clean_crm(crm_data)
        send_to_firehose(FIREHOSE_MAP['customer-interaction'], clean_crm_data)

    # === WEB LOGS ===
    weblog_data = fetch_data("http://3.248.199.26:8000/api/web-traffic/")
    if weblog_data:
        clean_weblog_data = clean_weblog(weblog_data)
        send_to_firehose(FIREHOSE_MAP['web-logs'], clean_weblog_data)

    return {
        'statusCode': 200,
        'message': 'CRM and Web Log data pushed to Firehose'
    }
