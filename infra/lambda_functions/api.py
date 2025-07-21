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
        print(f" Failed to fetch from {api_url}: {e}")
        return None
    
    def send_to_firehose(stream_name, data):
    try:
        firehose.put_record(
            DeliveryStreamName=stream_name,
            Record={'Data': json.dumps(data) + '\n'}
        )
        print(f" Sent to Firehose: {stream_name}")
    except Exception as e:
        print(f" Firehose failed ({stream_name}): {e}")