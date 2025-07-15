import os
import boto3
import json
import time
import requests
from botocore.exceptions import ClientError
from typing import Dict, Generator, Optional
import logging
import watchtower

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(watchtower.CloudWatchLogHandler(log_group="kinesis-pipeline-logs"))
logger.info("Logger initialized for Kinesis pipeline.")

def poll_api(
    base_url: str,
    headers: Optional[Dict] = None,
    params: Optional[Dict] = None,
    poll_interval: float = 2.0,
    timeout: int = 30,
    max_events: Optional[int] = None,
    max_retries: int = 3,
) -> Generator[Dict, None, None]:
    headers = headers or {}
    params = params or {}
    event_count = 0

    while True:
        if max_events and event_count >= max_events:
            break

        for attempt in range(max_retries):
            try:
                response = requests.get(base_url, headers=headers, params=params, timeout=timeout)
                response.raise_for_status()
                data = response.json()

                required_fields = ["customer_id", "interaction_type", "channel", "rating", "timestamp"]
                if isinstance(data, dict) and all(field in data for field in required_fields):
                    yield data
                    event_count += 1
                else:
                    logger.info(f"[INFO] Empty or malformed response: {data}")
                break
            except requests.exceptions.RequestException as e:
                wait = poll_interval * (2 ** attempt)
                logger.warning(f"[Retry {attempt+1}] Error: {e}. Retrying in {wait:.1f}s...")
                time.sleep(wait)
        else:
            logger.error("[ERROR] Max retries reached for API.")
            break

        logger.info(f"[POLLING] Event #{event_count}")
        time.sleep(poll_interval)


def send_to_kinesis(stream_name: str, region: str, event: dict, max_retries=3):
    kinesis = boto3.client("kinesis", region_name=region)
    cloudwatch = boto3.client("cloudwatch", region_name=region)

    for attempt in range(max_retries):
        try:
            response = kinesis.put_record(
                StreamName=stream_name,
                Data=json.dumps(event),
                PartitionKey=str(event.get("user_id", "default"))
            )

            print(f"[SENT] SequenceNumber: {response['SequenceNumber']}")
            cloudwatch.put_metric_data(
                Namespace='KinesisIngestion',
                MetricData=[{
                    'MetricName': 'RecordsSent',
                    'Value': 1,
                    'Unit': 'Count'
                }]
            )
            break

        except ClientError as e:
            logger.warning(f"[Retry {attempt+1}] Failed to send to Kinesis: {e}")
            time.sleep(2 ** attempt)

            cloudwatch.put_metric_data(
                Namespace='KinesisIngestion',
                MetricData=[{
                    'MetricName': 'SendRetries',
                    'Value': 1,
                    'Unit': 'Count'
                }]
            )
    else:
        logger.error(f"[ERROR] Failed to send after {max_retries} retries.")
        cloudwatch.put_metric_data(
            Namespace='KinesisIngestion',
            MetricData=[{
                'MetricName': 'SendFailures',
                'Value': 1,
                'Unit': 'Count'
            }]
        )


if __name__ == "__main__":
    API_URL = os.getenv("API_URL")
    STREAM_NAME = os.getenv("STREAM_NAME")
    REGION = os.getenv("REGION")

    try:
        for event in poll_api(API_URL):
            send_to_kinesis(STREAM_NAME, REGION, event)
    except KeyboardInterrupt:
        logger.info("[EXIT] Graceful shutdown by user.")
