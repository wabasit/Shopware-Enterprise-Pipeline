import os
import boto3
import json
import time
import requests
from botocore.exceptions import ClientError
from typing import Dict, Generator, Optional

def poll_api(
    base_url: str,
    headers: Optional[Dict] = None,
    params: Optional[Dict] = None,
    poll_interval: float = 2.0,
    timeout: int = 10,
    max_events: Optional[int] = None,
    max_retries: int = 3,
) -> Generator[Dict, None, None]:
    """
    Polls an API endpoint that returns one event per call.

    :param base_url: URL to fetch from
    :param headers: HTTP headers
    :param params: Optional query parameters
    :param poll_interval: Seconds to wait between polls
    :param timeout: Request timeout
    :param max_events: Stop after this many events (optional)
    :param max_retries: Retry attempts on failure
    :yield: Each event as a dict
    """
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

                # Check for new schema structure
                required_fields = ["customer_id", "interaction_type", "channel", "rating", "timestamp"]

                if isinstance(data, dict) and all(field in data for field in required_fields):
                    yield data
                else:
                    print(f"[INFO] Empty or malformed response: {data}")

                break  # break out of retry loop

            except requests.exceptions.RequestException as e:
                wait = poll_interval * (2 ** attempt)
                print(f"[Retry {attempt+1}] Error: {e}. Retrying in {wait:.1f}s...")
                time.sleep(wait)
        else:
            print("[ERROR] Max retries reached. Stopping polling.")
            break

        print(f"[POLLING] Attempt #{event_count + 1 if max_events else '?'}")


        time.sleep(poll_interval)


def send_to_kinesis(stream_name: str, region: str, event: dict):
    client = boto3.client('kinesis', region_name=region)

    try:
        response = client.put_record(
            StreamName=stream_name,
            Data=json.dumps(event),
            PartitionKey=str(event.get("user_id", "default"))
        )
        print(f"[SENT] SequenceNumber: {response['SequenceNumber']}")
    except ClientError as e:
        print(f"[ERROR] Failed to send to Kinesis: {e}")

if __name__ == "__main__":
    API_URL = os.getenv("API_URL")
    STREAM_NAME = os.getenv("STREAM_NAME")
    REGION = os.getenv("REGION")

    for event in poll_api(API_URL):
        send_to_kinesis(STREAM_NAME, REGION, event)
