import requests
import time
from typing import Dict, Generator, Optional

def extract_data(
    base_url: str,
    headers: Optional[Dict] = None,
    params: Optional[Dict] = None,
    pagination_strategy: str = 'auto',
    limit: int = 100,
    max_pages: int = 50,
    delay: float = 1.0,
    max_retries: int = 3,
    timeout: int = 10,
) -> Generator[Dict, None, None]:
    """
    API extractor with support for page, offset, cursor, link, and auto-detection pagination.
    Streams data one record at a time.

    Args:
        base_url (str): API endpoint
        headers (dict): Optional request headers
        params (dict): Query parameters
        pagination_strategy (str): 'page', 'offset', 'cursor', 'link', or 'auto'
        limit (int): Items per request (for offset or cursor)
        max_pages (int): Max requests before stopping
        delay (float): Delay between requests
        max_retries (int): Max retry attempts
        timeout (int): Timeout for requests

    Yields:
        dict: Single record from the API
    """
    headers = headers or {}
    params = params or {}

    page = 1
    offset = 0
    cursor = None
    next_url = base_url

    for _ in range(max_pages):
        full_params = params.copy()

        if pagination_strategy == 'page' or (pagination_strategy == 'auto' and 'page' in params):
            full_params['page'] = page
        elif pagination_strategy == 'offset':
            full_params['offset'] = offset
            full_params['limit'] = limit
        elif pagination_strategy == 'cursor' and cursor:
            full_params['cursor'] = cursor

        url_to_fetch = next_url if pagination_strategy == 'link' else base_url

        # Retry mechanism
        for attempt in range(max_retries):
            try:
                response = requests.get(url_to_fetch, headers=headers, params=full_params, timeout=timeout)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                wait_time = delay * (2 ** attempt)
                print(f"[Attempt {attempt + 1}] Error fetching data: {e}. Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
        else:
            print(f"[ERROR] Failed to fetch data after {max_retries} attempts.")
            break

        try:
            data = response.json()
        except ValueError:
            print("[ERROR] Invalid JSON. Skipping this response.")
            break

        # Handle various data structures
        if isinstance(data, list):
            records = data
        else:
            records = data.get('results') or data.get('data') or []

        if not records:
            print("[INFO] No records found in response.")
            break

        for record in records:
            yield record

        # Pagination strategy
        if pagination_strategy == 'link' or (pagination_strategy == 'auto' and data.get('next')):
            next_url = data.get('next')
            if not next_url:
                break
        elif pagination_strategy == 'cursor' or (pagination_strategy == 'auto' and ('next_cursor' in data or 'next_page_token' in data)):
            cursor = data.get('next_cursor') or data.get('next_page_token')
            if not cursor:
                break
        elif pagination_strategy == 'offset':
            offset += limit
        elif pagination_strategy == 'page':
            page += 1
        else:
            break

        time.sleep(delay)