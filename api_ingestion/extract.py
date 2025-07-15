import requests
import time
from typing import Dict, Generator, Optional, Union

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
    Flexible API extractor supporting page, offset, cursor, link-based and single-record APIs.
    Yields one record (dict) at a time.
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
            print(f"[ERROR] Failed after {max_retries} retries. Exiting.")
            break

        try:
            data = response.json()
        except ValueError:
            print("[ERROR] Invalid JSON. Skipping this response.")
            break

        # Debug output to verify API structure
        # print("[DEBUG] Raw response:", data)

        # Handle various response formats
        if isinstance(data, list):
            for record in data:
                yield record

        elif isinstance(data, dict):
            # Case 1: Single object (not paginated)
            if all(k in data for k in ['session_id', 'timestamp', 'event_type']):
                yield data
                break

            # Case 2: Paginated data under 'results' or 'data'
            records = data.get('results') or data.get('data')
            if isinstance(records, list):
                for record in records:
                    yield record
            else:
                print("[INFO] No records found in response.")
                break

            # Pagination handling
            if pagination_strategy == 'link' or (pagination_strategy == 'auto' and data.get('next')):
                next_url = data.get('next')
                if not next_url:
                    break
            elif pagination_strategy == 'cursor' or (pagination_strategy == 'auto' and 'next_cursor' in data):
                cursor = data.get('next_cursor') or data.get('next_page_token')
                if not cursor:
                    break
            elif pagination_strategy == 'offset':
                offset += limit
            elif pagination_strategy == 'page':
                page += 1
            else:
                break

        else:
            print("[WARN] Unsupported response format.")
            break

        time.sleep(delay)
