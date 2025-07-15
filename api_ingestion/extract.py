import requests
from typing import Generator, Dict, Any
from api_ingestion.utils import log, retry_request

def extract_paginated_data(api_url: str, headers: Dict[str, str] = None, params: Dict[str, Any] = None) -> Generator[Dict, None, None]:
    """
    Extracts data from a paginated API endpoint.
    """
    page = 1
    has_more = True

    while has_more:
        try:
            full_params = params.copy() if params else {}
            full_params.update({'page': page})
            
            log(f"Requesting page {page} from API...")
            response = retry_request(api_url, headers=headers, params=full_params)
            response.raise_for_status()
            
            data = response.json()
            if isinstance(data, dict):
                yield data
                # Decide how to end pagination
                has_more = False  # Modify if API has next_page token
            elif isinstance(data, list):
                for item in data:
                    yield item
                if len(data) == 0:
                    has_more = False
            else:
                log("Unexpected response format")
                break

            page += 1

        except requests.RequestException as e:
            log(f"API request failed: {e}")
            break
