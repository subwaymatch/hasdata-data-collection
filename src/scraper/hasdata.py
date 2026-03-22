"""
HasData API client.

Mirrors the retry/backoff logic from your original notebook,
wrapped in a class for reuse across different endpoint types.
"""

import time
from urllib.parse import urlencode

import requests

from .config import settings


class HasDataClient:
    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update(
            {
                "x-api-key": settings.hasdata_api_key,
                "Content-Type": "application/json",
            }
        )

    def _get(self, url: str) -> dict:
        """GET with exponential backoff, matching your original notebook logic."""
        for attempt in range(1, settings.max_retries + 1):
            try:
                response = self._session.get(url, timeout=settings.request_timeout)

                if response.status_code == 200:
                    return response.json()

                print(f"  HTTP {response.status_code} on attempt {attempt}")

            except Exception as exc:
                print(f"  Request error on attempt {attempt}: {exc}")

            sleep_time = settings.backoff_factor**attempt
            print(f"  Retrying in {sleep_time}s…")
            time.sleep(sleep_time)

        raise RuntimeError(f"Max retries exceeded for: {url}")

    def fetch_listings_page(
        self,
        location: str,
        listing_type: str,
        page: int,
        hide_55_plus: bool = True,
    ) -> dict:
        """
        Fetch one page of Zillow listing search results.

        Returns the full HasData response dict:
          { requestMetadata, searchInformation, properties, pagination }
        """
        params = {
            "keyword": location,
            "type": listing_type,
            "hide55plusCommunities": str(hide_55_plus).lower(),
            "page": page,
        }
        url = f"{settings.hasdata_base_url}?{urlencode(params)}"
        return url, self._get(url)

    def close(self):
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
