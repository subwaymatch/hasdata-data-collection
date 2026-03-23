"""
Endpoint registry for HasData API scrapers.

Each EndpointConfig describes:
  - Which HasData API path to call
  - Which Postgres table to store results in
  - Which subdirectory under scraped_json/ to use for local backups
  - How to extract a unique ID from each scraped item
  - Whether the endpoint is paginated (listings) or per-item (details)
  - For per-item endpoints: which source table/column provides the URLs to fetch

Adding a new endpoint requires only a new entry in ENDPOINTS.
"""

from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class EndpointConfig:
    # Unique name for this endpoint (used as a key in ENDPOINTS dict)
    name: str

    # HasData API path, e.g. "/scrape/zillow/property"
    api_path: str

    # Postgres table name where results are stored
    table_name: str

    # Subdirectory under scraped_json/ for local JSON backups
    backup_subdir: str

    # Key in the JSON response that contains the scraped data.
    # For list responses (e.g. "jobs", "properties") each element is upserted.
    # For single-object responses (e.g. "property", "job") the object itself is upserted.
    results_key: str

    # Callable(item: dict) -> str  that extracts a stable unique ID from one item.
    id_extractor: Callable[[dict], str]

    # True  → paginated listing endpoint (iterates pages until exhausted)
    # False → per-item detail endpoint (one URL fetched per source row)
    paginated: bool = False

    # --- Per-item endpoint fields (ignored when paginated=True) ---

    # Source Postgres table whose rows provide the URLs to fetch
    source_table: Optional[str] = None

    # Column in source_table that contains the URL to pass to HasData
    source_url_column: str = "url"

    # Query-parameter name to use when passing the URL to HasData
    source_url_param: str = "url"


# ---------------------------------------------------------------------------
# Registered endpoints
# ---------------------------------------------------------------------------

ENDPOINTS: dict[str, EndpointConfig] = {
    # ------------------------------------------------------------------
    # Zillow: listing search  (already implemented in scraper.py; included
    # here so it can also be driven through the generic pipeline)
    # ------------------------------------------------------------------
    "zillow_listing": EndpointConfig(
        name="zillow_listing",
        api_path="/scrape/zillow/listing",
        table_name="zillow_listings",
        backup_subdir="zillow_listings",
        results_key="properties",
        id_extractor=lambda item: str(item["id"]),
        paginated=True,
    ),

    # ------------------------------------------------------------------
    # Zillow: property details  (fetched per listing URL)
    # ------------------------------------------------------------------
    "zillow_property": EndpointConfig(
        name="zillow_property",
        api_path="/scrape/zillow/property",
        table_name="zillow_properties",
        backup_subdir="zillow_properties",
        results_key="property",          # single object in response
        id_extractor=lambda item: str(item["zpid"]),
        paginated=False,
        source_table="zillow_listings",
        source_url_column="url",
        source_url_param="url",
    ),

    # ------------------------------------------------------------------
    # Glassdoor: job listing search
    # ------------------------------------------------------------------
    "glassdoor_listing": EndpointConfig(
        name="glassdoor_listing",
        api_path="/scrape/glassdoor/listing",
        table_name="glassdoor_listings",
        backup_subdir="glassdoor_listings",
        results_key="jobs",
        id_extractor=lambda item: item["url"],  # job URL is the stable unique key
        paginated=True,
    ),

    # ------------------------------------------------------------------
    # Glassdoor: job details  (fetched per listing URL)
    # ------------------------------------------------------------------
    "glassdoor_job": EndpointConfig(
        name="glassdoor_job",
        api_path="/scrape/glassdoor/job",
        table_name="glassdoor_jobs",
        backup_subdir="glassdoor_jobs",
        results_key="job",               # single object in response
        id_extractor=lambda item: item["url"],
        paginated=False,
        source_table="glassdoor_listings",
        source_url_column="item_id",     # glassdoor_listings uses item_id (the job URL)
        source_url_param="url",
    ),
}
