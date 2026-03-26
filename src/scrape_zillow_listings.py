"""
Scrape Zillow sold house listings for Champaign, IL.

Strategy
--------
Zillow caps total paginated results per search query, so we break the search
into granular sub-queries along two dimensions:

  1. yearBuilt — one query per year from 1900 to 2026 (inclusive)
  2. beds      — two ranges per year:
                   • 1–3 bedrooms
                   • 4+ bedrooms (no upper bound)

Only `homeTypes[]=house` is queried.

For each combination, all available pages are paginated through and results
are upserted into zillow_listings.  JSON responses are backed up under
scraped_json/zillow_listings/ with filenames that embed the year and bed range.

Resume support: any page whose URL is already recorded in scraped_pages is
skipped automatically (pass skip_done=False to force a full re-fetch).
"""

from concurrent.futures import ThreadPoolExecutor, as_completed

from scraper.config import settings
from scraper.db import close_db, init_db
from scraper.endpoints import ENDPOINTS
from scraper.generic_scraper import scrape_paginated

LOCATION = "Ann Arbor, MI"
LISTING_TYPE = "sold"

# Year-built loop: one year at a time
YEAR_MIN = 1900
YEAR_MAX = 2026

# Bedroom ranges: (beds_min, beds_max) — beds_max=None means no upper bound (4+)
BED_RANGES = [
    (1, 3),
    (4, None),
]

# Number of (year, beds) combinations to scrape in parallel.
# Each worker makes its own independent sequence of paginated requests.
# Raise to increase throughput; lower if you hit API rate limits.
MAX_WORKERS = 15

config = ENDPOINTS["zillow_listing"]


def _scrape_combo(year: int, beds_min: int, beds_max: int | None) -> None:
    beds_label = (
        f"beds-{beds_min}-{beds_max}"
        if beds_max is not None
        else f"beds-{beds_min}plus"
    )
    location_label = LOCATION.lower().replace(",", "").replace(" ", "-")
    page_label = f"{location_label}-{LISTING_TYPE}-year-{year}-{beds_label}"

    base_params: dict = {
        "keyword": LOCATION,
        "type": LISTING_TYPE,
        "hide55plusCommunities": "true",
        "homeTypes[]": "house",
        "yearBuilt[min]": year,
        "yearBuilt[max]": year,
        "beds[min]": beds_min,
    }
    if beds_max is not None:
        base_params["beds[max]"] = beds_max

    scrape_paginated(
        config=config,
        base_params=base_params,
        skip_done=True,
        page_label=page_label,
    )


combos = [
    (year, beds_min, beds_max)
    for year in range(YEAR_MIN, YEAR_MAX + 1)
    for beds_min, beds_max in BED_RANGES
]

init_db(settings.postgres_dsn)
try:
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_scrape_combo, *combo): combo for combo in combos}
        for future in as_completed(futures):
            future.result()
finally:
    close_db()
