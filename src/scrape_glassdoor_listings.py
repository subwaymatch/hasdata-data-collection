"""
Scrape Glassdoor job listings.

Paginates through Glassdoor job search results for the given keyword and
location.  Results are stored in the glassdoor_listings table and backed up
under scraped_json/glassdoor_listings/.

Edit the variables below to change what is scraped.

Usage
-----
  python -m scrape_glassdoor_listings
"""

from scraper.config import settings
from scraper.db import close_db, init_db
from scraper.endpoints import ENDPOINTS
from scraper.generic_scraper import scrape_paginated

# ── Search parameters — edit these as needed ──────────────────────────────── #
keyword = "software engineer"
location = "United States"
sort = "recent"           # "recent" | "relevance"
domain = "www.glassdoor.com"
# ─────────────────────────────────────────────────────────────────────────── #

base_params = {
    "keyword": keyword,
    "location": location,
    "sort": sort,
    "domain": domain,
}

init_db(settings.postgres_dsn)
try:
    scrape_paginated(
        config=ENDPOINTS["glassdoor_listing"],
        base_params=base_params,
        skip_done=True,
        delay=1.0,
        page_label=f"{keyword} @ {location}",
    )
finally:
    close_db()
