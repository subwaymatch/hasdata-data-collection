"""
Scrape Glassdoor job listings.

Paginates through Glassdoor job search results for the configured keyword and
location.  Results are stored in the glassdoor_listings table and backed up
under scraped_json/glassdoor_listings/.

Configuration via environment variables (or .env file):
  GLASSDOOR_KEYWORD   — search keyword,  default "software engineer"
  GLASSDOOR_LOCATION  — location string, default "United States"
  GLASSDOOR_SORT      — sort order,      default "recent"
  GLASSDOOR_DOMAIN    — Glassdoor domain, default "www.glassdoor.com"

Usage
-----
  python -m scrape_glassdoor_listings
"""

import os

from dotenv import load_dotenv

load_dotenv()

from scraper.config import settings
from scraper.db import close_db, init_db
from scraper.endpoints import ENDPOINTS
from scraper.generic_scraper import scrape_paginated

keyword = os.getenv("GLASSDOOR_KEYWORD", "software engineer")
location = os.getenv("GLASSDOOR_LOCATION", "United States")
sort = os.getenv("GLASSDOOR_SORT", "recent")
domain = os.getenv("GLASSDOOR_DOMAIN", "www.glassdoor.com")

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
