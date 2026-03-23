"""
Scrape Glassdoor job detail pages.

Reads every job URL from the glassdoor_listings table and fetches the full job
details from the HasData Glassdoor Job API.  Results are stored in the
glassdoor_jobs table and backed up under scraped_json/glassdoor_jobs/.

Usage
-----
  python -m scrape_glassdoor_jobs
"""

from scraper.config import settings
from scraper.db import close_db, init_db
from scraper.endpoints import ENDPOINTS
from scraper.generic_scraper import scrape_per_item

init_db(settings.postgres_dsn)
try:
    scrape_per_item(
        config=ENDPOINTS["glassdoor_job"],
        skip_done=True,
        delay=1.0,
    )
finally:
    close_db()
