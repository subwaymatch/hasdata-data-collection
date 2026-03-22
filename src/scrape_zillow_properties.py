"""
Scrape Zillow property detail pages.

Reads every URL from the zillow_listings table and fetches the full property
details from the HasData Zillow Property API.  Results are stored in the
zillow_properties table and backed up under scraped_json/zillow_properties/.

Usage
-----
  python -m scrape_zillow_properties
  # or via the project entry-point if configured in pyproject.toml
"""

from scraper.config import settings
from scraper.db import close_db, init_db
from scraper.endpoints import ENDPOINTS
from scraper.generic_scraper import scrape_per_item

init_db(settings.postgres_dsn)
try:
    scrape_per_item(
        config=ENDPOINTS["zillow_property"],
        skip_done=True,
        delay=1.0,
    )
finally:
    close_db()
