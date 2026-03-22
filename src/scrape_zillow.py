from scraper.config import settings
from scraper.db import close_db, init_db
from scraper.scraper import scrape

init_db(settings.postgres_dsn)
try:
    scrape(
        location=settings.default_location,
        listing_type=settings.default_listing_type,
        skip_done=True,  # set False to force re-fetch
        hide_55_plus=True,  # set False to include 55+ communities
        delay=1.0,
    )
finally:
    close_db()
