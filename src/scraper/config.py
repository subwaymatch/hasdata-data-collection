import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class Settings:
    # HasData
    hasdata_api_key: str = os.environ["HASDATA_API_KEY"]
    hasdata_base_url: str = os.getenv(
        "HASDATA_BASE_URL", "https://api.hasdata.com/scrape/zillow/listing"
    )

    # Postgres
    postgres_dsn: str = os.environ["POSTGRES_DSN"]

    # Local backup
    backup_dir: Path = Path(os.getenv("BACKUP_DIR", "scraped_json/zillow_listings"))

    # Scraper behaviour
    default_listing_type: str = os.getenv("DEFAULT_LISTING_TYPE", "sold")
    default_location: str = os.getenv("DEFAULT_LOCATION", "Champaign, IL")
    max_retries: int = int(os.getenv("MAX_RETRIES", "5"))
    backoff_factor: int = int(os.getenv("BACKOFF_FACTOR", "2"))
    request_timeout: int = int(os.getenv("REQUEST_TIMEOUT", "30"))

    def __init__(self):
        self.backup_dir.mkdir(parents=True, exist_ok=True)


settings = Settings()
