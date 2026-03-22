# hasdata-data-collection

Scrapes data via the [HasData](https://hasdata.com) API, upserts structured data to Postgres (via [Peewee ORM](https://docs.peewee-orm.com)), and keeps local JSON backups of every page response. Track scraped page URLs in a `scraped_pages` table to enable safe re-runs without duplicate API calls.

## Project layout

```
hasdata-data-collection/
├── pyproject.toml
├── .env                        # copy from .env.example and fill in
├── .env.example
└── src/
    └── scraper/
        ├── __init__.py
        ├── config.py           # Settings loaded from .env
        ├── db.py               # DB init, dedup helpers, upsert logic
        ├── hasdata.py          # HasData API client with retry/backoff
        ├── models.py           # Peewee ORM models (ScrapedPage, ZillowListing)
        └── scraper.py          # Pagination loop
```

## Setup

```bash
# 1. Install (editable mode)
pip install -e .

# 2. Configure
cp .env.example .env
# edit .env — add HASDATA_API_KEY and POSTGRES_DSN
```

```python
# 3. Create tables (Python script usage)
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


```

## Database tables

### `scraped_pages`

Tracks every HasData page URL that has been successfully fetched. This is the dedup/resume key — re-running the scraper will skip any URL already present here (unless `--force` is passed).

| Column           | Type        | Notes                              |
| ---------------- | ----------- | ---------------------------------- |
| `url`            | TEXT (UQ)   | Full HasData request URL           |
| `location`       | TEXT        | e.g. `Champaign, IL`               |
| `listing_type`   | TEXT        | e.g. `sold`                        |
| `page_number`    | INTEGER     |                                    |
| `property_count` | INTEGER     | Properties upserted from this page |
| `scraped_at`     | TIMESTAMPTZ |                                    |

### `zillow_listings`

One row per Zillow property, upserted on `property_id` (Zillow zpid). Re-scraping updates `price`, `status`, `zestimate`, `rent_zestimate`, `days_on_zillow`, `raw_json`, and `updated_at`.

| Column                      | Type        |
| --------------------------- | ----------- |
| `property_id`               | BIGINT PK   |
| `url`                       | TEXT        |
| `home_type`                 | TEXT        |
| `status`                    | TEXT        |
| `listing_type`              | TEXT        |
| `price`                     | INTEGER     |
| `zestimate`                 | INTEGER     |
| `rent_zestimate`            | INTEGER     |
| `beds`                      | INTEGER     |
| `baths`                     | FLOAT       |
| `area`                      | INTEGER     |
| `days_on_zillow`            | INTEGER     |
| `address_raw`               | TEXT        |
| `street/city/state/zipcode` | TEXT        |
| `latitude/longitude`        | FLOAT       |
| `broker_name`               | TEXT        |
| `raw_json`                  | JSONB       |
| `created_at`                | TIMESTAMPTZ |
| `updated_at`                | TIMESTAMPTZ |

## Local backups

Every page response is saved as:

```
zillow_data/zillow-listings-champaign-il-sold-page-01.json
```

On reruns, if the local file already exists it is loaded directly (no API call made). This means you can safely re-run after a crash without burning API credits.
