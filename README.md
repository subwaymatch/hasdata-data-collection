# zillow-scraper

Scrapes Zillow listings via the [HasData](https://hasdata.com) API, upserts structured data to Postgres (via [Peewee ORM](https://docs.peewee-orm.com)), and keeps local JSON backups of every page response.

## Project layout

```
zillow-scraper/
├── pyproject.toml
├── .env                        # copy from .env.example and fill in
├── .env.example
└── src/
    └── zillow_scraper/
        ├── __init__.py
        ├── cli.py              # Typer CLI entry point
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

# 3. Create tables
zillow-scraper init-db
```

## Usage

```bash
# Scrape default location + type (from .env)
zillow-scraper scrape

# Override location and listing type
zillow-scraper scrape --location "Urbana, IL" --type forSale

# Force re-fetch even for pages already in the DB
zillow-scraper scrape --force

# Slower, polite delay between requests
zillow-scraper scrape --delay 2.0
```

## Database tables

### `scraped_pages`
Tracks every HasData page URL that has been successfully fetched. This is the dedup/resume key — re-running the scraper will skip any URL already present here (unless `--force` is passed).

| Column           | Type      | Notes                              |
|------------------|-----------|------------------------------------|
| `url`            | TEXT (UQ) | Full HasData request URL           |
| `location`       | TEXT      | e.g. `Champaign, IL`               |
| `listing_type`   | TEXT      | e.g. `sold`                        |
| `page_number`    | INTEGER   |                                    |
| `property_count` | INTEGER   | Properties upserted from this page |
| `scraped_at`     | TIMESTAMPTZ |                                  |

### `zillow_listings`
One row per Zillow property, upserted on `property_id` (Zillow zpid). Re-scraping updates `price`, `status`, `zestimate`, `rent_zestimate`, `days_on_zillow`, `raw_json`, and `updated_at`.

| Column           | Type    |
|------------------|---------|
| `property_id`    | BIGINT PK |
| `url`            | TEXT    |
| `home_type`      | TEXT    |
| `status`         | TEXT    |
| `listing_type`   | TEXT    |
| `price`          | INTEGER |
| `zestimate`      | INTEGER |
| `rent_zestimate` | INTEGER |
| `beds`           | INTEGER |
| `baths`          | FLOAT   |
| `area`           | INTEGER |
| `days_on_zillow` | INTEGER |
| `address_raw`    | TEXT    |
| `street/city/state/zipcode` | TEXT |
| `latitude/longitude` | FLOAT |
| `broker_name`    | TEXT    |
| `raw_json`       | JSONB   |
| `created_at`     | TIMESTAMPTZ |
| `updated_at`     | TIMESTAMPTZ |

## Local backups

Every page response is saved as:
```
zillow_data/zillow-listings-champaign-il-sold-page-01.json
```
On reruns, if the local file already exists it is loaded directly (no API call made). This means you can safely re-run after a crash without burning API credits.
