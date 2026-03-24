# hasdata-data-collection

Scrapes data via the [HasData](https://hasdata.com) API, inserts structured data to Postgres (via [Peewee ORM](https://docs.peewee-orm.com)), and keeps local JSON backups of every response. A `scraped_pages` table tracks which paginated URLs have already been fetched so reruns are safe and idempotent.

## Project layout

```
hasdata-data-collection/
├── pyproject.toml
├── .env                              # copy from .env.example and fill in
├── .env.example
└── src/
    ├── scrape_zillow.py              # Zillow listing search
    ├── scrape_zillow_properties.py   # Zillow property details (reads zillow_listings)
    ├── scrape_glassdoor_listings.py  # Glassdoor job listing search
    ├── scrape_glassdoor_jobs.py      # Glassdoor job details (reads glassdoor_listings)
    └── scraper/
        ├── __init__.py
        ├── config.py           # Settings loaded from .env
        ├── db.py               # DB init, dedup helpers, insert logic
        ├── endpoints.py        # EndpointConfig registry — add new APIs here
        ├── generic_scraper.py  # scrape_paginated / scrape_per_item
        ├── hasdata.py          # HasData API client with retry/backoff
        └── models.py           # Peewee ORM models
```

## Setup

```bash
# 1. Install (editable mode)
pip install -e .

# 2. Configure
cp .env.example .env
# edit .env — set HASDATA_API_KEY and POSTGRES_DSN
```

## Endpoints & usage examples

### 1. Zillow listing search → `zillow_listings`

Paginates through Zillow search results for a location and listing type.
Local backups written to `scraped_json/zillow_listings/`.

```python
from scraper.config import settings
from scraper.db import close_db, init_db
from scraper.endpoints import ENDPOINTS
from scraper.generic_scraper import scrape_paginated

base_params = {
    "keyword": "Champaign, IL",
    "type": "sold",   # "sold" | "forSale" | "forRent"
    "hide55plusCommunities": "true",
    "homeTypes[]": "house",
}

init_db(settings.postgres_dsn)
try:
    scrape_paginated(
        config=ENDPOINTS["zillow_listing"],
        base_params=base_params,
        skip_done=True,
        delay=1.0,
        page_label="champaign-il-sold",
    )
finally:
    close_db()
```

### 2. Zillow property details → `zillow_properties`

Fetches full property details for every URL already in `zillow_listings`.
Requires running the listing scraper first.
Local backups written to `scraped_json/zillow_properties/`.

```python
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
```

### 3. Glassdoor job listing search → `glassdoor_listings`

Paginates through Glassdoor job search results for a keyword and location.
Local backups written to `scraped_json/glassdoor_listings/`.

```python
from scraper.config import settings
from scraper.db import close_db, init_db
from scraper.endpoints import ENDPOINTS
from scraper.generic_scraper import scrape_paginated

base_params = {
    "keyword": "software engineer",
    "location": "United States",
    "sort": "recent",           # "recent" | "relevance"
    "domain": "www.glassdoor.com",
}

init_db(settings.postgres_dsn)
try:
    scrape_paginated(
        config=ENDPOINTS["glassdoor_listing"],
        base_params=base_params,
        skip_done=True,
        delay=1.0,
        page_label="software engineer @ United States",
    )
finally:
    close_db()
```

### 4. Glassdoor job details → `glassdoor_jobs`

Fetches full job details for every listing URL already in `glassdoor_listings`.
Requires running the Glassdoor listing scraper first.
Local backups written to `scraped_json/glassdoor_jobs/`.

```python
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
```

## Adding a new endpoint

1. Add an `EndpointConfig` entry to `ENDPOINTS` in `src/scraper/endpoints.py`.
2. Create an entry-point script in `src/` (copy one of the existing ones as a template).

Each endpoint automatically gets:
- Its own Postgres table (`item_id TEXT PK`, `url TEXT`, `raw_json JSONB`, `scraped_at TIMESTAMPTZ`)
- Its own backup subdirectory under `scraped_json/<backup_subdir>/`
- Idempotent inserts — existing rows are never overwritten

## Database tables

### `scraped_pages`

Tracks every HasData paginated URL that has been successfully fetched.

| Column           | Type        | Notes                              |
| ---------------- | ----------- | ---------------------------------- |
| `url`            | TEXT (UQ)   | Full HasData request URL           |
| `location`       | TEXT        |                                    |
| `listing_type`   | TEXT        |                                    |
| `page_number`    | INTEGER     |                                    |
| `property_count` | INTEGER     | Items inserted from this page      |
| `scraped_at`     | TIMESTAMPTZ |                                    |

### `zillow_listings`

One row per Zillow property. Existing rows are not overwritten on rerun.

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

### Generic endpoint tables (`zillow_properties`, `glassdoor_listings`, `glassdoor_jobs`, …)

All endpoints added through `EndpointConfig` share the same schema:

| Column      | Type        | Notes                              |
| ----------- | ----------- | ---------------------------------- |
| `item_id`   | TEXT PK     | Stable unique ID from the response |
| `url`       | TEXT        | Source URL that was fetched        |
| `raw_json`  | JSONB       | Full response payload              |
| `scraped_at`| TIMESTAMPTZ |                                    |

## Local backups

Every response is saved as a JSON file before any DB write:

```
scraped_json/
├── zillow_listings/
│   └── zillow_listing-champaign,-il-page-001.json
├── zillow_properties/
│   └── 346891592.json
├── glassdoor_listings/
│   └── glassdoor_listing-software-engineer-@-united-states-page-001.json
└── glassdoor_jobs/
    └── https:__www.glassdoor.com_job-listing_....json
```

On reruns, `scraped_pages` is used to skip already-processed paginated URLs.
Backups are still written for auditing/recovery, but scraping does not read
from local JSON cache.
