"""
Database helpers: connect, create tables, and upsert data.
All SQL-level work lives here so models.py stays clean.
"""

from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from .models import ScrapedPage, ZillowListing, db, get_item_model

# --------------------------------------------------------------------------- #
#  Connection                                                                  #
# --------------------------------------------------------------------------- #


def init_db(postgres_dsn: str) -> None:
    """
    Parse the DSN and initialise the lazy PostgresqlDatabase, then
    create tables if they don't exist yet.
    """
    p = urlparse(postgres_dsn)

    # psycopg2 connect kwargs expected by Peewee
    connect_kwargs: dict[str, Any] = {"sslmode": "require"}

    # Forward any query-string params (e.g. sslrootcert=system)
    if p.query:
        for part in p.query.split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                connect_kwargs[k] = v

    db.init(
        p.path.lstrip("/"),
        host=p.hostname,
        port=p.port or 5432,
        user=p.username,
        password=p.password,
        **connect_kwargs,
    )
    db.connect(reuse_if_open=True)
    db.create_tables([ScrapedPage, ZillowListing], safe=True)


def init_endpoint_table(table_name: str) -> None:
    """Create the generic scraped-item table for *table_name* if it doesn't exist."""
    model = get_item_model(table_name)
    db.create_tables([model], safe=True)


def close_db() -> None:
    if not db.is_closed():
        db.close()


# --------------------------------------------------------------------------- #
#  ScrapedPage helpers                                                         #
# --------------------------------------------------------------------------- #


def is_page_done(url: str) -> bool:
    """Return True if this HasData page URL has already been processed."""
    return ScrapedPage.select().where(ScrapedPage.url == url).exists()


def mark_page_done(
    url: str,
    location: str,
    listing_type: str,
    page_number: int,
    property_count: int,
) -> None:
    ScrapedPage.insert(
        url=url,
        location=location,
        listing_type=listing_type,
        page_number=page_number,
        property_count=property_count,
        scraped_at=datetime.now(timezone.utc),
    ).on_conflict_ignore().execute()


# --------------------------------------------------------------------------- #
#  ZillowListing upsert                                                        #
# --------------------------------------------------------------------------- #


def upsert_properties(properties: list[dict], listing_type: str) -> int:
    """
    Upsert a batch of property dicts into zillow_listings.
    Returns the number of rows affected.
    """
    if not properties:
        return 0

    now = datetime.now(timezone.utc)
    rows = []

    for p in properties:
        address = p.get("address") or {}
        rows.append(
            {
                "property_id": int(p["id"]),
                "url": p.get("url"),
                "home_type": p.get("homeType"),
                "status": p.get("status"),
                "listing_type": listing_type,
                "price": p.get("price"),
                "zestimate": p.get("zestimate"),
                "rent_zestimate": p.get("rentZestimate"),
                "beds": p.get("beds"),
                "baths": p.get("baths"),
                "area": p.get("area"),
                "days_on_zillow": p.get("daysOnZillow"),
                "address_raw": p.get("addressRaw"),
                "street": address.get("street"),
                "city": address.get("city"),
                "state": address.get("state"),
                "zipcode": address.get("zipcode"),
                "latitude": p.get("latitude"),
                "longitude": p.get("longitude"),
                "broker_name": p.get("brokerName"),
                "raw_json": p,
                "created_at": now,
                "updated_at": now,
            }
        )

    # Bulk insert — skip rows whose property_id already exists
    (ZillowListing.insert_many(rows).on_conflict_ignore().execute())

    return len(rows)


# --------------------------------------------------------------------------- #
#  Generic per-endpoint item helpers                                           #
# --------------------------------------------------------------------------- #


def is_item_scraped(table_name: str, item_id: str) -> bool:
    """Return True if *item_id* already exists in *table_name*."""
    model = get_item_model(table_name)
    return model.select().where(model.item_id == item_id).exists()


def upsert_item(table_name: str, item_id: str, url: str, raw_json: dict) -> None:
    """Insert a scraped item into *table_name*; silently skip if it already exists."""
    model = get_item_model(table_name)
    (
        model.insert(item_id=item_id, url=url, raw_json=raw_json, scraped_at=datetime.now(timezone.utc))
        .on_conflict_ignore()
        .execute()
    )


def get_source_urls(source_table: str, url_column: str) -> list[str]:
    """
    Return all non-null values of *url_column* from *source_table*.

    Used by per-item scrapers to discover which URLs to fetch next.
    """
    cursor = db.execute_sql(
        f'SELECT "{url_column}" FROM "{source_table}" WHERE "{url_column}" IS NOT NULL'
    )
    return [row[0] for row in cursor.fetchall()]
