"""
Peewee ORM models.

Tables:
  - ScrapedPage   : tracks every HasData API URL that was successfully fetched
                    (dedup / resume key — one row per page URL)
  - ZillowListing : one row per Zillow property, upserted on property_id
  - ScrapedItem   : generic table used by all other endpoints; one table per
                    endpoint, created dynamically via get_item_model().
"""

from datetime import datetime, timezone

from peewee import (
    BigIntegerField,
    DateTimeField,
    FloatField,
    IntegerField,
    Model,
    PostgresqlDatabase,
    TextField,
)
from playhouse.postgres_ext import BinaryJSONField

# Lazy database — configured in db.init_db()
db = PostgresqlDatabase(None)

# Cache so we don't recreate the same model class twice
_item_model_cache: dict[str, type] = {}


class BaseModel(Model):
    class Meta:
        database = db


class ScrapedPage(BaseModel):
    """
    Tracks every HasData listing-search URL that has been successfully
    fetched and processed.  Used to skip pages on reruns.
    """

    url = TextField(unique=True)  # full HasData request URL — the dedup key
    location = TextField()        # e.g. "Champaign, IL"
    listing_type = TextField()    # e.g. "sold", "forSale"
    page_number = IntegerField()
    property_count = IntegerField(default=0)  # how many properties were on the page
    scraped_at = DateTimeField(default=lambda: datetime.now(timezone.utc))

    class Meta:
        table_name = "scraped_pages"


class ZillowListing(BaseModel):
    """
    One row per Zillow property.  Upserted on property_id so re-runs
    refresh price and raw JSON without duplicating rows.
    """

    property_id = BigIntegerField(primary_key=True)  # Zillow zpid
    url = TextField(null=True)
    home_type = TextField(null=True)
    status = TextField(null=True)        # e.g. "SOLD", "FOR_SALE"
    listing_type = TextField(null=True)  # parameter used in the search

    # Price / estimates
    price = IntegerField(null=True)
    zestimate = IntegerField(null=True)
    rent_zestimate = IntegerField(null=True)

    # Property details
    beds = IntegerField(null=True)
    baths = FloatField(null=True)
    area = IntegerField(null=True)
    days_on_zillow = IntegerField(null=True)

    # Address
    address_raw = TextField(null=True)
    street = TextField(null=True)
    city = TextField(null=True)
    state = TextField(null=True)
    zipcode = TextField(null=True)

    # Geo
    latitude = FloatField(null=True)
    longitude = FloatField(null=True)

    # Broker
    broker_name = TextField(null=True)

    # Full raw payload for forward-compatibility
    raw_json = BinaryJSONField(null=True)

    created_at = DateTimeField(default=lambda: datetime.now(timezone.utc))
    updated_at = DateTimeField(default=lambda: datetime.now(timezone.utc))

    class Meta:
        table_name = "zillow_listings"


# ---------------------------------------------------------------------------
# Generic per-endpoint item model (one table per endpoint)
# ---------------------------------------------------------------------------


def get_item_model(table_name: str) -> type:
    """
    Return (and cache) a Peewee model class bound to *table_name*.

    Schema
    ------
    item_id    TEXT  PRIMARY KEY  — stable unique ID extracted from the response
    url        TEXT  NULLABLE     — the source URL that was fetched
    raw_json   JSONB NULLABLE     — full response payload for the item
    scraped_at TIMESTAMPTZ        — when this row was written
    """
    if table_name in _item_model_cache:
        return _item_model_cache[table_name]

    class ScrapedItem(BaseModel):
        item_id = TextField(primary_key=True)
        url = TextField(null=True)
        raw_json = BinaryJSONField(null=True)
        scraped_at = DateTimeField()

        class Meta:
            pass  # table_name set dynamically below

    # Peewee reads table_name from Meta at class-creation time, so we patch it.
    ScrapedItem._meta.table_name = table_name
    ScrapedItem.__name__ = f"ScrapedItem_{table_name}"

    _item_model_cache[table_name] = ScrapedItem
    return ScrapedItem
