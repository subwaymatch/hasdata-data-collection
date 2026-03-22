"""
Peewee ORM models.

Two tables:
  - ScrapedPage   : tracks every HasData API URL that was successfully fetched
                    (dedup / resume key — one row per page URL)
  - ZillowListing : one row per property, upserted on property_id
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
