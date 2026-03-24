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
    BooleanField,
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


class ZillowProperty(BaseModel):
    """
    One row per Zillow property detail page, with individual columns for
    all expected response fields.  Container/list fields are stored as JSONB.
    """

    # Primary key (Zillow zpid)
    property_id = BigIntegerField(primary_key=True)

    # Top-level scalar fields
    url = TextField(null=True)
    home_type = TextField(null=True)
    status = TextField(null=True)
    true_status = TextField(null=True)
    building_id = TextField(null=True)
    date_posted = DateTimeField(null=True)
    days_on_zillow = IntegerField(null=True)
    price = IntegerField(null=True)
    last_sold_price = IntegerField(null=True)
    currency = TextField(null=True)
    description = TextField(null=True)
    beds = IntegerField(null=True)
    baths = FloatField(null=True)
    year_built = IntegerField(null=True)
    image = TextField(null=True)
    down_payment_assistance = BooleanField(null=True)
    foreclosure_judicial_type = TextField(null=True)

    # Geo (from geo object)
    latitude = FloatField(null=True)
    longitude = FloatField(null=True)

    # Area (from area object)
    lot_size = IntegerField(null=True)
    living_area = IntegerField(null=True)
    lot_area_value = FloatField(null=True)
    lot_area_units = TextField(null=True)
    lot_size_raw = TextField(null=True)
    living_area_raw = TextField(null=True)
    living_area_units = TextField(null=True)
    living_area_units_short = TextField(null=True)

    # Address (from address object)
    address_street = TextField(null=True)
    address_city = TextField(null=True)
    address_state = TextField(null=True)
    address_zipcode = TextField(null=True)
    address_county = TextField(null=True)
    address_country = TextField(null=True)
    address_raw = TextField(null=True)
    county_fips = TextField(null=True)
    address_undisclosed = BooleanField(null=True)
    address_parent_region = TextField(null=True)
    address_subdivision = TextField(null=True)

    # Agent info (from agentInfo object)
    agent_name = TextField(null=True)
    broker_name = TextField(null=True)
    buyer_agent_name = TextField(null=True)
    buyer_broker_name = TextField(null=True)
    agent_phone_number = TextField(null=True)
    broker_phone_number = TextField(null=True)

    # Fees (from top-level fees object)
    fees_monthly_hoa_fee = TextField(null=True)

    # Zestimate (from zestimate object)
    zestimate = IntegerField(null=True)
    rent_zestimate = IntegerField(null=True)
    rent_zestimate_url = TextField(null=True)
    zestimate_low_percent = FloatField(null=True)
    zestimate_high_percent = FloatField(null=True)

    # Parcel (from parcelData object)
    parcel_id = TextField(null=True)
    parcel_number = TextField(null=True)

    # resoData scalar fields
    reso_attic = TextField(null=True)
    reso_stories = IntegerField(null=True)
    reso_stories_decimal = FloatField(null=True)
    reso_basement = TextField(null=True)
    reso_basement_yn = BooleanField(null=True)
    reso_bedrooms = IntegerField(null=True)
    reso_bathrooms = FloatField(null=True)
    reso_bathrooms_full = IntegerField(null=True)
    reso_bathrooms_half = IntegerField(null=True)
    reso_bathrooms_one_quarter = IntegerField(null=True)
    reso_bathrooms_three_quarter = IntegerField(null=True)
    reso_bathrooms_float = FloatField(null=True)
    reso_home_type = TextField(null=True)
    reso_roof_type = TextField(null=True)
    reso_furnished = BooleanField(null=True)
    reso_has_garage = BooleanField(null=True)
    reso_has_attached_garage = BooleanField(null=True)
    reso_has_open_parking = BooleanField(null=True)
    reso_garage_parking_capacity = IntegerField(null=True)
    reso_parking_capacity = IntegerField(null=True)
    reso_covered_parking_capacity = IntegerField(null=True)
    reso_ownership = TextField(null=True)
    reso_architectural_style = TextField(null=True)
    reso_has_cooling = BooleanField(null=True)
    reso_has_heating = BooleanField(null=True)
    reso_has_spa = BooleanField(null=True)
    reso_has_view = BooleanField(null=True)
    reso_has_fireplace = BooleanField(null=True)
    reso_fireplaces = IntegerField(null=True)
    reso_has_private_pool = BooleanField(null=True)
    reso_has_carport = BooleanField(null=True)
    reso_has_association = BooleanField(null=True)
    reso_has_land_lease = BooleanField(null=True)
    reso_has_home_warranty = BooleanField(null=True)
    reso_is_new_construction = BooleanField(null=True)
    reso_has_attached_property = BooleanField(null=True)
    reso_has_additional_parcels = BooleanField(null=True)
    reso_can_raise_horses = BooleanField(null=True)
    reso_high_school = TextField(null=True)
    reso_elementary_school = TextField(null=True)
    reso_middle_or_junior_school = TextField(null=True)
    reso_high_school_district = TextField(null=True)
    reso_elementary_school_district = TextField(null=True)
    reso_middle_or_junior_school_district = TextField(null=True)
    reso_city_region = TextField(null=True)
    reso_tax_annual_amount = FloatField(null=True)
    reso_tax_assessed_value = IntegerField(null=True)
    reso_living_area = TextField(null=True)
    reso_lot_size = TextField(null=True)
    reso_building_area = TextField(null=True)
    reso_above_grade_finished_area = TextField(null=True)
    reso_below_grade_finished_area = TextField(null=True)
    reso_parcel_number = TextField(null=True)
    reso_price_per_square_foot = IntegerField(null=True)
    reso_on_market_date = DateTimeField(null=True)
    reso_listing_terms = TextField(null=True)
    reso_special_listing_conditions = TextField(null=True)
    reso_lot_size_dimensions = TextField(null=True)
    reso_subdivision_name = TextField(null=True)
    reso_building_name = TextField(null=True)
    reso_association_fee = TextField(null=True)
    reso_hoa_fee = TextField(null=True)
    reso_hoa_fee_total = TextField(null=True)
    reso_water_view_yn = BooleanField(null=True)
    reso_levels = TextField(null=True)
    reso_structure_type = TextField(null=True)
    reso_stories_total = IntegerField(null=True)
    reso_main_level_bathrooms = IntegerField(null=True)
    reso_main_level_bedrooms = IntegerField(null=True)
    reso_common_walls = TextField(null=True)
    reso_fencing = TextField(null=True)
    reso_municipality = TextField(null=True)
    reso_zoning = TextField(null=True)
    reso_list_aor = TextField(null=True)
    reso_entry_location = TextField(null=True)

    # Container / list fields stored as JSONB
    photos = BinaryJSONField(null=True)
    nearby = BinaryJSONField(null=True)
    schools = BinaryJSONField(null=True)
    tax_history = BinaryJSONField(null=True)
    price_history = BinaryJSONField(null=True)
    static_map_urls = BinaryJSONField(null=True)
    listing_sub_types = BinaryJSONField(null=True)
    agent_emails = BinaryJSONField(null=True)

    # resoData container fields
    reso_rooms = BinaryJSONField(null=True)
    reso_room_types = BinaryJSONField(null=True)
    reso_appliances = BinaryJSONField(null=True)
    reso_sewer = BinaryJSONField(null=True)
    reso_cooling = BinaryJSONField(null=True)
    reso_heating = BinaryJSONField(null=True)
    reso_flooring = BinaryJSONField(null=True)
    reso_water_source = BinaryJSONField(null=True)
    reso_fees_and_dues = BinaryJSONField(null=True)
    reso_at_a_glance_facts = BinaryJSONField(null=True)
    reso_parking_features = BinaryJSONField(null=True)
    reso_laundry_features = BinaryJSONField(null=True)
    reso_exterior_features = BinaryJSONField(null=True)
    reso_interior_features = BinaryJSONField(null=True)
    reso_community_features = BinaryJSONField(null=True)
    reso_fireplace_features = BinaryJSONField(null=True)
    reso_patio_and_porch_features = BinaryJSONField(null=True)
    reso_property_sub_type = BinaryJSONField(null=True)
    reso_construction_materials = BinaryJSONField(null=True)
    reso_accessibility_features = BinaryJSONField(null=True)
    reso_foundation_details = BinaryJSONField(null=True)
    reso_lot_features = BinaryJSONField(null=True)
    reso_association_fee_includes = BinaryJSONField(null=True)
    reso_associations = BinaryJSONField(null=True)
    reso_association_amenities = BinaryJSONField(null=True)
    reso_security_features = BinaryJSONField(null=True)
    reso_electric = BinaryJSONField(null=True)
    reso_view = BinaryJSONField(null=True)
    reso_pool_features = BinaryJSONField(null=True)
    reso_media = BinaryJSONField(null=True)
    reso_other_structures = BinaryJSONField(null=True)

    # Full raw response payload
    raw_json = BinaryJSONField(null=True)

    scraped_at = DateTimeField(default=lambda: datetime.now(timezone.utc))

    class Meta:
        table_name = "zillow_properties"


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
