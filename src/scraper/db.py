"""
Database helpers: connect, create tables, and upsert data.
All SQL-level work lives here so models.py stays clean.
"""

from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from .models import LogMissingField, ScrapedPage, ZillowListing, ZillowProperty, db, get_item_model

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
    db.create_tables([ScrapedPage, ZillowListing, ZillowProperty, LogMissingField], safe=True)
    # Add has_next_page column to existing scraped_pages tables that predate it
    db.execute_sql(
        "ALTER TABLE scraped_pages ADD COLUMN IF NOT EXISTS has_next_page BOOLEAN"
    )


def init_endpoint_table(table_name: str) -> None:
    """Create the table for *table_name* if it doesn't exist."""
    if table_name == "zillow_listings":
        db.create_tables([ZillowListing], safe=True)
        return
    if table_name == "zillow_properties":
        db.create_tables([ZillowProperty], safe=True)
        return
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
    has_next_page: bool = False,
) -> None:
    ScrapedPage.insert(
        url=url,
        location=location,
        listing_type=listing_type,
        page_number=page_number,
        property_count=property_count,
        has_next_page=has_next_page,
        scraped_at=datetime.now(timezone.utc),
    ).on_conflict_ignore().execute()


def get_page_has_next(url: str) -> bool | None:
    """
    Return the stored has_next_page value for a previously scraped URL.
    Returns None if the row doesn't exist or the column is NULL (legacy row).
    """
    row = ScrapedPage.get_or_none(ScrapedPage.url == url)
    if row is None:
        return None
    return row.has_next_page


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
#  ZillowProperty upsert (zillow_properties table)                            #
# --------------------------------------------------------------------------- #

# JSON keys that are explicitly mapped to columns in zillow_properties.
# Anything present in the response but absent from these sets is logged to
# log_missing_fields for later review.
_ZP_TOP_LEVEL_MAPPED: set[str] = {
    "id", "url", "image", "status", "trueStatus", "yearBuilt", "homeType",
    "beds", "baths", "area", "price", "lastSoldPrice", "currency", "zestimate",
    "address", "geo", "description", "parcelData", "listingSubTypes",
    "foreclosureJudicialType", "downPaymentAssistance", "resoData", "agentInfo",
    "datePosted", "daysOnZillow", "priceHistory", "taxHistory", "photos",
    "nearby", "schools", "staticMapUrls", "agentEmails",
}

_ZP_RESO_MAPPED: set[str] = {
    "attic", "stories", "storiesDecimal", "basement", "basementYN",
    "bedrooms", "bathrooms", "bathroomsFull", "bathroomsHalf", "bathroomsFloat",
    "homeType", "roofType", "furnished", "hasGarage", "hasAttachedGarage",
    "hasOpenParking", "garageParkingCapacity", "parkingCapacity",
    "coveredParkingCapacity", "ownership", "architecturalStyle",
    "hasCooling", "hasHeating", "hasSpa", "hasView", "hasFireplace",
    "fireplaces", "hasLandLease", "hasHomeWarranty", "isNewConstruction",
    "hasAttachedProperty", "hasAdditionalParcels", "canRaiseHorses",
    "highSchool", "elementarySchool", "middleOrJuniorSchool",
    "highSchoolDistrict", "elementarySchoolDistrict", "middleOrJuniorSchoolDistrict",
    "cityRegion", "taxAnnualAmount", "taxAssessedValue", "livingArea", "lotSize",
    "buildingArea", "belowGradeFinishedArea", "parcelNumber", "pricePerSquareFoot",
    "onMarketDate", "listingTerms", "specialListingConditions", "lotSizeDimensions",
    "subdivisionName", "rooms", "roomTypes", "appliances", "sewer", "cooling",
    "heating", "flooring", "waterSource", "feesAndDues", "atAGlanceFacts",
    "parkingFeatures", "laundryFeatures", "exteriorFeatures", "interiorFeatures",
    "communityFeatures", "fireplaceFeatures", "patioAndPorchFeatures",
    "propertySubType", "constructionMaterials", "accessibilityFeatures",
    "foundationDetails", "lotFeatures", "associationFeeIncludes",
    "securityFeatures", "electric", "associationAmenities",
}


def _ms_to_datetime(ms: int | None) -> datetime | None:
    """Convert a Unix timestamp in milliseconds to a timezone-aware datetime."""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _log_missing_fields(table: str, response: dict) -> None:
    """
    Compare *response* against the known-mapped key sets for *table* and
    insert a row in log_missing_fields for every unmapped key found.
    Duplicate entries are silently ignored (unique constraint on table+column).
    """
    if table != "zillow_properties":
        return  # only implemented for this table for now

    reso = response.get("resoData") or {}

    missing: list[str] = []
    for key in response:
        if key not in _ZP_TOP_LEVEL_MAPPED:
            missing.append(key)
    for key in reso:
        if key not in _ZP_RESO_MAPPED:
            missing.append(f"resoData.{key}")

    if not missing:
        return

    rows = [{"table_name": table, "missing_column": col} for col in missing]
    LogMissingField.insert_many(rows).on_conflict_ignore().execute()


def upsert_zillow_property(item_id: str, url: str, raw_json: dict) -> None:
    """Insert a Zillow property detail into zillow_properties with expanded columns."""
    p = raw_json
    geo = p.get("geo") or {}
    area = p.get("area") or {}
    address = p.get("address") or {}
    agent = p.get("agentInfo") or {}
    zest = p.get("zestimate") or {}
    parcel = p.get("parcelData") or {}
    reso = p.get("resoData") or {}

    (
        ZillowProperty.insert(
            property_id=int(item_id),
            url=p.get("url"),
            home_type=p.get("homeType"),
            status=p.get("status"),
            true_status=p.get("trueStatus"),
            date_posted=p.get("datePosted"),
            days_on_zillow=p.get("daysOnZillow"),
            price=p.get("price"),
            last_sold_price=p.get("lastSoldPrice"),
            currency=p.get("currency"),
            description=p.get("description"),
            beds=p.get("beds"),
            baths=p.get("baths"),
            year_built=p.get("yearBuilt"),
            image=p.get("image"),
            down_payment_assistance=p.get("downPaymentAssistance"),
            foreclosure_judicial_type=p.get("foreclosureJudicialType"),
            # Geo
            latitude=geo.get("latitude"),
            longitude=geo.get("longitude"),
            # Area
            lot_size=area.get("lotSize"),
            living_area=area.get("livingArea"),
            lot_area_value=area.get("lotAreaValue"),
            lot_area_units=area.get("lotAreaUnits"),
            lot_size_raw=area.get("lotSizeRaw"),
            living_area_raw=area.get("livingAreaRaw"),
            living_area_units=area.get("livingAreaUnits"),
            living_area_units_short=area.get("livingAreaUnitsShort"),
            # Address
            address_street=address.get("street"),
            address_city=address.get("city"),
            address_state=address.get("state"),
            address_zipcode=address.get("zipcode"),
            address_county=address.get("county"),
            address_country=address.get("country"),
            address_raw=address.get("addressRaw"),
            county_fips=address.get("countyFIPS"),
            address_undisclosed=address.get("undisclosed"),
            address_parent_region=address.get("parentRegion"),
            address_subdivision=address.get("subdivision"),
            # Agent info
            agent_name=agent.get("agentName"),
            broker_name=agent.get("brokerName"),
            buyer_agent_name=agent.get("buyerAgentName"),
            buyer_broker_name=agent.get("buyerBrokerName"),
            agent_phone_number=agent.get("agentPhoneNumber"),
            broker_phone_number=agent.get("brokerPhoneNumber"),
            # Zestimate
            zestimate=zest.get("zestimate"),
            rent_zestimate=zest.get("rentZestimate"),
            rent_zestimate_url=zest.get("rentZestimateURL"),
            zestimate_low_percent=zest.get("zestimateLowPercent"),
            zestimate_high_percent=zest.get("zestimateHighPercent"),
            # Parcel
            parcel_id=parcel.get("parcelId"),
            parcel_number=parcel.get("parcelNumber"),
            # resoData scalars
            reso_attic=reso.get("attic"),
            reso_stories=reso.get("stories"),
            reso_stories_decimal=reso.get("storiesDecimal"),
            reso_basement=reso.get("basement"),
            reso_basement_yn=reso.get("basementYN"),
            reso_bedrooms=reso.get("bedrooms"),
            reso_bathrooms=reso.get("bathrooms"),
            reso_bathrooms_full=reso.get("bathroomsFull"),
            reso_bathrooms_half=reso.get("bathroomsHalf"),
            reso_bathrooms_float=reso.get("bathroomsFloat"),
            reso_home_type=reso.get("homeType"),
            reso_roof_type=reso.get("roofType"),
            reso_furnished=reso.get("furnished"),
            reso_has_garage=reso.get("hasGarage"),
            reso_has_attached_garage=reso.get("hasAttachedGarage"),
            reso_has_open_parking=reso.get("hasOpenParking"),
            reso_garage_parking_capacity=reso.get("garageParkingCapacity"),
            reso_parking_capacity=reso.get("parkingCapacity"),
            reso_covered_parking_capacity=reso.get("coveredParkingCapacity"),
            reso_ownership=reso.get("ownership"),
            reso_architectural_style=reso.get("architecturalStyle"),
            reso_has_cooling=reso.get("hasCooling"),
            reso_has_heating=reso.get("hasHeating"),
            reso_has_spa=reso.get("hasSpa"),
            reso_has_view=reso.get("hasView"),
            reso_has_fireplace=reso.get("hasFireplace"),
            reso_fireplaces=reso.get("fireplaces"),
            reso_has_land_lease=reso.get("hasLandLease"),
            reso_has_home_warranty=reso.get("hasHomeWarranty"),
            reso_is_new_construction=reso.get("isNewConstruction"),
            reso_has_attached_property=reso.get("hasAttachedProperty"),
            reso_has_additional_parcels=reso.get("hasAdditionalParcels"),
            reso_can_raise_horses=reso.get("canRaiseHorses"),
            reso_high_school=reso.get("highSchool"),
            reso_elementary_school=reso.get("elementarySchool"),
            reso_middle_or_junior_school=reso.get("middleOrJuniorSchool"),
            reso_high_school_district=reso.get("highSchoolDistrict"),
            reso_elementary_school_district=reso.get("elementarySchoolDistrict"),
            reso_middle_or_junior_school_district=reso.get("middleOrJuniorSchoolDistrict"),
            reso_city_region=reso.get("cityRegion"),
            reso_tax_annual_amount=reso.get("taxAnnualAmount"),
            reso_tax_assessed_value=reso.get("taxAssessedValue"),
            reso_living_area=reso.get("livingArea"),
            reso_lot_size=reso.get("lotSize"),
            reso_building_area=reso.get("buildingArea"),
            reso_below_grade_finished_area=reso.get("belowGradeFinishedArea"),
            reso_parcel_number=reso.get("parcelNumber"),
            reso_price_per_square_foot=reso.get("pricePerSquareFoot"),
            reso_on_market_date=_ms_to_datetime(reso.get("onMarketDate")),
            reso_listing_terms=reso.get("listingTerms"),
            reso_special_listing_conditions=reso.get("specialListingConditions"),
            reso_lot_size_dimensions=reso.get("lotSizeDimensions"),
            reso_subdivision_name=reso.get("subdivisionName"),
            # Container / list fields
            photos=p.get("photos"),
            nearby=p.get("nearby"),
            schools=p.get("schools"),
            tax_history=p.get("taxHistory"),
            price_history=p.get("priceHistory"),
            static_map_urls=p.get("staticMapUrls"),
            listing_sub_types=p.get("listingSubTypes"),
            agent_emails=p.get("agentEmails"),
            # resoData containers
            reso_rooms=reso.get("rooms"),
            reso_room_types=reso.get("roomTypes"),
            reso_appliances=reso.get("appliances"),
            reso_sewer=reso.get("sewer"),
            reso_cooling=reso.get("cooling"),
            reso_heating=reso.get("heating"),
            reso_flooring=reso.get("flooring"),
            reso_water_source=reso.get("waterSource"),
            reso_fees_and_dues=reso.get("feesAndDues"),
            reso_at_a_glance_facts=reso.get("atAGlanceFacts"),
            reso_parking_features=reso.get("parkingFeatures"),
            reso_laundry_features=reso.get("laundryFeatures"),
            reso_exterior_features=reso.get("exteriorFeatures"),
            reso_interior_features=reso.get("interiorFeatures"),
            reso_community_features=reso.get("communityFeatures"),
            reso_fireplace_features=reso.get("fireplaceFeatures"),
            reso_patio_and_porch_features=reso.get("patioAndPorchFeatures"),
            reso_property_sub_type=reso.get("propertySubType"),
            reso_construction_materials=reso.get("constructionMaterials"),
            reso_accessibility_features=reso.get("accessibilityFeatures"),
            reso_foundation_details=reso.get("foundationDetails"),
            reso_lot_features=reso.get("lotFeatures"),
            reso_association_fee_includes=reso.get("associationFeeIncludes"),
            reso_security_features=reso.get("securityFeatures"),
            reso_electric=reso.get("electric"),
            reso_association_amenities=reso.get("associationAmenities"),
            # Full raw payload
            raw_json=p,
            scraped_at=datetime.now(timezone.utc),
        )
        .on_conflict_ignore()
        .execute()
    )

    _log_missing_fields("zillow_properties", p)


# --------------------------------------------------------------------------- #
#  Generic per-endpoint item helpers                                           #
# --------------------------------------------------------------------------- #


def is_item_scraped(table_name: str, item_id: str) -> bool:
    """Return True if *item_id* already exists in *table_name*."""
    if table_name == "zillow_properties":
        return ZillowProperty.select().where(ZillowProperty.property_id == int(item_id)).exists()
    model = get_item_model(table_name)
    return model.select().where(model.item_id == item_id).exists()


def upsert_item(table_name: str, item_id: str, url: str, raw_json: dict) -> None:
    """Insert a scraped item into *table_name*; silently skip if it already exists."""
    if table_name == "zillow_listings":
        address = raw_json.get("address") or {}
        now = datetime.now(timezone.utc)
        (
            ZillowListing.insert(
                property_id=int(item_id),
                url=url or raw_json.get("url"),
                home_type=raw_json.get("homeType"),
                status=raw_json.get("status"),
                listing_type=raw_json.get("listingType"),
                price=raw_json.get("price"),
                zestimate=raw_json.get("zestimate"),
                rent_zestimate=raw_json.get("rentZestimate"),
                beds=raw_json.get("beds"),
                baths=raw_json.get("baths"),
                area=raw_json.get("area"),
                days_on_zillow=raw_json.get("daysOnZillow"),
                address_raw=raw_json.get("addressRaw"),
                street=address.get("street"),
                city=address.get("city"),
                state=address.get("state"),
                zipcode=address.get("zipcode"),
                latitude=raw_json.get("latitude"),
                longitude=raw_json.get("longitude"),
                broker_name=raw_json.get("brokerName"),
                raw_json=raw_json,
                created_at=now,
                updated_at=now,
            )
            .on_conflict_ignore()
            .execute()
        )
        return
    if table_name == "zillow_properties":
        upsert_zillow_property(item_id, url, raw_json)
        return
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
