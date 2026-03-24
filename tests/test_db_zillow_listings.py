from datetime import datetime

from scraper import db as db_module


def test_init_endpoint_table_uses_zillow_listing_model(monkeypatch):
    created = {}

    def fake_create_tables(models, safe):
        created["models"] = models
        created["safe"] = safe

    monkeypatch.setattr(db_module.db, "create_tables", fake_create_tables)

    db_module.init_endpoint_table("zillow_listings")

    assert created["models"] == [db_module.ZillowListing]
    assert created["safe"] is True


def test_upsert_item_zillow_listings_writes_zillow_listing_row(monkeypatch):
    payload = {
        "id": "3224249",
        "url": "https://www.zillow.com/homedetails/401-W-Bradley-Ave-Champaign-IL-61820/3224249_zpid/",
        "homeType": "SINGLE_FAMILY",
        "status": "SOLD",
        "price": 100000,
        "zestimate": 101600,
        "rentZestimate": 1484,
        "beds": 4,
        "baths": 2,
        "area": 1200,
        "daysOnZillow": 50,
        "addressRaw": "401 W Bradley Ave, Champaign, IL 61820",
        "address": {
            "street": "401 W Bradley Ave",
            "city": "Champaign",
            "state": "IL",
            "zipcode": "61820",
        },
        "latitude": 40.126953,
        "longitude": -88.24856,
        "brokerName": "Coldwell Banker R.E. Group",
    }

    captured = {}

    class _InsertQuery:
        def on_conflict_ignore(self):
            return self

        def execute(self):
            captured["executed"] = True

    def fake_insert(**kwargs):
        captured["kwargs"] = kwargs
        return _InsertQuery()

    monkeypatch.setattr(db_module.ZillowListing, "insert", fake_insert)

    db_module.upsert_item("zillow_listings", payload["id"], payload["url"], payload)

    assert captured["executed"] is True
    assert captured["kwargs"]["property_id"] == int(payload["id"])
    assert captured["kwargs"]["url"] == payload["url"]
    assert captured["kwargs"]["city"] == "Champaign"
    assert captured["kwargs"]["raw_json"] == payload
    assert captured["kwargs"]["created_at"] <= datetime.now(captured["kwargs"]["created_at"].tzinfo)
    assert captured["kwargs"]["updated_at"] <= datetime.now(captured["kwargs"]["updated_at"].tzinfo)
