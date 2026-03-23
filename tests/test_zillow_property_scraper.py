import os

os.environ.setdefault("HASDATA_API_KEY", "test-key")
os.environ.setdefault("POSTGRES_DSN", "postgresql://user:pass@localhost:5432/testdb")

from scraper.endpoints import ENDPOINTS
from scraper import generic_scraper


def test_zillow_property_id_extractor_uses_zpid():
    item = {"zpid": 65290913}
    assert ENDPOINTS["zillow_property"].id_extractor(item) == "65290913"


def test_scrape_per_item_extracts_zpid_and_upserts(monkeypatch):
    config = ENDPOINTS["zillow_property"]
    upserted = []

    monkeypatch.setattr(generic_scraper, "init_endpoint_table", lambda *_: None)
    monkeypatch.setattr(
        generic_scraper,
        "get_source_urls",
        lambda *_: ["https://www.zillow.com/homedetails/example/65290913_zpid/"],
    )
    monkeypatch.setattr(generic_scraper, "is_item_scraped", lambda *_: False)
    monkeypatch.setattr(generic_scraper.time, "sleep", lambda *_: None)
    monkeypatch.setattr(generic_scraper, "_item_backup_path", lambda *_: os.devnull)

    def fake_upsert(table_name, item_id, url, raw_json):
        upserted.append((table_name, item_id, url, raw_json))

    monkeypatch.setattr(generic_scraper, "upsert_item", fake_upsert)

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

        def fetch(self, *_):
            return "https://api.hasdata.com/scrape/zillow/property?url=...", {
                "property": {"zpid": 65290913, "url": "https://www.zillow.com/homedetails/example/65290913_zpid/"}
            }

    monkeypatch.setattr(generic_scraper, "HasDataClient", FakeClient)

    generic_scraper.scrape_per_item(config=config, skip_done=True, delay=0.0)

    assert len(upserted) == 1
    assert upserted[0][0] == "zillow_properties"
    assert upserted[0][1] == "65290913"
