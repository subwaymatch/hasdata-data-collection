import importlib
import json
import os
from pathlib import Path

import pytest

os.environ.setdefault("HASDATA_API_KEY", "test-key")
os.environ.setdefault("POSTGRES_DSN", "postgresql://user:pass@localhost:5432/testdb")

from scraper.endpoints import ENDPOINTS


@pytest.fixture
def generic_scraper_module():
    return importlib.import_module("scraper.generic_scraper")


def test_page_backup_path_includes_location_and_listing_type(generic_scraper_module):
    config = ENDPOINTS["zillow_listing"]
    path = generic_scraper_module._page_backup_path(
        config=config,
        page=1,
        extra="champaign-il-sold-year-2024-beds-1-3",
    )
    assert path.name == "zillow_listing-champaign-il-sold-year-2024-beds-1-3-001.json"


def test_scrape_per_item_backup_uses_item_id_not_source_url(
    monkeypatch, tmp_path: Path, generic_scraper_module
):
    config = ENDPOINTS["zillow_property"]

    monkeypatch.setattr(
        generic_scraper_module.settings, "scraped_json_base_dir", tmp_path / "scraped_json"
    )
    monkeypatch.setattr(generic_scraper_module, "init_endpoint_table", lambda *_: None)
    monkeypatch.setattr(
        generic_scraper_module,
        "get_source_urls",
        lambda *_: ["https://www.zillow.com/homedetails/example/3224249_zpid/"],
    )
    monkeypatch.setattr(generic_scraper_module, "is_item_scraped", lambda *_: False)

    upsert_calls = []

    def fake_upsert(table_name, item_id, url, raw_json):
        upsert_calls.append((table_name, item_id, url, raw_json))

    monkeypatch.setattr(generic_scraper_module, "upsert_item", fake_upsert)
    monkeypatch.setattr(generic_scraper_module.time, "sleep", lambda *_: None)

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

        def fetch(self, *_):
            return (
                "https://api.hasdata.com/scrape/zillow/property?url=...",
                {"property": {"id": "3224249", "url": "https://www.zillow.com/homedetails/example/"}},
            )

    monkeypatch.setattr(generic_scraper_module, "HasDataClient", FakeClient)

    generic_scraper_module.scrape_per_item(config=config, skip_done=True, delay=0.0)

    backup_dir = tmp_path / "scraped_json" / "zillow_properties"
    expected_backup = backup_dir / "3224249.json"
    assert expected_backup.exists()
    assert not (backup_dir / "https.json").exists()

    data = json.loads(expected_backup.read_text())
    assert data["property"]["id"] == "3224249"
    assert upsert_calls and upsert_calls[0][1] == "3224249"
