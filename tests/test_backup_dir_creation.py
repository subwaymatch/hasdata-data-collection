import shutil
import tempfile
import os
import sys
import types
import importlib
from pathlib import Path

os.environ.setdefault("HASDATA_API_KEY", "test-key")
os.environ.setdefault("POSTGRES_DSN", "postgresql://user:pass@localhost:5432/testdb")


def _import_scraper_module():
    fake_models = types.ModuleType("scraper.models")
    fake_models.ScrapedPage = object
    fake_models.ZillowListing = object
    fake_models.db = object()
    fake_models.get_item_model = lambda table_name: object
    sys.modules.setdefault("scraper.models", fake_models)
    return importlib.import_module("scraper.scraper")


def test_get_backup_path_creates_backup_directory(monkeypatch):
    scraper = _import_scraper_module()
    root = Path(tempfile.mkdtemp())
    backup_dir = root / "missing" / "json_backups"

    monkeypatch.setattr(scraper.settings, "backup_dir", backup_dir)
    assert not backup_dir.exists()

    backup_path = scraper._get_backup_path("Urbana, IL", "forSale", 1)

    assert backup_dir.exists()
    assert backup_dir.is_dir()
    assert str(backup_path).endswith(".json")

    shutil.rmtree(root, ignore_errors=True)


def test_scrape_ignores_existing_local_cache(monkeypatch):
    scraper = _import_scraper_module()
    root = Path(tempfile.mkdtemp())
    backup_file = root / "existing.json"
    backup_file.write_text("{ not valid json")

    monkeypatch.setattr(scraper, "_get_backup_path", lambda *_: str(backup_file))
    monkeypatch.setattr(scraper, "is_page_done", lambda *_: False)
    monkeypatch.setattr(scraper, "upsert_properties", lambda properties, _: len(properties))
    monkeypatch.setattr(scraper, "mark_page_done", lambda **_: None)
    monkeypatch.setattr(scraper.time, "sleep", lambda *_: None)

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

        def fetch_listings_page(self, *_):
            return (
                "https://api.hasdata.com/scrape/zillow/listing?page=1",
                {"properties": [{"id": 1, "url": "https://x/1"}], "pagination": {}},
            )

    monkeypatch.setattr(scraper, "HasDataClient", FakeClient)

    scraper.scrape(location="Urbana, IL", listing_type="forSale", skip_done=True, delay=0.0)
    assert backup_file.read_text()

    shutil.rmtree(root, ignore_errors=True)
