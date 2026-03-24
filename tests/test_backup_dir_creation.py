import importlib
import json
import os
import shutil
import tempfile
from pathlib import Path

os.environ.setdefault("HASDATA_API_KEY", "test-key")
os.environ.setdefault("POSTGRES_DSN", "postgresql://user:pass@localhost:5432/testdb")

from scraper.endpoints import ENDPOINTS


def _import_generic_scraper():
    return importlib.import_module("scraper.generic_scraper")


def test_backup_dir_creates_missing_directory(monkeypatch):
    scraper = _import_generic_scraper()
    root = Path(tempfile.mkdtemp())
    backup_base = root / "missing" / "scraped_json"
    config = ENDPOINTS["zillow_listing"]

    monkeypatch.setattr(scraper.settings, "scraped_json_base_dir", backup_base)
    assert not backup_base.exists()

    result = scraper._backup_dir(config)

    expected = backup_base / config.backup_subdir
    assert result == expected
    assert result.exists()
    assert result.is_dir()

    shutil.rmtree(root, ignore_errors=True)


def test_scrape_paginated_ignores_existing_local_cache(monkeypatch, tmp_path: Path):
    scraper = _import_generic_scraper()
    config = ENDPOINTS["zillow_listing"]
    backup_base = tmp_path / "scraped_json"
    backup_dir = backup_base / config.backup_subdir
    backup_dir.mkdir(parents=True)

    # Pre-create a corrupt/stale backup file
    existing_file = backup_dir / "zillow_listing-page-001.json"
    existing_file.write_text("{ not valid json")

    monkeypatch.setattr(scraper.settings, "scraped_json_base_dir", backup_base)
    monkeypatch.setattr(scraper, "init_endpoint_table", lambda *_: None)
    monkeypatch.setattr(scraper, "is_page_done", lambda *_: False)
    monkeypatch.setattr(scraper, "mark_page_done", lambda **_: None)
    monkeypatch.setattr(scraper, "upsert_item", lambda *_: None)
    monkeypatch.setattr(scraper.time, "sleep", lambda *_: None)

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

        def fetch(self, *_):
            return (
                "https://api.hasdata.com/scrape/zillow/listing?page=1",
                {"properties": [{"id": 1, "url": "https://x/1"}], "pagination": {}},
            )

    monkeypatch.setattr(scraper, "HasDataClient", FakeClient)

    scraper.scrape_paginated(
        config=config,
        base_params={"keyword": "Urbana, IL", "type": "forSale"},
        skip_done=False,
        delay=0.0,
    )

    # Backup file should now contain valid JSON
    backup_files = list(backup_dir.glob("*.json"))
    assert backup_files
    data = json.loads(backup_files[0].read_text())
    assert "properties" in data

