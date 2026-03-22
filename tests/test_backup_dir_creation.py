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
