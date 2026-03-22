import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from rich.console import Console
from rich.progress import track

from .config import settings
from .db import get_connection, is_url_done, mark_url_done
from .hasdata import HasDataClient

console = Console()
log = logging.getLogger(__name__)


def _backup_path(url: str) -> Path:
    """Stable filename derived from the URL."""
    safe = url.replace("://", "_").replace("/", "_").replace("?", "_")[:180]
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return settings.backup_dir / f"{ts}_{safe}.json"


def run_pipeline(
    urls: Iterable[str],
    source: str = "generic",
    skip_done: bool = True,
    **hasdata_params,
) -> None:
    """
    Main entry point.

    Parameters
    ----------
    urls          : iterable of URLs to scrape
    source        : logical label stored in DB (e.g. "zillow_listing")
    skip_done     : skip URLs already present in scraped_urls
    hasdata_params: extra kwargs forwarded to HasDataClient.scrape()
    """
    url_list = list(urls)
    console.print(
        f"[bold]Starting pipeline[/bold] — {len(url_list)} URL(s), source=[cyan]{source}[/cyan]"
    )

    with get_connection() as conn, HasDataClient() as client:
        for url in track(url_list, description="Scraping…"):
            if skip_done and is_url_done(conn, url):
                console.print(f"  [dim]SKIP[/dim] {url}")
                continue

            try:
                payload = client.scrape(url, **hasdata_params)
            except Exception as exc:
                console.print(f"  [red]ERROR[/red] {url} — {exc}")
                log.exception("Scrape failed for %s", url)
                continue

            # 1. Write local JSON backup
            backup = _backup_path(url)
            backup.write_text(json.dumps(payload, indent=2))

            # 2. Persist to Postgres (URL + payload, atomically)
            mark_url_done(conn, url, source, payload)

            console.print(f"  [green]OK[/green] {url}")

    console.print("[bold green]Done.[/bold green]")
