"""
Generic scraping pipeline for any registered HasData endpoint.

Two modes:

  scrape_paginated(config, base_params, ...)
      Iterates through pages of a listing-style endpoint until results are
      exhausted.  Each page's items are upserted into the target table and
      backed up as a JSON file under scraped_json/<backup_subdir>/.

  scrape_per_item(config, ...)
      Reads source URLs from the DB (config.source_table / source_url_column),
      fetches each one from HasData, and upserts the result into the target
      table.  A local JSON backup is written for each item.
"""

import hashlib
import json
import time
from pathlib import Path
from urllib.parse import urlencode

from rich.console import Console

from .config import settings
from .db import (
    get_page_has_next,
    get_source_urls,
    init_endpoint_table,
    is_item_scraped,
    is_page_done,
    mark_page_done,
    upsert_item,
)
from .endpoints import EndpointConfig
from .hasdata import HasDataClient

console = Console()


# --------------------------------------------------------------------------- #
#  Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _backup_dir(config: EndpointConfig) -> Path:
    d = settings.scraped_json_base_dir / config.backup_subdir
    d.mkdir(parents=True, exist_ok=True)
    return d


def _page_backup_path(config: EndpointConfig, page: int, extra: str = "") -> Path:
    slug = extra.replace(" ", "-").lower() if extra else "page"
    filename = f"{config.name}-{slug}-{page:03d}.json"
    return _backup_dir(config) / filename


def _item_backup_path(config: EndpointConfig, item_id: str) -> Path:
    # Sanitise item_id so it's safe as a filename
    safe_id = item_id.replace("/", "_").replace("?", "_").replace("&", "_")[:180]
    if not safe_id:
        safe_id = hashlib.md5(item_id.encode()).hexdigest()
    return _backup_dir(config) / f"{safe_id}.json"


def _extract_items(config: EndpointConfig, data: dict) -> list[dict]:
    """
    Pull the list of items from an API response using config.results_key.

    Handles both list responses (e.g. {"jobs": [...]}) and single-object
    responses (e.g. {"property": {...}}).  Single objects are wrapped in a
    list so callers always see a list.
    """
    value = data.get(config.results_key)
    if value is None:
        return []
    if isinstance(value, list):
        return value
    # Single-object response — wrap it
    return [value]


def _has_next_page(pagination: dict, current_page: int) -> bool:
    """
    Detect whether there is a next page regardless of the pagination format.

    Supports:
      - Zillow style:    {"nextPage": "..."}
      - Glassdoor style: {"otherPages": {"2": "...", "3": "..."}}
    """
    if pagination.get("nextPage"):
        return True
    other = pagination.get("otherPages") or {}
    return str(current_page + 1) in other


# --------------------------------------------------------------------------- #
#  Paginated endpoints                                                         #
# --------------------------------------------------------------------------- #


def scrape_paginated(
    config: EndpointConfig,
    base_params: dict,
    skip_done: bool = True,
    delay: float = 1.0,
    page_label: str = "",
) -> None:
    """
    Paginate through a listing-style HasData endpoint and upsert all items.

    Parameters
    ----------
    config      : registered EndpointConfig with paginated=True
    base_params : query parameters dict (without page number)
    skip_done   : skip pages already recorded in scraped_pages
    delay       : polite sleep between live API calls (seconds)
    page_label  : optional human-readable label for progress messages
    """
    init_endpoint_table(config.table_name)

    page = 1
    label = page_label or config.name

    with HasDataClient() as client:
        while True:
            params = {**base_params, "page": page}
            request_url = (
                f"https://api.hasdata.com{config.api_path}?{urlencode(params)}"
            )
            backup_path = _page_backup_path(config, page, page_label)

            # ---------------------------------------------------------------- #
            # 1. Skip if already recorded in DB (resume after crash)           #
            # ---------------------------------------------------------------- #
            if skip_done and is_page_done(request_url):
                console.print(
                    f"  [dim]SKIP[/dim]   [{label}] page {page} (already in DB)"
                )
                # has_next_page=None means a legacy row recorded before this
                # column existed — fall through and keep paginating to be safe.
                if get_page_has_next(request_url) is False:
                    console.print(
                        f"  [bold green]Finished pagination for [{label}].[/bold green]"
                    )
                    break
                page += 1
                continue

            # ---------------------------------------------------------------- #
            # 2. Fetch from HasData                                             #
            # ---------------------------------------------------------------- #
            console.print(f"  [cyan]FETCH[/cyan]  [{label}] page {page} → {request_url}")
            request_url, data = client.fetch(config.api_path, params)

            with open(backup_path, "w") as f:
                json.dump(data, f, indent=2)
            console.print(f"           saved → {backup_path}")

            time.sleep(delay)

            # ---------------------------------------------------------------- #
            # 3. Upsert items                                                   #
            # ---------------------------------------------------------------- #
            items = _extract_items(config, data)
            if not items:
                console.print(
                    f"  [yellow]EMPTY[/yellow]  [{label}] page {page} — stopping."
                )
                break

            for item in items:
                item_id = config.id_extractor(item)
                upsert_item(config.table_name, item_id, item.get("url"), item)

            console.print(
                f"  [green]OK[/green]     [{label}] page {page}"
                f" — {len(items)} items upserted"
            )

            # ---------------------------------------------------------------- #
            # 4. Advance pagination                                             #
            # ---------------------------------------------------------------- #
            pagination = data.get("pagination", {})
            next_page_exists = _has_next_page(pagination, page)

            mark_page_done(
                url=request_url,
                location=page_label,
                listing_type=config.name,
                page_number=page,
                property_count=len(items),
                has_next_page=next_page_exists,
            )

            if not next_page_exists:
                console.print(
                    f"  [bold green]Finished pagination for [{label}].[/bold green]"
                )
                break

            page += 1


# --------------------------------------------------------------------------- #
#  Per-item endpoints                                                          #
# --------------------------------------------------------------------------- #


def scrape_per_item(
    config: EndpointConfig,
    skip_done: bool = True,
    delay: float = 1.0,
) -> None:
    """
    Fetch detail pages for every URL found in config.source_table.

    Parameters
    ----------
    config    : registered EndpointConfig with paginated=False
    skip_done : skip items whose item_id is already in the target table
    delay     : polite sleep between live API calls (seconds)
    """
    if config.paginated:
        raise ValueError(
            f"scrape_per_item() called with a paginated endpoint '{config.name}'. "
            "Use scrape_paginated() instead."
        )

    init_endpoint_table(config.table_name)

    source_urls = get_source_urls(config.source_table, config.source_url_column)
    console.print(
        f"[bold]scrape_per_item[/bold] [{config.name}]"
        f" — {len(source_urls)} source URL(s) from '{config.source_table}'"
    )

    with HasDataClient() as client:
        for source_url in source_urls:
            params = {config.source_url_param: source_url}

            # ---------------------------------------------------------------- #
            # 1. Skip if already in target table (URL-keyed endpoints only)   #
            # ---------------------------------------------------------------- #
            # We can only check the DB early when item_id == source_url, i.e.
            # when id_extractor simply returns item["url"].  For numeric-ID
            # endpoints (e.g. Zillow property), the zpid is only known after
            # fetching.
            if skip_done:
                try:
                    prospective_id = config.id_extractor({"url": source_url})
                except (KeyError, TypeError):
                    prospective_id = None
                if prospective_id and prospective_id != "None" and is_item_scraped(
                    config.table_name, prospective_id
                ):
                    console.print(f"  [dim]SKIP[/dim]   (DB) {prospective_id}")
                    continue

            # ---------------------------------------------------------------- #
            # 2. Fetch from HasData                                             #
            # ---------------------------------------------------------------- #
            console.print(f"  [cyan]FETCH[/cyan]  {source_url}")
            try:
                request_url, data = client.fetch(config.api_path, params)
            except Exception as exc:
                console.print(f"  [red]ERROR[/red]  {source_url} — {exc}")
                continue

            items = _extract_items(config, data)
            if not items:
                console.print(f"  [yellow]EMPTY[/yellow] {source_url} — no data in response")
                continue

            item = items[0]
            item_id = config.id_extractor(item)
            backup_path = _item_backup_path(config, item_id)

            # Save backup before touching DB
            with open(backup_path, "w") as f:
                json.dump(data, f, indent=2)

            upsert_item(config.table_name, item_id, source_url, item)
            console.print(f"  [green]OK[/green]     {item_id}")

            time.sleep(delay)
