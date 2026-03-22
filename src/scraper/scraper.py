"""
Core scraping pipeline.

Reproduces the logic from your original notebook's scrape() function,
with added features:
  - Skip pages already tracked in scraped_pages (resume support)
  - Load from local JSON cache if the file exists
  - Save JSON backup before touching the database
  - Upsert properties via Peewee
"""

import json
import os
import re
import time

from rich.console import Console

from .config import settings
from .db import is_page_done, mark_page_done, upsert_properties
from .hasdata import HasDataClient

console = Console()


# --------------------------------------------------------------------------- #
#  Filename helpers (same logic as your notebook)                              #
# --------------------------------------------------------------------------- #

def _slugify(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def _get_backup_path(location: str, listing_type: str, page: int) -> str:
    location_slug = _slugify(location)
    filename = f"zillow-listings-{location_slug}-{listing_type}-page-{page:02d}.json"
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    return os.path.join(settings.backup_dir, filename)


# --------------------------------------------------------------------------- #
#  Main loop                                                                   #
# --------------------------------------------------------------------------- #

def scrape(
    location: str,
    listing_type: str,
    skip_done: bool = True,
    hide_55_plus: bool = True,
    delay: float = 1.0,
) -> None:
    """
    Paginate through all Zillow listing results for a given location
    and listing type.

    Parameters
    ----------
    location      : search keyword, e.g. "Champaign, IL"
    listing_type  : "sold", "forSale", etc.
    skip_done     : skip pages whose URL is already in scraped_pages
    hide_55_plus  : pass hide55plusCommunities=true to HasData
    delay         : polite sleep between live API calls (seconds)
    """
    page = 1

    with HasDataClient() as client:
        while True:
            backup_path = _get_backup_path(location, listing_type, page)

            # ---------------------------------------------------------------- #
            # 1. Load from local cache if available                            #
            # ---------------------------------------------------------------- #
            if os.path.exists(backup_path):
                console.print(f"  [dim]CACHE[/dim]  page {page} → {backup_path}")
                with open(backup_path) as f:
                    data = json.load(f)
                # Reconstruct the URL for dedup tracking
                from urllib.parse import urlencode
                params = {
                    "keyword": location,
                    "type": listing_type,
                    "hide55plusCommunities": str(hide_55_plus).lower(),
                    "page": page,
                }
                url = f"{settings.hasdata_base_url}?{urlencode(params)}"

            else:
                # ----------------------------------------------------------  #
                # 2. Skip if already in DB (resume after interruption)        #
                # ----------------------------------------------------------  #
                # We need to know the URL before checking; build it manually.
                from urllib.parse import urlencode
                params = {
                    "keyword": location,
                    "type": listing_type,
                    "hide55plusCommunities": str(hide_55_plus).lower(),
                    "page": page,
                }
                url = f"{settings.hasdata_base_url}?{urlencode(params)}"

                if skip_done and is_page_done(url):
                    console.print(f"  [dim]SKIP[/dim]   page {page} (already in DB)")
                    # We still need to advance pagination — peek at the backup
                    # or just increment; no backup means we must fetch.
                    page += 1
                    continue

                # ----------------------------------------------------------  #
                # 3. Fetch from HasData                                        #
                # ----------------------------------------------------------  #
                console.print(f"  [cyan]FETCH[/cyan]  page {page} → {url}")
                url, data = client.fetch_listings_page(
                    location, listing_type, page, hide_55_plus
                )

                # Save JSON backup immediately after a successful fetch
                with open(backup_path, "w") as f:
                    json.dump(data, f, indent=2)
                console.print(f"           saved → {backup_path}")

                time.sleep(delay)

            # ---------------------------------------------------------------- #
            # 4. Process properties                                             #
            # ---------------------------------------------------------------- #
            properties = data.get("properties", [])

            if not properties:
                console.print(f"  [yellow]EMPTY[/yellow]  page {page} — stopping.")
                break

            count = upsert_properties(properties, listing_type)
            console.print(f"  [green]OK[/green]     page {page} — {count} properties upserted")

            # Mark page as done in DB
            mark_page_done(
                url=url,
                location=location,
                listing_type=listing_type,
                page_number=page,
                property_count=count,
            )

            # ---------------------------------------------------------------- #
            # 5. Advance to next page                                           #
            # ---------------------------------------------------------------- #
            pagination = data.get("pagination", {})
            if not pagination.get("nextPage"):
                console.print("  [bold green]Finished pagination.[/bold green]")
                break

            page += 1
