"""
CLI entry point.

Usage:
    zillow-scraper init-db
    zillow-scraper scrape
    zillow-scraper scrape --location "Urbana, IL" --type forSale
    zillow-scraper scrape --force
"""

import typer
from rich.console import Console

from .config import settings
from .db import init_db, close_db
from .scraper import scrape as run_scrape

app = typer.Typer(help="Zillow scraper via HasData → Postgres + local JSON backups")
console = Console()


def _connect():
    init_db(settings.postgres_dsn)


@app.command()
def init_db_cmd():
    """Create Postgres tables (safe to run multiple times)."""
    _connect()
    console.print("[green]Schema applied.[/green]")
    close_db()


@app.command()
def scrape(
    location: str = typer.Option(
        None, "--location", "-l", help="Search keyword, e.g. 'Champaign, IL'"
    ),
    listing_type: str = typer.Option(
        None, "--type", "-t", help="Listing type: sold, forSale, etc."
    ),
    force: bool = typer.Option(
        False, "--force", "-f", help="Re-fetch pages already recorded in the DB"
    ),
    delay: float = typer.Option(
        1.0, "--delay", "-d", help="Seconds to sleep between API calls"
    ),
    no_55_plus: bool = typer.Option(
        False, "--include-55-plus", help="Include 55+ communities (excluded by default)"
    ),
):
    """Paginate through Zillow listings and store results."""
    loc = location or settings.default_location
    lt = listing_type or settings.default_listing_type

    console.print(
        f"[bold]Scraping[/bold] [cyan]{loc}[/cyan] "
        f"([yellow]{lt}[/yellow]) "
        f"— skip_done=[{'red' if force else 'green'}]{not force}[/]"
    )

    _connect()
    try:
        run_scrape(
            location=loc,
            listing_type=lt,
            skip_done=not force,
            hide_55_plus=not no_55_plus,
            delay=delay,
        )
    finally:
        close_db()


# Allow running as `python -m zillow_scraper.cli`
if __name__ == "__main__":
    app()
