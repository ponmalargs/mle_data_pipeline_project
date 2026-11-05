from pathlib import Path

import click
import pandas as pd
import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = PROJECT_ROOT / "data" / "green_taxi" / "raw"


def download(url: str, dest: Path) -> None:
    if dest.exists():
        click.echo(f"Skip (exists): {dest.name}")
        return
    tmp = dest.with_suffix(".parquet.part")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
    tmp.rename(dest)
    click.echo(f"Downloaded: {dest.name}")


@click.command()
@click.option("--year", "-y", type=int, required=True, default=2025, help="Year, e.g. 2025")
@click.option(
    "--month", "-m",
    type=click.IntRange(1, 12),
    multiple=True,
    default=[1, 2, 3],
    required=True,
    help="Month 1-12. Repeat for several: -m 1 -m 2 -m 3",
)
@click.option(
    "--out-dir",
    type=click.Path(path_type=Path),
    default=DEFAULT_OUT,
    show_default=True,
    help="Folder to save files in",
)
def main(year: int, month: tuple[int, ...], out_dir: Path) -> None:
    """Download NYC Green Taxi trip data (Parquet) for the given year/months."""
    out_dir.mkdir(parents=True, exist_ok=True)

    for m in sorted(set(month)):
        filename = f"green_tripdata_{year}-{m:02d}.parquet"
        dest = out_dir / filename
        try:
            download(f"{BASE_URL}/{filename}", dest)
            df = pd.read_parquet(dest)
            click.echo(f"  {len(df):,} rows, {df.shape[1]} columns")
        except requests.HTTPError as e:
            click.echo(f"Failed {filename}: {e}", err=True)


if __name__ == "__main__":
    main()