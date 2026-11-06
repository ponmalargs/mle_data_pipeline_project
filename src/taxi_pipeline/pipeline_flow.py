import shutil
import subprocess
import sys
import click
from pathlib import Path

from prefect import flow, task, get_run_logger
from .config import CFG, file_name, run_period

# Reuse your existing scripts (same folder)
from .download_files import download_green_taxi, BASE_URL, DEFAULT_OUT as RAW_DIR
from .clean_green_taxi import clean_file


DBT_DIR = CFG["paths"]["dbt_project"]
P = CFG["prefect"]


@task(retries=P["download_retries"], retry_delay_seconds=P["retry_delay_seconds"])
def extract(year: int, month: int) -> Path:
    """E: download one month of raw data."""
    name = file_name(year, month)
    dest = RAW_DIR / name
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    download_green_taxi(year, (month,), RAW_DIR)
    get_run_logger().info(f"Raw file ready: {dest}")
    return dest


@task
def light_transform(src: Path, year: int, month: int) -> None:
    """t: Polars clean + rescue split for one month."""
    clean_file(src, src.name, year, month)
    get_run_logger().info(f"Cleaned {src.name}")


@task
def dbt_build() -> None:
    """L + T: seeds, models, tests."""
    logger = get_run_logger()
    # Use the dbt installed in this venv
    dbt_exe = shutil.which("dbt", path=str(Path(sys.executable).parent)) or "dbt"

    for cmd in (["deps"], ["build"]):
        result = subprocess.run(
            [dbt_exe, *cmd],
            cwd=DBT_DIR,
            capture_output=True,
            text=True,
        )
        logger.info(result.stdout)
        if result.returncode != 0:
            logger.error(result.stderr)
            raise RuntimeError(f"dbt {cmd[0]} failed")


@flow(name="nyc-green-taxi-etlt", log_prints=True)
def pipeline(year: int | None = None, months: list[int] | None = None) -> None:
    if year is None or months is None:
        year, months = run_period()
    for m in months:
        raw = extract(year, m)
        light_transform(raw, year, m)
    dbt_build()

@click.command()
@click.option("--year", "-y", type=int, default=None, help="Default: from config")
@click.option("--month", "-m", type=click.IntRange(1, 12), multiple=True)
def main(year: int | None, month: tuple[int, ...]) -> None:
    """Run the full pipeline once: download, clean, dbt build."""
    pipeline(year, list(month) or None)

if __name__ == "__main__":
   main()