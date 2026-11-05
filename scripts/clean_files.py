from datetime import datetime
from pathlib import Path

import click
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data"  / "green_taxi"/ "raw"
CLEAN_DIR = PROJECT_ROOT / "data"  / "green_taxi"/ "clean" 
RESCUE_DIR = PROJECT_ROOT / "data" / "green_taxi"/ "rescue"

RENAME = {
    "VendorID": "vendor_id",
    "lpep_pickup_datetime": "pickup_datetime",
    "lpep_dropoff_datetime": "dropoff_datetime",
    "RatecodeID": "ratecode_id",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
}

SCHEMA = {
    "vendor_id": pl.Int32,
    "pickup_datetime": pl.Datetime("us"),
    "dropoff_datetime": pl.Datetime("us"),
    "store_and_fwd_flag": pl.Utf8,
    "ratecode_id": pl.Int32,
    "pickup_location_id": pl.Int32,
    "dropoff_location_id": pl.Int32,
    "passenger_count": pl.Int32,
    "trip_distance": pl.Float64,
    "fare_amount": pl.Float64,
    "extra": pl.Float64,
    "mta_tax": pl.Float64,
    "tip_amount": pl.Float64,
    "tolls_amount": pl.Float64,
    "ehail_fee": pl.Float64,
    "improvement_surcharge": pl.Float64,
    "total_amount": pl.Float64,
    "payment_type": pl.Int32,
    "trip_type": pl.Int32,
    "congestion_surcharge": pl.Float64,
    "cbd_congestion_fee": pl.Float64,
}


def write_or_remove(df: pl.DataFrame, dest: Path) -> None:
    """Write the file if there are rows; otherwise delete any old file from a previous run."""
    if df.height > 0:
        dest.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(dest)
    elif dest.exists():
        dest.unlink()


def clean_file(src: Path, name: str, year: int, month: int) -> None:
    raw = pl.read_parquet(src)
    df = raw.rename(RENAME, strict=False)

    # 1. Same columns and types for every month
    df = df.select([
        pl.col(c).cast(t, strict=False) if c in df.columns
        else pl.lit(None, dtype=t).alias(c)
        for c, t in SCHEMA.items()
    ])

    # 2. Label each row with the FIRST rule it breaks (null = valid)
    df = df.with_columns(
        pl.when(pl.col("pickup_datetime").is_null())
          .then(pl.lit("missing_pickup_datetime"))
        .when(
            (pl.col("pickup_datetime").dt.year() != year)
            | (pl.col("pickup_datetime").dt.month() != month)
        )
          .then(pl.lit("pickup_outside_file_month"))
        .when(pl.col("dropoff_datetime") < pl.col("pickup_datetime"))
          .then(pl.lit("dropoff_before_pickup"))
        .otherwise(None)
        .alias("reject_reason")
    )

    # 3. Duplicates: keep the first copy, send the extra copies to rescue
    data_cols = list(SCHEMA.keys())
    df = df.with_columns(
        pl.when(pl.col("reject_reason").is_null()
                & ~pl.struct(data_cols).is_first_distinct())
          .then(pl.lit("duplicate"))
          .otherwise(pl.col("reject_reason"))
          .alias("reject_reason")
    )

    # 4. Lineage columns (on both outputs)
    df = df.with_columns(
        pl.lit(src.name).alias("source_file"),
        pl.lit(datetime.now()).alias("loaded_at"),
    )

    # 5. Split
    clean = df.filter(pl.col("reject_reason").is_null()).drop("reject_reason")
    rescue = df.filter(pl.col("reject_reason").is_not_null())

    write_or_remove(clean, CLEAN_DIR / name)
    write_or_remove(rescue, RESCUE_DIR / name)

    # 6. Summary
    click.echo(f"{name}: {raw.height:,} raw -> {clean.height:,} clean, "
               f"{rescue.height:,} rescue")
    if rescue.height:
        for row in rescue.group_by("reject_reason").len().sort("len", descending=True).iter_rows():
            click.echo(f"    {row[0]}: {row[1]:,}")

    # Safety check: no rows lost
    assert clean.height + rescue.height == raw.height, "Row count mismatch!"


@click.command()
@click.option("--year", "-y", type=int, default=2025, show_default=True)
@click.option("--month", "-m", type=click.IntRange(1, 12), multiple=True,
              default=[1, 2, 3], show_default=True)
def main(year: int, month: tuple[int, ...]) -> None:
    """Clean raw Green Taxi files; move rejected rows to the rescue folder."""
    for m in sorted(set(month)):
        name = f"green_tripdata_{year}-{m:02d}.parquet"
        src = RAW_DIR / name
        if not src.exists():
            click.echo(f"Missing: {src}", err=True)
            continue
        clean_file(src, name, year, m)


if __name__ == "__main__":
    main()