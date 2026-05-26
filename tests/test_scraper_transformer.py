import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from intelligence_system.transformers.scraper import transform
from intelligence_system.schemas.schemas import EXPECTED_COLUMNS


CACHE_DIR = ROOT / ".data_cache"
SCRAPER_CACHE = CACHE_DIR / "scraper.csv"


def assert_expected_schema(df: pd.DataFrame) -> None:
    assert list(df.columns) == EXPECTED_COLUMNS


def test_scraper_transformer_outputs_expected_schema():
    assert SCRAPER_CACHE.exists(), (
        "Missing .data_cache/scraper.csv. "
        "Run the data bridge first."
    )

    raw_df = pd.read_csv(SCRAPER_CACHE)

    cleaned_df = transform(raw_df)

    assert not cleaned_df.empty

    # Processed schema contract
    assert_expected_schema(cleaned_df)

    # Critical fields
    critical_columns = [
        "product_id",
        "product_name",
        "price",
        "fetched_time",
        "source",
    ]

    for col in critical_columns:
        assert cleaned_df[col].notna().all(), f"{col} contains missing values."

    # Type checks
    assert pd.api.types.is_numeric_dtype(cleaned_df["price"])
    assert pd.api.types.is_datetime64_any_dtype(cleaned_df["fetched_time"])

    # Source-specific checks
    assert (cleaned_df["source"] == "Web_scraper").all()
    assert (cleaned_df["seller_name"] == "JUMIA").all()

    # Scraper-specific generated fields
    assert cleaned_df["brand"].notna().all()
    assert cleaned_df["category"].notna().all()
    assert "product_rating" in cleaned_df.columns
    assert "rating_count" in cleaned_df.columns
    assert "stock_qty" in cleaned_df.columns

    print(f"Raw shape: {raw_df.shape}")
    print(f"Cleaned shape: {cleaned_df.shape}")
    print(f"Rows dropped: {raw_df.shape[0] - cleaned_df.shape[0]}")


def test_scraper_processed_schema_breaks_when_expected_column_changes():
    assert SCRAPER_CACHE.exists(), (
        "Missing .data_cache/scraper.csv. "
        "Run the data bridge first."
    )

    raw_df = pd.read_csv(SCRAPER_CACHE)
    cleaned_df = transform(raw_df)

    damaged_df = cleaned_df.rename(columns={"product_name": "title"})

    with pytest.raises(AssertionError):
        assert_expected_schema(damaged_df)