import pytest
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))
 
from intelligence_system.transformers.api_ingest import transform
from intelligence_system.schemas.schemas import EXPECTED_COLUMNS

# Path to the real cached data
CACHE_DIR = ROOT / ".data_cache"
API_INGEST_CACHE = CACHE_DIR / "api_ingest.csv"
 
 
def test_api_ingest_transformer_on_real_cached_data():
    # Ensure the cache file exists before running the test
    assert API_INGEST_CACHE.exists(), (
        "Missing .data_cache/api_ingest.csv. "
        "Run the data bridge first to fetch the latest Phase-1 data."
    )

    raw_df = pd.read_csv(API_INGEST_CACHE)

    cleaned_df = transform(raw_df)

    # Output should exist
    assert not cleaned_df.empty

    # Schema contract
    assert list(cleaned_df.columns) == EXPECTED_COLUMNS

    # Critical fields
    assert cleaned_df["product_id"].notna().all()
    assert cleaned_df["price"].notna().all()
    assert cleaned_df["fetched_time"].notna().all()

    # Type checks
    assert pd.api.types.is_numeric_dtype(cleaned_df["price"])
    assert pd.api.types.is_datetime64_any_dtype(cleaned_df["fetched_time"])

    # Extracted fields should exist
    assert "stock_qty" in cleaned_df.columns
    assert "seller_name" in cleaned_df.columns
    assert "product_rating" in cleaned_df.columns
    assert "rating_count" in cleaned_df.columns

    # Source should be correctly tagged
    assert (cleaned_df["source"] == "api_ingest").all()

    print(f"Raw shape: {raw_df.shape}")
    print(f"Cleaned shape: {cleaned_df.shape}")
    print(f"Rows dropped: {raw_df.shape[0] - cleaned_df.shape[0]}")

    
def test_transform_handles_missing_critical_columns():
    """ If upstream schema drifts (e.g., 'name' renamed to 'title'),
    the transformer fails loudly instead of producing silent garbage.
    """
    # Arrange: Load real data, then deliberately break it
    raw = pd.read_csv(API_INGEST_CACHE)
    raw.rename(columns={"name": "title"}, inplace=True)  # Break the schema

    # Act + Assert: Must raise, not return garbage
    with pytest.raises(KeyError, ValueError):
        transform(raw)