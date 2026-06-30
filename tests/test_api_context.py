import sys
import pytest
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from llm_system.context.api_context import load_api_features, load_api_timeseries, FEATURES 
from llm_system.context.api_context import build_product_context, build_brand_context, build_seller_context

API_FEATURES_DIR = Path("data/features/api_ingest")
API_TIMESERIES_DIR = Path("data/timeseries/api_ingest")


def test_dir_exist():
    """ This test if the directory actually exit within the system """

    assert API_FEATURES_DIR.exists(), (
        "Missing API features directory, please creat one first!"
    )

    assert API_TIMESERIES_DIR.exists(), (
        "Missing API Timeseries directory, please create one first!"
    )

# Making sure the load_api_features and load_api_timeseries are functioning as intended
def test_load_data():
    assets = load_api_features
    assert not assets.empty

    """Testing to see if the asset[name] loops through each vales stored within the FEATURES Dicttionary"""
    assert assets["product"] == pd.read_parquet(API_FEATURES_DIR / "product_features.parquet")
    assert assets["brand"] == pd.read_parquet(API_FEATURES_DIR / "brand_features.parquet")
    
    asset = load_api_timeseries
    assert not asset.empty
    assert asset["product"] == pd.read_parquet( API_TIMESERIES_DIR / "product_timeseries.parquet")
    assert asset["brand"] == pd.read_parquet(API_TIMESERIES_DIR / "brand_timeseries.parquet")


# Making sure that the context buildout does not return empty files..Must work as intended
def test_context_build():
    product_context = build_product_context
    brand_context = build_brand_context
    seller_context = build_seller_context

    assert not product_context.empty
    assert not brand_context.empty
    assert not seller_context.empty

