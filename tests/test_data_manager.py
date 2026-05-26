"""
test_data_manager.py
--------------------
Real endpoints. Real HTTP. Temp cache auto-deleted after test.
Two tests total — no redundancy.
"""

import sys
import pytest
import pandas as pd
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import data_bridge.data_manager as dm 
from data_bridge.config import URL_ENDPOINT
 
def test_all_endpoints_return_valid_dataframes_and_persist_to_cache(monkeypatch, tmp_path):
    """
    PROMISE: Every GitHub raw URL returns a non-empty DataFrame
    AND writes its cache file to the isolated temp folder.
    """
    monkeypatch.setattr(dm, "cache_file", tmp_path)

    for name, url in URL_ENDPOINT.items():
        cache_name = f"{name}.csv"
        df = dm.fetch_via_http(url, cache_ttl_sec=0, cache_name=cache_name)

        assert isinstance(df, pd.DataFrame), f"{name}: not a DataFrame"
        assert len(df) > 0, f"{name}: empty data"

        cache_path = tmp_path / cache_name
        assert cache_path.exists(), f"{name}: not written to temp cache"

        print(f" {name}: {len(df)} rows, cache saved at {cache_path}")


def test_ttl_logic_routes_correctly_between_cache_and_http(monkeypatch, tmp_path):
    """
    TTL logic correctly decides between disk and network.
    
    This is ONE promise with TWO verification gates:
    1. Fresh cache (< TTL) → read from disk (fast, no HTTP).
    2. Expired cache (> TTL) → refetch from URL (mtime changes).
    
    We use one endpoint and one temp file to prove both gates.
    """
    monkeypatch.setattr(dm, "cache_file", tmp_path)

    name, url = "api_ingest", URL_ENDPOINT["api_ingest"]
    cache_name = "api_ingest.csv"
    cache_path = tmp_path / cache_name

    # Gate 1: Initial fetch (no cache exists, hits HTTP, writes to disk)
    df = dm.fetch_via_http(url, cache_ttl_sec=86400, cache_name=cache_name)
    assert isinstance(df, pd.DataFrame)
    assert cache_path.exists()
    old_mtime = cache_path.stat().st_mtime

    # Gate 2: Fresh cache (< 86400s) — must load from disk instantly
    start = time.time()
    df_fresh = dm.fetch_via_http(url, cache_ttl_sec=86400, cache_name=cache_name)
    elapsed = time.time() - start

    assert isinstance(df_fresh, pd.DataFrame)
    assert elapsed < 1.0, f"Too slow ({elapsed:.2f}s) — fresh cache should read from disk"

    # Gate 3: Expired cache (TTL=0) — must refetch and overwrite file
    time.sleep(1)
    df_expired = dm.fetch_via_http(url, cache_ttl_sec=0, cache_name=cache_name)
    new_mtime = cache_path.stat().st_mtime

    assert isinstance(df_expired, pd.DataFrame)
    assert new_mtime > old_mtime, "Expired cache was not overwritten by HTTP refetch"