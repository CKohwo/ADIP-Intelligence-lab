import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from intelligence_system.orchestrator.transform_pipeline import run_orchestration
from intelligence_system.schemas.schemas import EXPECTED_COLUMNS


PROCESSED_DIR = ROOT / "data" / "processed"
RAW_DIR = ROOT / "data" / "raw"


def test_orchestrator_creates_expected_outputs():
    """
    Validates that the ADIP Intelligence orchestrator:
    - fetches source datasets through the data bridge
    - transforms api_ingest
    - transforms scraper
    - preserves api_auth as raw passthrough
    - writes all expected parquet outputs
    """

    outputs = run_orchestration()

    # -------------------------------
    # Output manifest checks
    # -------------------------------
    assert isinstance(outputs, dict)

    assert "api_ingest" in outputs
    assert "scraper" in outputs
    assert "api_auth" in outputs

    # -------------------------------
    # File existence checks
    # -------------------------------
    api_path = Path(outputs["api_ingest"])
    scraper_path = Path(outputs["scraper"])
    auth_path = Path(outputs["api_auth"])

    assert api_path.exists(), f"Missing api_ingest output: {api_path}"
    assert scraper_path.exists(), f"Missing scraper output: {scraper_path}"
    assert auth_path.exists(), f"Missing api_auth output: {auth_path}"

    # -------------------------------
    # Read parquet outputs
    # -------------------------------
    api_df = pd.read_parquet(api_path)
    scraper_df = pd.read_parquet(scraper_path)
    auth_df = pd.read_parquet(auth_path)

    # -------------------------------
    # Non-empty output checks
    # -------------------------------
    assert not api_df.empty
    assert not scraper_df.empty
    assert not auth_df.empty

    # -------------------------------
    # Processed schema checks
    # -------------------------------
    assert list(api_df.columns) == EXPECTED_COLUMNS
    assert list(scraper_df.columns) == EXPECTED_COLUMNS

    # -------------------------------
    # Raw passthrough check
    # api_auth is intentionally not forced into EXPECTED_COLUMNS.
    # -------------------------------
    assert list(auth_df.columns) != EXPECTED_COLUMNS

    print(f"api_ingest output: {api_path} | Shape: {api_df.shape}")
    print(f"scraper output: {scraper_path} | Shape: {scraper_df.shape}")
    print(f"api_auth output: {auth_path} | Shape: {auth_df.shape}")