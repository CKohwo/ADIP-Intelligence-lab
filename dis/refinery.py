import pandas as pd
import numpy as np
import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from config import DATA_STREAMS

class DataRefinery:
    """
    The Standardization Engine for ADIP.
    Transforms raw multi-source data streams into canonical, analysis-ready formats.
    """

    # --- HELPER UTILITIES (Private Methods) ---
    def _unpack_dict(self, val, target_key, default_val=None):
        """Safely extracts a value from a stringified dictionary."""
        if pd.isna(val):
            return default_val
        try:
            parsed_dict = ast.literal_eval(str(val))
            return parsed_dict.get(target_key, default_val)
        except (ValueError, SyntaxError):
            return default_val

    def _extract_nested_rating(self, val):
        """Specifically handles the deeply nested product_rating JSON."""
        if pd.isna(val): return np.nan
        try:
            d = ast.literal_eval(str(val))
            return d.get('quality', {}).get('average', np.nan)
        except:
            return np.nan

    def _clean_price(self, price_val):
        """Strips currencies and commas, returns float."""
        if pd.isna(price_val): return np.nan
        cleaned = re.sub(r'[^\d.]', '', str(price_val))
        try:
            return float(cleaned)
        except ValueError:
            return np.nan

    def _extract_brand(self, row, name_col, brand_col):
        """Imputes missing brands using the first word of the product name."""
        if pd.isna(row.get(brand_col)) or str(row.get(brand_col)).strip() == "":
            return str(row.get(name_col, "")).split()[0].upper()
        return str(row.get(brand_col)).upper()

    # --- PIPELINE 1: RETAIL API DATA ---
    def process_retail_api(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardizes data originating from the E-commerce API."""
        print("⚙️ Refining Retail API Stream...")
        df = df.copy()

        # 1. Drop Ghost Columns
        df = df.drop(columns=['deal_price', 'final_price', 'description'], errors='ignore')

        # 2. Unpack JSON Strings
        df['stock'] = df['stock'].apply(lambda x: self._unpack_dict(x, 'quantity', 0))
        df['seller'] = df['seller'].apply(lambda x: self._unpack_dict(x, 'name', 'Unknown'))
        df['rating'] = df['product_rating'].apply(self._extract_nested_rating)

        # 3. Impute Brands & Hardcoded Fixes (V1)
        df['brand'] = df.apply(lambda row: self._extract_brand(row, 'name', 'brand'), axis=1)
        df['brand'] = df['brand'].replace("JCO4", "HP")

        # 4. Standardize Timestamps
        df['fetched_at'] = pd.to_datetime(df['fetched_at'], utc=True)

        return df

    # --- PIPELINE 2: SCRAPER DATA ---
    def process_retail_scraper(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardizes data originating from HTML Web Scrapers."""
        print("⚙️ Refining Scraper Stream...")
        df = df.copy()

        # 1. Feature Engineer Brand
        df['Brand'] = df['Name'].str.split().str[0].str.upper()

        # 2. Clean Price to Numeric
        df['Price'] = df['Price'].apply(self._clean_price)

        # 3. Clean Ratings (Extract first number)
        df['Ratings'] = df['Ratings'].astype(str).str.split().str[0]
        df['Ratings'] = pd.to_numeric(df['Ratings'], errors='coerce').fillna(0.0)

        # 4. Standardize Timestamps
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], utc=True)

        return df

    # --- PIPELINE 3: ENVIRONMENTAL/WEATHER DATA ---
    def process_environment(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardizes geographical and environmental telemetry data."""
        print("⚙️ Refining Environmental Stream...")
        df = df.copy()

        # Convert UNIX Epoch timestamp to readable UTC Datetime
        if 'Timestamp_UTC' in df.columns:
            df['observed_at'] = pd.to_datetime(df['Timestamp_UTC'], unit='s', utc=True)
            df = df.drop(columns=['Timestamp_UTC'])

        return df

    # --- THE MASTER ROUTER ---
    def execute_pipeline(self, config_registry):
        """
        Dynamically loops through the config dictionary, loads the data, 
        and routes it to the correct processing method.
        """
        refined_datasets = {}

        print("🚀 Booting ADIP Data Refinery Pipeline...\n")

        # Loop through each item in our config file
        for stream_name, settings in config_registry.items():
            file_path = settings['input_path']
            target_method_name = settings['method']
            
            try:
                # 1. Load the raw data
                print(f"📥 Fetching [{stream_name}] from {file_path}...")
                raw_df = pd.read_csv(file_path)

                # 2. Dynamically locate the correct cleaning function
                # getattr() looks inside this class (self) for a method matching the string name
                processor_function = getattr(self, target_method_name)

                # 3. Execute the function and store the result
                clean_df = processor_function(raw_df)
                refined_datasets[stream_name] = clean_df
                
                print(f"✅ Successfully refined [{stream_name}] -> {len(clean_df)} records.\n")

            except FileNotFoundError:
                print(f"⚠️ Warning: Could not find data for [{stream_name}] at {file_path}. Skipping.\n")
            except AttributeError:
                print(f"❌ Critical Error: Method '{target_method_name}' does not exist in DataRefinery.\n")

        return refined_datasets    