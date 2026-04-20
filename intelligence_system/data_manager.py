import os
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import sys
from pathlib import Path
from io import BytesIO 

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from config import URL_ENDPOINT


# Cache file for saving temporary files  
cache_file = Path(".data_cache")
cache_file.mkdir(parents=True,exist_ok=True)

url = URL_ENDPOINT 

def requests_sessions(total_retries: int=3, backoff_factor: float=0.5):
    """Download a file from a URL with retries on failure."""
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods= ["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session

    
def fetch_via_http(url:str, cache_ttl_sec=172800, cache_name:str = None) -> pd.DataFrame:
    
    if cache_name is None:
        file_name = url.split("/")[-1]
        cache_name = f"{file_name}"
    
    cache_path = cache_file / cache_name

    if cache_path.exists():
        file_age = time.time() - cache_path.stat().st_mtime
        if file_age < cache_ttl_sec:
            print(f"Loading data from store cache {cache_name}")
            return pd.read_csv(cache_path)

    #But if the cache data stored in the directory is older than 48 Hour
    # we will refetch new data from the ingestion lab server.

    print(f"Fetching new data from the {url}")
    session = requests_sessions()
    try:
        response = session.get(url, timeout = 150)
        response.raise_for_status()

        df = pd.read_csv(BytesIO(response.content))
        df.to_csv(cache_path, index = False)
        print(f"Data stored within the cache path:{cache_path}")

        return df

    except requests.exceptions.RequestException as e:
        print(f"An error occured while fetching data:{e}")
        if cache_path.exists():
            print(f"Network failed while loading data from url, retrieving the expired cache from:{cache_path}")
            return pd.read_csv(cache_path)

        return None    


def ingest_adip_data() -> dict:
    """Orchestrates the downloading of all required datasets defined in config."""
    session = requests_sessions()
    adip_dataframes = {}
    
    # Iterate through our dictionary of endpoints
    for name, url in URL_ENDPOINT.items():
        # Store the resulting DataFrame in a new dictionary using the dataset name
        adip_dataframes[name] = fetch_via_http(url=url, cache_name=f"{name}.csv")
        
    return adip_dataframes
   

if __name__ == "__main__":
    print("Starting data ingestion process...")
    data = ingest_adip_data()
    print(f"Data ingestion completed. Datasets available:{list(data.keys())}")
             
        
        

