import os
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from pathlib import Path
from io import BytesIO 

# Cache file for saving temporary files  
cache_file = Path(".data_cache")
cache_file.mkdir(parents=True,exist_ok=True)

def download_file(url, max_retries=3, backoff_factor=0.5):
    """Download a file from a URL with retries on failure."""
    session = requests.Session()
    retry = Retry(
        total=max_retries,
        read=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session

    
def fetch_via_http(url:str, cache_ttl_sec=3600, cache_name:str = None) -> pd.Dataframe:
    
    if cache_name is None:
        file_name = url.split("/")[-1]
        cache_name = f"{file_name}"
    
    cache_path = cache_file / cache_name

    if cache_path.exists():
        file_age = time.time() - cache_path.stat().st_mtime
        if file_age < cache_ttl_sec:
            print(f"Loading data from store cache {"cache_name"}")
            return pd.read_csv(cache_path)

    #But if the cache data stored in the directory is older than an Hour
    # we will refetch new data from the ingestion lab server.

    print(f"Fetching new data from the {url}")
    session = download_file()
    try:
        response = session.get(url, timeout = 20)
        response.raise_for_status()

        df = pd.read_csv(BytesIO(response.content))
        df.to_csv(cache_path, index = False)
        print(f"stored loaded data within the cache path:{cache_path}")

        return df

    except requests.exceptions.RequestException as e:
        print(f"An error occured while fetching data:{e}")
        if cache_path.exists():
            print(f"Network failed while loading data from url, retrieving the expired cache from:{cache_path}")
            return pd.read_csv(cache_path)

        return None    

            
        
        

