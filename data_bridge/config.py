import os

# Below consist of all the necessary data(specified by their url endpoints) gotten from the data ingestion process,
# needed for data intelligence

URL_ENDPOINT = {
    "scraper":"https://raw.githubusercontent.com/CKohwo/ADIP-ingestion-lab/main/data/scraper_dataset.csv",
    "api_ingest":"https://raw.githubusercontent.com/CKohwo/ADIP-ingestion-lab/main/data/api_ingestor.csv",
    "api_auth":"https://raw.githubusercontent.com/CKohwo/ADIP-ingestion-lab/main/data/api_auth.csv"
    }

 
# Base path for our raw data
DATA_CACHE = "../.data_cache/" 

# The Registry: Maps the data stream to its file and its specific cleaning method
DATA_STREAMS = {
    "retail_api": {
        "input_path": os.path.join(DATA_CACHE, "api_ingest.csv"),
        "method": "process_retail_api",
        "description": "Raw JSON payload from Ecommerce API"
    },
    "retail_scraper": {
        "input_path": os.path.join(DATA_CACHE, "scraper.csv"),
        "method": "process_retail_scraper",
        "description": "HTML parsed data from Ecommerce Scraper"
    },
    "environmental": {
        "input_path": os.path.join(DATA_CACHE, "api_auth.csv"),
        "method": "process_environment",
        "description": "Geographical and AQI telemetry"
    }
}    