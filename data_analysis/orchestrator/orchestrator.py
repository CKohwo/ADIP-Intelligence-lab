import pandas as pd
from data_bridge import ingest_adip_data
from data_analysis.transformers.clean_api_ingest import transform as clean_api
from data_analysis.transformers.clean_scraper import transform as clean_scraper
 
# Orchestrator function to build the final dataset from both sources -- 

def build_dataset(api_df, scraper_df): 

    api = clean_api(api_df)
    scraper = clean_scraper(scraper_df)
     
    final_df = pd.concat([api, scraper], ignore_index=True)

    final_df.to_parquet("data/processed_data/final_dataset.parquet", index=False)

    print("✅ Final dataset built:", final_df.shape)

    return final_df 


if __name__ == "__main__":
     
""" 

 -- I am not done yet! I still need to link this orchestrator to the data bridge where the system
collects data from ingestion sytem and then passes it to the build_dataset function to 
create the final dataset-- 

"""

build_dataset(api_df, scraper_df)    