import pandas as pd
from pathlib import Path

from intelligence_system.features.api_features import save_features
from intelligence_system.features.api_features import build_product_features, build_brand_featrures, build_seller_features 
from intelligence_system.features.api_forecasting_features import build_product_timeseries, build_brand_timeseries, build_seller_timeseries

from intelligence_system.features.scraper_features import build_category_features, build_brand_features as build_sc_brand_features
from intelligence_system.features.scraper_forecasting_features import build_category_timeseries, build_brand_timeseries as build_sc_brand_timeseries
 

API_Transformed_data_dir = Path("data/transformed/api_ingest.parquet")
API_Features_data_dir = Path("data/features/api_ingest")

SCRAPER_Transformed_data_dir = Path("data/transformed/scraper.parquet") 
SCRAPER_Features_data_dir = Path("data/features/scraper")

Forecasting_data_dir = Path("data/forecasting")

API_Features_data_dir.mkdir(parents=True, exist_ok=True) 
SCRAPER_Features_data_dir.mkdir(parents=True, exist_ok=True) 
Forecasting_data_dir.mkdir(parents=True, exist_ok=True)


#API FEATURES PIPELINE FUNCTION
def run_api_features_pipeline():
    """ 
    Runs the ADIP Intelligence Features pipeline layer- 
    """
    try:
        df = pd.read_parquet(API_Transformed_data_dir)
        product_features = build_product_features(df)
        brand_features = build_brand_featrures(product_features)
        seller_features = build_seller_features(df)
        save_features(product_features, API_Features_data_dir/ "product_features.parquet", "Product")
        save_features(brand_features, API_Features_data_dir/ "brand_features.parquet", "Brand")
        save_features(seller_features, API_Features_data_dir/ "seller_features.parquet", "Seller")
         
        return {"product_features": product_features, "brand_features": brand_features, 
                "seller_feature":seller_features} 
    
    except RuntimeError:
        print("The API pipeline failed to run successfully, please inspect the codebase and workflow!")       


# SCRAPER FEATURES PIPELINE FUNCTION 
def run_scraper_features_pipeline(): 
    try: 
        df = pd.read_parquet(SCRAPER_Transformed_data_dir)
        category_features = build_category_features(df)
        brand_features = build_sc_brand_features(df)
        
        save_features(category_features, SCRAPER_Features_data_dir / "category_features.parquet", "Category")
        save_features(brand_features, SCRAPER_Features_data_dir / "brand_features.parquet", "Brand") 

        return {"category_features": category_features,"brand_features": brand_features}
    
    except Exception as e:
        raise RuntimeError (f"The SCRAPER pipeline failed to run:{e}")       


def run_api_forecasting_pipeline():
    try:
         if not API_Transformed_data_dir.exists():
              raise FileNotFoundError(
                   f"The {API_Transformed_data_dir} directory not found," 
                   "please create the directory")
         
         df = pd.read_parquet(API_Transformed_data_dir)
         product_timeseries = build_product_timeseries(df)
         brand_timeseries = build_brand_timeseries(df)
         seller_timeseries = build_seller_timeseries(df)

         save_features(product_timeseries, Forecasting_data_dir/"api_ingest"/"product_timeseries.parquet", "Product_timeseries") 
         save_features(brand_timeseries, Forecasting_data_dir/"api_ingest"/"brand_timeseries.parquet", "Brand_timeseries")
         save_features(seller_timeseries, Forecasting_data_dir/"api_ingest"/"seller_timeseries.parquet", "Seller_timeseries") 
         
         return {"Product_timeseries": product_timeseries, "Brand_timeseries": brand_timeseries,
                 "seller_timeseries": seller_timeseries}  

    except Exception as e:
        raise RuntimeError(f"There was an error while running this pipeline:{e}")


def run_scraper_forecasting_pipeline():
    try:
         if not SCRAPER_Transformed_data_dir.exists():
              raise FileNotFoundError(
                   f"The {SCRAPER_Transformed_data_dir} directory not found," 
                   "please create the directory")
         
         df = pd.read_parquet(SCRAPER_Transformed_data_dir)
         category_timeseries = build_category_timeseries(df)
         brand_timeseries = build_sc_brand_timeseries(df)
          
         save_features(category_timeseries, Forecasting_data_dir/"scraper"/"category_timeseries.parquet", "Category_timeseries") 
         save_features(brand_timeseries, Forecasting_data_dir/"scraper"/"brand_timeseries.parquet", "Brand_timeseries")
          
         return {"Category_timeseries": category_timeseries, "Brand_timeseries": brand_timeseries} 
                 
    except Exception as e:
        raise RuntimeError(f"There was an error while running this pipeline: {e}")



if __name__ == "__main__":
     run_api_features_pipeline()
     run_scraper_features_pipeline()
     run_api_forecasting_pipeline()
     run_scraper_forecasting_pipeline()

     print("The Intelligence Feature engineering pipeline has sucessfully completed its orchestration!")