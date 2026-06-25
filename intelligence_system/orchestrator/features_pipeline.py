import pandas as pd
from pathlib import Path

from intelligence_system.features.api_features import save_features
from intelligence_system.features.api_features import build_product_features, build_brand_featrures, build_seller_features 
from intelligence_system.features.api_forecasting_features import build_product_timeseries, build_brand_timeseries, build_seller_timeseries

from intelligence_system.features.scraper_features import build_category_features, build_brand_features as build_scraper_brand_features
from intelligence_system.features.scraper_forecasting_features import build_category_timeseries, build_brand_timeseries as build_scraper_brand_timeseries
 

API_Transformed_data_dir = Path("data/transformed/api_ingest.parquet")
API_Features_data_dir = Path("data/features/api_ingest")

SCRAPER_Transformed_data_dir = Path("data/transformed/scraper.parquet") 
SCRAPER_Features_data_dir = Path("data/features/scraper")

Forecasting_data_dir = Path("data/timeseries")

for directory in [API_Features_data_dir, SCRAPER_Features_data_dir, Forecasting_data_dir]:
    directory.mkdir(parents=True, exist_ok=True)

 
#API FEATURES PIPELINE FUNCTION
def run_api_features_pipeline():
    """ 
    Runs the ADIP Intelligence API Features pipeline layer- 
    """
    if not API_Transformed_data_dir.exists():
        raise FileNotFoundError(
            f"Missing transformed api data :{API_Transformed_data_dir},"
            "Run the transform pipeline first"
        )
    print("Building API Pipeline....")

    df = pd.read_parquet(API_Transformed_data_dir)
    product_features = build_product_features(df)
    brand_features = build_brand_featrures(product_features)
    seller_features = build_seller_features(df)
    save_features(product_features, API_Features_data_dir/ "product_features.parquet", "Product")
    save_features(brand_features, API_Features_data_dir/ "brand_features.parquet", "Brand")
    save_features(seller_features, API_Features_data_dir/ "seller_features.parquet", "Seller")

    print(f"Product features : {product_features.shape}")
    print(f"Brand feature: {brand_features.shape}") 
    print(f"Seller features: {seller_features.shape}")
    
    return {"product_features": product_features, "brand_features": brand_features, 
            "seller_feature":seller_features} 
  

# SCRAPER FEATURES PIPELINE FUNCTION 
def run_scraper_features_pipeline():
    """ 
    Runs the ADIP Intelligence SCRAPER Features pipeline layer- 
    """

    if not SCRAPER_Transformed_data_dir.exists():
        raise FileNotFoundError(
            f"Missing transformed Scraper data :{SCRAPER_Transformed_data_dir},"
            "Run the transform pipeline first"
        )
    print("Building SCRAPER Pipeline....")


    df = pd.read_parquet(SCRAPER_Transformed_data_dir)
    category_features = build_category_features(df)
    brand_features = build_scraper_brand_features(df)
   
    save_features(category_features, SCRAPER_Features_data_dir / "category_features.parquet", "Category")
    save_features(brand_features, SCRAPER_Features_data_dir / "brand_features.parquet", "Brand") 

    print(f"Category features : {category_features.shape}")
    print(f"Brand feature: {brand_features.shape}") 
   
    return {"category_features": category_features,"brand_features": brand_features}

 

def run_api_forecasting_pipeline():
    """ 
    Runs the ADIP Intelligence API Timeseries pipeline layer- 
    """

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
    
    print(f"Product Timeseries : {product_timeseries.shape}")
    print(f"Brand Timeseries: {brand_timeseries.shape}") 
    print(f"Seller Timeseries: {seller_timeseries.shape}")
    
    return {"Product_timeseries": product_timeseries, "Brand_timeseries": brand_timeseries,
            "seller_timeseries": seller_timeseries}  

     

def run_scraper_forecasting_pipeline():
    """ 
    Runs the ADIP Intelligence SCRAPER Timeseries pipeline layer- 
    """

    if not SCRAPER_Transformed_data_dir.exists():
            raise FileNotFoundError(
                f"The {SCRAPER_Transformed_data_dir} directory not found," 
                "please create the directory")
        
    df = pd.read_parquet(SCRAPER_Transformed_data_dir)
    category_timeseries = build_category_timeseries(df)
    brand_timeseries = build_scraper_brand_timeseries(df)
    
    save_features(category_timeseries, Forecasting_data_dir/"scraper"/"category_timeseries.parquet", "Category_timeseries") 
    save_features(brand_timeseries, Forecasting_data_dir/"scraper"/"brand_timeseries.parquet", "Brand_timeseries")
    
    print(f"Category Timeseries : {category_timeseries.shape}")
    print(f"Brand Timeseries: {brand_timeseries.shape}") 
   
    return {"Category_timeseries": category_timeseries, "Brand_timeseries": brand_timeseries} 



def run_all_features_pipelines() -> dict:
     """
     Executes all the features and timeseries pipelines.
     This is wiil be called directly from the master orchestrator, not run directly.
     """
     print("ADIP Features & Timeseries Pipeline running...")

     results = { 
      "api_features": run_api_features_pipeline(),
      "scraper": run_scraper_features_pipeline(),
      "api_timeseries": run_api_forecasting_pipeline(),
      "scraper_timeseries": run_scraper_forecasting_pipeline()
     }

     return results      

if __name__ == "__main__": 
     run_all_features_pipelines()
         