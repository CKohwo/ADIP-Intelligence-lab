import json 
import pandas as pd
import numpy as np
import asyncio

from pathlib import Path 
from fastapi import APIRouter, BackgroundTasks, HTTPException

from Master_orchestrator.master_orchestrator import run_orchestrator

router = APIRouter()

PROJECT_DIR = Path(__file__).resolve().parent.parent 
DATA_DIR = PROJECT_DIR / "data"
FEATURES_DIR = DATA_DIR / "features"
TIMESERIES_DIR = DATA_DIR / "timeseries"
INSIGHT_DIR = DATA_DIR / "llm_insight"

# ---------------- 
# HELPER FUNCTION  
# ----------------  
def clean_df_for_json(df):
    """
    Replacing Nan/inf with None so FastAPI can serialize Json
    """
    return df.replace([np.nan, np.inf, -np.inf], None).to_dict(orient="records")
#------------------------- 
# SYSTEM ENDPOINTS    
#------------------------ 

@router.get("/health")
def health():
    """
    HEALTH CHECK ENDPOINT
    """
    sources = {
        "api_product":{
            "features": (FEATURES_DIR/"api_ingest"/"product_features.parquet").exists(),
            "timeseries": (TIMESERIES_DIR/"api_ingest"/"product_timeseries.parquet").exists(),
             "llm_insight": (INSIGHT_DIR/"api_product_insight.json").exists()
        },
        "api_brand":{
            "features": (FEATURES_DIR/"api_ingest"/"brand_features.parquet").exists(),
            "timeseries": (TIMESERIES_DIR/"api_ingest"/"brand_timeseries.parquet").exists(),
             "llm_insight": (INSIGHT_DIR/"api_brand_insight.json").exists()
        },
        "api_seller":{
            "features": (FEATURES_DIR/"api_ingest"/"seller_features.parquet").exists(),
            "timeseries": (TIMESERIES_DIR/"api_ingest"/"seller_timeseries.parquet").exists(),
             "llm_insight": (INSIGHT_DIR/"api_seller_insight.json").exists()
        },
        "scraper_categories":{
            "features": (FEATURES_DIR/"scraper"/"category_features.parquet").exists(),
            "timeseries": (TIMESERIES_DIR/"scraper"/"category_timeseries.parquet").exists(),
             "llm_insight": (INSIGHT_DIR/"scraper_category_insight.json").exists()
        },
        "scraper_brand":{
            "features": (FEATURES_DIR/"scraper"/"brand_features.parquet").exists(),
            "timeseries": (TIMESERIES_DIR/"scraper"/"brand_timeseries.parquet").exists(),
             "llm_insight": (INSIGHT_DIR/"scraper_brand_insight.json").exists()
        },
    }        
         
    return {"status": "healthy",
            "service": "AUTOMATED DATA INTELLIGENCE PLATFORM (ADIP) API",
            "version": "1.0.0",
            "sources" : sources 
    }  


@router.post("/run-application")
async def run_application(background_tasks: BackgroundTasks):
    """
    RUN THE ADIP INTELLIGENCE APPLICATION PIPELINE ASYNCHRONOUSLY.
    """ 
    try:
        background_tasks.add_task(run_orchestrator)
        return{
            "status": "success",
            "message": "ADIP INTELLIGENCE APPLICATION RUNNING SUCCESSFULLY IN THE BACKGROUND. Poll /health for status updates." 
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error running application: {str(e)}")


#------------------------
# ADIP FEATURED DATA ENDPOINTS
#------------------------

"""
PRODUCT FEATURES DATA ENDPOINTS - API PRODUCT FEATURES.
"""
@router.get("/features/products")
def get_api_product_features():
    """
    GET API PRODUCT FEATURES DATA   
    """
    try:
        api_product_features = pd.read_parquet(FEATURES_DIR / "api_ingest" / "product_features.parquet") 
        return clean_df_for_json(api_product_features)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving API product features: {str(e)}")
    

"""
BRAND FEATURES DATA ENDPOINT - 
1. API BRAND FEATURES
2. SCRAPED BRAND FEATURES
"""

@router.get("/features/brand/api")
def get_api_brand_features():
    """
    GET API BRAND FEATURES DATA   
    """
    try:
        api_brand_features = pd.read_parquet(FEATURES_DIR / "api_ingest" / "brand_features.parquet") 
        return clean_df_for_json(api_brand_features)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving API brand features: {str(e)}")


@router.get("/features/brand/scraper")
def get_scraped_brand_features():
    """
    GET SCRAPED BRAND FEATURES DATA   
    """
    try:
        scraper_brand_features = pd.read_parquet(FEATURES_DIR / "scraper" / "brand_features.parquet") 
        return clean_df_for_json(scraper_brand_features)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving scraped brand features: {str(e)}")


"""
SELLER FEATURES DATA ENDPOINTS - API SELLER FEATURES.
""" 
@router.get("/features/sellers")
def get_api_seller_features():
    """
    GET API Seller FEATURES DATA   
    """
    try:
        api_seller_features = pd.read_parquet(FEATURES_DIR / "api_ingest" / "seller_features.parquet") 
        return clean_df_for_json(api_seller_features)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving API seller features: {str(e)}")


"""
SCRAPED CATAEGORIES FEATURES DATA ENDPOINT
""" 
@router.get("/features/categories")
def get_scraped_category_features():
    """
    GET SCRAPED CATEGORY FEATURES DATA   
    """
    try:
        scraped_category_features = pd.read_parquet(FEATURES_DIR / "scraper" / "category_features.parquet") 
        return clean_df_for_json(scraped_category_features)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving scraped category features: {str(e)}")


#------------------------
# ADIP TIMESERIES DATA ENDPOINTS
#------------------------

"""
PRODUCT TIMESERIES DATA ENDPOINTS - API PRODUCT TIMESERIES.
"""
@router.get("/timeseries/products")
def get_api_product_timeseries():
    """
    GET API PRODUCT TIMESERIES DATA   
    """
    try:
        api_product_timeseries = pd.read_parquet(TIMESERIES_DIR / "api_ingest" / "product_timeseries.parquet") 
        return clean_df_for_json(api_product_timeseries)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving API product timeseries: {str(e)}")
    

"""
BRAND TIMESERIES DATA ENDPOINT - 
1. API BRAND TIMESERIES
2. SCRAPED BRAND TIMESERIES
"""

@router.get("/timeseries/brands/api")
def get_api_brand_timeseries():
    """
    GET API BRAND TIMESERIES DATA   
    """
    try:
        api_brand_timeseries = pd.read_parquet(TIMESERIES_DIR / "api_ingest" / "brand_timeseries.parquet") 
        return clean_df_for_json(api_brand_timeseries)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving API brand timeseries: {str(e)}")


@router.get("/timeseries/brands/scraper")
def get_scraped_brand_timeseries():
    """
    GET SCRAPED BRAND TIMESERIES DATA   
    """
    try:
        scraper_timeseries_features = pd.read_parquet(TIMESERIES_DIR / "scraper" / "brand_timeseries.parquet") 
        return clean_df_for_json(scraper_timeseries_features)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving scraped brand timeseries: {str(e)}")


"""
SELLER TIMESERIES DATA ENDPOINTS - API SELLER TIMESERIES.
""" 
@router.get("/timeseries/sellers")
def get_api_seller_timeseries():
    """
    GET API Seller TIMESERIES DATA   
    """
    try:
        api_seller_timeseries = pd.read_parquet(TIMESERIES_DIR / "api_ingest" / "seller_timeseries.parquet") 
        return clean_df_for_json(api_seller_timeseries)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving API seller timeseries: {str(e)}")


"""
SCRAPED CATAEGORIES TIMESERIES DATA ENDPOINT
""" 
@router.get("/timeseries/categories")
def get_scraped_category_timeseries():
    """
    GET SCRAPED CATEGORY TIMESERIES DATA   
    """
    try:
        scraped_category_timeseries = pd.read_parquet(TIMESERIES_DIR / "scraper" / "category_timeseries.parquet") 
        return clean_df_for_json(scraped_category_timeseries)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving scraped category timeseries: {str(e)}")


#-----------------------------------
# ADIP LLM INSIGHT DATA ENDPOINTS
#-----------------------------------

"""
PRODUCT INSIGHTS DATA ENDPOINTS - API PRODUCT INSIGHTS.
"""
@router.get("/llm_insight/product")
def get_api_product_insight():
    """
    GET API PRODUCT INSIGHTS DATA   
    """
    try:
        with open (INSIGHT_DIR / "api_product_insight.json", "r") as file:
            return json.load(file)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving API product insights: {str(e)}")
    

"""
BRAND INSIGHTS DATA ENDPOINT - 
1. API BRAND INSIGHTS
2. SCRAPED BRAND INSIGHTS
"""

@router.get("/llm_insight/brands/api")
def get_api_brand_insights():
    """
    GET API BRAND INSIGHTS DATA   
    """
    try:
        with open (INSIGHT_DIR / "api_brand_insight.json", "r") as file:
            return json.load(file)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving API brand insights: {str(e)}")


@router.get("/llm_insight/brands/scraper")
def get_scraped_brand_insights():
    """
    GET SCRAPED BRAND INSIGHTS DATA   
    """
    try:
        with open (INSIGHT_DIR / "scraper_brand_insight.json", "r") as file:
            return json.load(file)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving scraped brand insight: {str(e)}")


"""
API SELLER INSIGHTS DATA ENDPOINT.
""" 
@router.get("/llm_insight/sellers")
def get_api_seller_insights():
    """
    GET API Seller INSIGHTS DATA   
    """
    try:
        with open (INSIGHT_DIR / "api_seller_insight.json", "r") as file:
            return json.load(file)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving API seller insight: {str(e)}")


"""
SCRAPED CATAEGORIES INSIGHTS DATA ENDPOINT
""" 
@router.get("/llm_insight/categories")
def get_scraped_category_insights():
    """
    GET SCRAPED CATEGORY INSIGHTS DATA   
    """
    try:
        with open(INSIGHT_DIR / "scraper_category_insight.json", "r") as file:
            return json.load(file)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving scraped category insights: {str(e)}")
