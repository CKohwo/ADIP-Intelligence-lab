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
             "llm_insight": (INSIGHT_DIR/"product_insight.json").exists()
        },
        "api_brand":{
            "features": (FEATURES_DIR/"api_ingest"/"brand_features.parquet").exists(),
            "timeseries": (TIMESERIES_DIR/"api_ingest"/"brand_timeseries.parquet").exists(),
             "llm_insight": (INSIGHT_DIR/"brand_insight.json").exists()
        },
        "api_seller":{
            "features": (FEATURES_DIR/"api_ingest"/"seller_features.parquet").exists(),
            "timeseries": (TIMESERIES_DIR/"api_ingest"/"seller_timeseries.parquet").exists(),
             "llm_insight": (INSIGHT_DIR/"seller_insight.json").exists()
        },
        "scraper_categories":{
            "features": (FEATURES_DIR/"scraper"/"category_features.parquet").exists(),
            "timeseries": (TIMESERIES_DIR/"scraper"/"category_timeseries.parquet").exists(),
             "llm_insight": (INSIGHT_DIR/"category_insight.json").exists()
        },
        "scraper_brand":{
            "features": (FEATURES_DIR/"scraper"/"brand_features.parquet").exists(),
            "timeseries": (TIMESERIES_DIR/"scraper"/"brand_timeseries.parquet").exists(),
             "llm_insight": (INSIGHT_DIR/"sc_brand_insight.json").exists()
        },
    }        
         
    return {"status": "Active and Healthy endpoints",
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
        raise HTTPException(status_code=400, detail=f"Error running application: Unknown source")


#------------------------
# ADIP DATA ENDPOINTS
#------------------------

"""
PRODUCT DATA ENDPOINTS - API FEATURED, TIMESERIES & AI INSIGHTS .
"""
@router.get("/dashboard/product")
def get_product():
    try:
        product_features = pd.read_parquet(FEATURES_DIR / "api_ingest" / "product_features.parquet" )
        product_timeseries = pd.read_parquet(TIMESERIES_DIR / "api_ingest" / "product_timeseries.parquet") 
         
        with open (INSIGHT_DIR / "product_insight.json", "r") as file:
            product_insight = json.load(file)

        return {"features" : clean_df_for_json(product_features), 
                "timeseries" : clean_df_for_json(product_timeseries),
                "insight": product_insight
                } 
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error retrieving API product features, timeseries & insight: Unknown source")
    


"""
BRAND DATA ENDPOINTS - FEATURED, TIMESERIES & AI INSIGHTS 
"""

@router.get("/dashboard/brand")
def get_brand(source: str):
    if source == "api":
        brand_features = pd.read_parquet(FEATURES_DIR / "api_ingest" / "brand_features.parquet") 
        brand_timeseries = pd.read_parquet(TIMESERIES_DIR / "api_ingest" / "brand_timeseries.parquet") 
         
        with open (INSIGHT_DIR / "brand_insight.json", "r") as file:
            brand_insight = json.load(file)

        return {"features" : clean_df_for_json(brand_features), 
                "timeseries" : clean_df_for_json(brand_timeseries),
                "insight": brand_insight
                }

    elif source == "scraper":
        brand_features = pd.read_parquet(FEATURES_DIR / "scraper" / "brand_features.parquet")#
        brand_timeseries = pd.read_parquet(TIMESERIES_DIR / "scraper" / "brand_timeseries.parquet") 
             
        with open (INSIGHT_DIR / "sc_brand_insight.json", "r") as file:
            brand_insight = json.load(file)

        return {"features" : clean_df_for_json(brand_features), 
                "timeseries" : clean_df_for_json(brand_timeseries),
                "insight": brand_insight
                }

    else:
        raise HTTPException(status_code=400, detail=f"Error retrieving API brand features, timeseries & insight: Unknown source")

 
"""
SELLER DATA ENDPOINTS - API FEATURES & TIMESERIES & AI INSIGHTS
""" 
@router.get("/dashboard/seller")
def get_seller():
    try:
        seller_features = pd.read_parquet(FEATURES_DIR / "api_ingest" / "seller_features.parquet")
        seller_timeseries = pd.read_parquet(TIMESERIES_DIR / "api_ingest" / "seller_timeseries.parquet") 
         
        with open (INSIGHT_DIR / "seller_insight.json", "r") as file:
            seller_insight = json.load(file)

        return {"features" : clean_df_for_json(seller_features), 
                "timeseries" : clean_df_for_json(seller_timeseries),
                "insight": seller_insight
                }
     
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error retrieving API seller features, timeseries & insight: Unknown source")


"""
SCRAPED CATAEGORIES DATA ENDPOINT - FEATURED, TIMESERIES, AI INSIGHTS
""" 
@router.get("/dashboard/category")
def get_category():
    try:
        category_features = pd.read_parquet(FEATURES_DIR / "scraper" / "category_features.parquet") 
        category_timeseries = pd.read_parquet(TIMESERIES_DIR / "scraper" / "category_timeseries.parquet")
         
        with open(INSIGHT_DIR / "category_insight.json", "r") as file:
            category_insight = json.load(file)

        return {"features" : clean_df_for_json(category_features), 
                "timeseries" : clean_df_for_json(category_timeseries),
                "insight": category_insight
                } 
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error retrieving scraped category features, timeseries & llm_insight: Unknown source")
 