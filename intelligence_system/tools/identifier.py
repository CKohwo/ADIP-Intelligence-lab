# This module provides utility functions for generating unique identifiers for products 
# based on their name, price, and timestamp.
import hashlib
import re
import pandas as pd
from intelligence_system.tools.normalizer import clean_brand_value
 

def normalize_name(name: str) -> str:
    if pd.isna(name):
        return ""
     
    name = str(name).lower().strip()
    name = re.sub(r"[^a-z0-9\s]", "", name)
    name = re.sub(r"\s+","", name)
    
    # Normalize white spaces
    name = " ".join(name.split())
    
    return name.strip()


def generate_product_key(row: pd.Series) -> str:
    base = "|".join([      
        normalize_name(row.get("brand")),
        normalize_name(row.get("category")),
        normalize_name(row.get("product_name")),
    ]) 
    
    return hashlib.md5(base.encode()).hexdigest()
