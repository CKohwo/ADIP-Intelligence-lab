import re
import ast
import pandas as pd

# This module provides utility functions for parsing and cleaning data related to products, 
# such as prices, stock quantities, ratings, and seller information. 
# The functions are designed to handle various formats and potential inconsistencies in the input data.

def clean_price(price):
    if price is None:
        return None
    try:
        return float(re.sub(r"[^\d.]", "", str(price)))
    except:
        return None


def safe_literal_eval(value):
    try:
        return ast.literal_eval(value) if isinstance(value, str) else value
    except:
        return {}


def extract_stock_quantity(stock):
    stock_dict = safe_literal_eval(stock)
    return stock_dict.get("quantity")


def extract_rating(rating):
    rating_dict = safe_literal_eval(rating)
    quality = rating_dict.get("quality", {})
    return quality.get("average"), quality.get("number_of_ratings")


def extract_seller_name(seller):
    seller_dict = safe_literal_eval(seller)
    return seller_dict.get("name")


def validate_columns(df: pd.DataFrame) -> pd.DataFrame:
    columns = df[["price", "fetched_time", "product_id"]]
    
    for col in columns:
        missing_count = df[col].isna().sum()
        if missing_count > 0:
            print(f"Column '{col}' has {missing_count} missing values.")

    df = df.dropna(subset=[columns])
    return df    