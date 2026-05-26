# This module provides utility functions for generating unique identifiers for products 
# based on their name, price, and timestamp.
import hashlib
import re

def normalize_name(name: str) -> str:
    name = str(name).lower().strip()
    
    # Remove punctuation
    name = re.sub(r"[^\w\s]", "", name)
    
    # Normalize whitespace
    name = " ".join(name.split())
    
    return name


def generate_product_id(name, price, timestamp):
    normalized = normalize_name(name)
    base = f"{normalized}-{price}-{timestamp}"
    return hashlib.md5(base.encode()).hexdigest()
