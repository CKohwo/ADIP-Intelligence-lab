import re
import sys
import pandas as pd

""" This is a utility module for normalizing brand names in the dataset. It includes functions to clean and 
standardize brand values, as well as a mapping of common brand name variations to their canonical forms. 
This helps ensure that the brand features are accurate and consistent for analysis and modeling.
"""

INVALID_BRAND_TOKENS = {
    "usb", "pd", "hdmi", "replacement", "portable", "adjustable",
    "gaming", "laptop", "adapter", "charger", "battery", "magic",
    "rechargeable", "mini", "wireless", "bluetooth", "type", "c",
    "male", "female", "extension", "cable", "case", "cover",
    "screen", "protector", "stand", "mouse", "keyboard", "notebook",
    "computer", "macbook", "for", "with", "and", "the", "new",
}

BRAND_NAME_MAP = {
    "hp": "HP",
    "hewlett packard": "HP",
    "apple": "Apple",
    "logitech": "Logitech",
    "razer": "Razer",
    "lenovo": "Lenovo",
    "dell": "Dell",
    "asus": "ASUS",
    "acer": "Acer",
    "microsoft": "Microsoft",
    "baseus": "Baseus",
    "samsung": "Samsung",
    "sony": "Sony",
    "canon": "Canon",
    "epson": "Epson",
    "toshiba": "Toshiba",
    "msi": "MSI",
    "lg": "LG",
}


def clean_brand_value(value) -> str:
    """
    Clean and normalize brand values before feature engineering.

    This prevents generic product-name tokens like 'Usb', 'Replacement',
    '2024', or 'Laptop' from being treated as real brands.
    """

    if pd.isna(value):
        return "Unknown"

    brand = str(value).strip()

    if not brand:
        return "Unknown"

    normalized = brand.lower().strip()
    normalized = re.sub(r"\s+", " ", normalized)

    # Reject obvious non-brand tokens
    if normalized in INVALID_BRAND_TOKENS:
        return "Unknown"

    # Reject mostly numeric/spec-like values
    if re.fullmatch(r"\d+", normalized):
        return "Unknown"

    if re.fullmatch(r"\d+[a-zA-Z]+", normalized):
        return "Unknown"

    if re.fullmatch(r"\d+(\.\d+)?\s?(w|gb|tb|mah|hz|ghz|inch|inches|m|cm|mm)", normalized):
        return "Unknown"

    # Normalize known brands
    if normalized in BRAND_NAME_MAP:
        return BRAND_NAME_MAP[normalized]

    # Default formatting for unknown-but-plausible brands
    return brand.title()