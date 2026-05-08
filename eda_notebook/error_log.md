# 🛑 Data Anomalies & Transformation Blueprint

### Executive Summary of Findings
Exploratory data analysis across the ADIP ingestion streams (Retail API and HTML Web Scraper) revealed critical schema drift, corrupted payloads, and missing relational identities. 

To prevent downstream AI hallucinations, I incorporated a **Modular Transformation Architecture**, enforcing a strict "Data Contract" before any data enters the intelligence lake.

### ⚠️ Key Anomalies & Engineered Resolutions

**1. Schema Misalignment & Lineage Loss**
* **The Error:** The API and Scraper pipelines output fundamentally different data shapes. The Scraper lacked stock/seller metrics, while the API lacked categories. Furthermore, merged data lost its origin identity.
* **The Resolution:** Forged a single-source-of-truth **Canonical Schema** (`EXPECTED_COLUMNS`). Instantiated `pd.reindex()` to strictly conform all streams to this 11-column shape, injecting `NaN` for missing dimensionals. Injected a `source` column (`api_ingest` / `scraper`) to preserve absolute data lineage.

**2. The Identity Crisis (Missing Primary Keys)**
* **The Error:** Web-scraped data contained no unique SKUs, making time-series tracking and AI price-forecasting impossible.
* **The Resolution:** Engineered a deterministic identity generator (`dis/utils/identity.py`). Products lacking IDs are now assigned an immutable MD5 hash derived from `f"{name}-{price}-{timestamp}"`.

**3. Corrupt Payloads & Nested Stringification**
* **The Error:** The API payload contained ghost columns (`deal_price` = 100% Null), JS-corrupted text (`description` = `[object Object]`), and dictionary objects trapped in strings (`{'quantity': 20}`).
* **The Resolution:** Built an isolated utility layer (`data_analysis/tools/parser.py`) utilizing `ast.literal_eval` for safe, crash-proof nested extraction without mutating the raw data state. Missing brands are deterministically imputed from the leading nomenclature of the `product_name`.

### 🏗️ Infrastructure Update: The Transformer Pattern
* `data_analysis/schemas/` ➔ Holds the Schema contract.
* `data_analysis/tools/` ➔ Holds pure, isolated data parsing functions.
* `data_analysis/transformers/` ➔ Dedicated, isolated scripts for each data stream (`clean_api_ingest.py`, `clean_scraper.py`).
* `data_analysis/orchestrator/` ➔ The Orchestrator (`orchestrator.py`) that executes transformers and concatenates the output into a finalized `final_dataset.parquet`.
 