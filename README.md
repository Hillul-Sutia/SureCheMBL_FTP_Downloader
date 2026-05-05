# SureChEMBL Data Pipeline 🚀

## 📌 Overview
This project implements a **modular ETL pipeline** for ingesting and processing large-scale chemical datasets from the SureChEMBL FTP repository.

The pipeline is designed to handle **high-volume data ingestion**, with support for parallel downloads, fault tolerance, and configurable execution.

---

## ⚙️ Features
- Automated data ingestion from FTP directories
- Parallel file downloads using multi-threading
- Config-driven pipeline execution
- Logging for monitoring and debugging
- Retry mechanism for fault tolerance
- Modular architecture (Extract → Load, Transform-ready)

---

## 🏗️ Architecture
src/ \
│── extractor.py # Extract: discover directories and files \
│── downloader.py # Load: parallel download with retry logic \
│── utils.py # Logging and configuration utilities 

main.py # Pipeline orchestration \
config.yaml # Configurable parameters


---

## 🔄 Pipeline Flow

1. **Extract**
   - Scrape FTP directory structure
   - Identify relevant `.parquet` files (filtered by keyword)

2. **Load**
   - Download files in parallel using multi-threading
   - Save data into structured local directories

3. **(Future) Transform**
   - Designed to integrate data transformation (e.g., using Pandas)

---

## 🧰 Tech Stack

- **Python**
- **Requests** (HTTP data ingestion)
- **BeautifulSoup** (HTML parsing)
- **ThreadPoolExecutor** (parallel processing)
- **YAML** (configuration management)
- **Logging** (pipeline monitoring)

---

## ⚡ Performance Highlights

- Multi-threaded downloads significantly reduce ingestion time for large datasets
- Efficient handling of I/O-bound workloads using concurrent execution
- Scalable structure capable of handling millions of records

---

## 📂 Configuration

All pipeline parameters are configurable via `config.yaml`:

```yaml
base_url: https://ftp.ebi.ac.uk/pub/databases/chembl/SureChEMBL/bulk_data/
num_workers: 8
download_dir: data/
file_filter: compounds
timeout: 10
retries: 3
```
## ▶️ How to Run
1. Install dependencies \
```pip install -r requirements.txt```
2. Run pipeline \
```python main.py```