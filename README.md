# ETL: Weekly Aggregation of Trades
![ETL Workflow](https://github.com/8karpov/etl-project/actions/workflows/main.yml/badge.svg)

Automated ETL Project: Data Loading, Transformation, and Visualization with CI/CD via GitHub Actions
## Description
This project demonstrates a simple ETL process that includes:
- loading trade data from a CSV file;
- cleaning and normalizing the dataset;
- aggregating by week_start_date, client_type, user_id, and symbol;
- calculating key metrics: total_volume, avg_price, trade_count, and total_pnl;
- saving the results to a local database (agg_result.db), as well as exporting a report (top 3 bronze clients by total_volume and total_pnl) and generating visualizations.

## Folder Structure
```plaintext
etl-project/
├── .github/
│ └── workflows/
│  └── main.yml # CI/CD Pipeline (GitHub Actions) — configuration for automated testing and deployment
├── data/
│ └── trades.csv # source data files (CSV input)
├── output/
│ ├── agg_result.db # database containing aggregated ETL results
│ ├── top_clients.csv # top 3 bronze clients with the highest total_volume and total_pnl
│ ├── weekly_volume.png # weekly trading volume chart
│ └── symbols_total_volume.png # chart showing trading volume by asset (from the database)
├─ scripts/
│  └── etl.py             # main working pipeline
├── requirements.txt
├─ notebooks/
│  └── notebook.ipynb     # initial draft used as a prototype for building the etl.py file
├─ README.md
├─ automated_etl_project.pdf     # project presentation 
└─ .gitignore
```

# 1. Running the ETL Manually
## I. Installing Dependencies / Requirements
Install required packages:
```plaintext
pip install -r requirements.txt
```

Minimum dependencies:
pandas
matplotlib
openpyxl

## ІІ. Running the Script
```plaintext
python scripts/etl.py \
  --data data/trades.csv \
  --db output/agg_result.db \
  --table agg_trades_weekly \
  --top output/top_clients \
  --chart-weekly output/weekly_volume.png \
  --chart-symbols output/symbols_total_volume.png \
  --null-timestamp drop
```
## Command-Line Arguments
- `--data` — path to the source CSV file.
- `--db` — path to the database file containing aggregation results.
- `--table` — name of the target table in the database.
- `--top` — path to the report with the top 3 bronze clients by total_volume and total_pnl.
- `--chart-weekly` — path to the chart showing weekly trading volume dynamics.
- `--chart-symbols` — path to the chart showing trading volume distribution by symbols.
- `--no-pnl` — disable PnL calculation.
- `--null-timestamp` — specify how to handle missing timestamps (drop, fill, or error).
## Results
- `agg_result.db` — SQLite database containing the table agg_trades_weekly.
- `top_clients.csv` и `.xlsx` — report with the top 3 bronze clients by total_volume and total_pnl.
- `weekly_volume.png` — chart showing the weekly trading volume dynamics.
- `symbols_total_volume.png` — chart showing the trading volume of each asset available in the database.

# 2. How CI/CD Works
  The CI/CD pipeline is implemented using GitHub Actions in the main.yml file located in .github/workflows/
## Triggers:
- on: push (triggered on every push to the repository.) and workflow_dispatch (allows manual execution of the workflow).
## Pipeline Steps:
- Checkout the repository code
- Set up Python and install dependencies from requirements.txt
- Run the etl.py script with the specified arguments
- Save the generated artifacts (.db, .csv, .png) to the output/ directory and upload them as workflow artifacts
## Possible Improvements:
- Implement deployment or data transfer to a data warehouse
- Schedule automatic workflow runs (e.g., daily or weekly)

# 3. How I Would Scale the Solution for 100M+ Rows
## Data Storage and Format
- CSV → Parquet/Delta — switch to a columnar format with compression and predicate pushdown.
- Local files → Object storage (S3/GCS/Azure Blob) with partitioning by date or week (year=YYYY/month=MM/week=WW).
- Layered data model: bronze (raw) → silver (cleansed) → gold (aggregated). 
```plain text
This bronze/silver/gold structure refers to data layers,
not to client types present in our dataset (client_type: gold, silver, bronze).
```
## Compute Engine
- Pandas → Polars (lazy) or Dask for handling tens of millions of rows.
- For further scaling — Apache Spark or Databricks for distributed batch and streaming data processing.

## Orchestration and Scheduling
- Move from a one-time GitHub Actions workflow to Apache Airflow (or Prefect) with DAGs, retries, SLA monitoring, and incremental jobs (using watermarks).
## Data Quality Validation
- Add Great Expectations or Pandera to define validation schemas and rules (data types, NULL rate, value ranges) and to generate data quality reports for each layer.
## Serving and Analytics
- Replace SQLite with a proper data warehouse (BigQuery / Snowflake / Redshift / ClickHouse).
- Connect Tableau to the gold layer or directly to the DWH for dashboards like Weekly Volume, Top Symbols, and PnL, with automatic extract refresh on schedule.
## Monitoring and Alerting
- Export pipeline metrics to Prometheus, visualize and alert via Grafana.
- Key metrics: task duration, throughput, error rate, data freshness, %NULL, rejected rows, P95/P99 latency, SLA breaches.
## Infrastructure and Delivery
- Use Docker for containerization and Kubernetes (or managed Spark) for orchestration and scaling.
- Store artifacts and logs in Object Storage with centralized logging via ELK or OpenSearch.
## Migration Mini-Plan (Step-by-Step)
``` plain text
Step 1: Migrate CSV → Parquet with partitioning in S3/GCS; switch from Pandas → Polars (lazy).
Step 2: Introduce bronze/silver/gold data layers and validation rules (Great Expectations).
Step 3: Move aggregated data to a DWH (BigQuery / Snowflake / Redshift / ClickHouse).
Step 4: Connect Tableau to the DWH (gold layer) and configure extract refresh schedules.
Step 5: Implement orchestration with Airflow (DAGs, retries, incremental loads).
Step 6: Export metrics to Prometheus and set up monitoring/alerting in Grafana.
```

# 4. Technologies to Replace or Add
- Data Format: CSV → Parquet/Delta (columnar format, compression, predicate pushdown)
- File Storage (Data Lake): local → S3 / GCS / Azure Blob
- Compute Engine: Pandas → Polars (lazy) / Dask → at larger scale, Apache Spark (or Databricks)
- DWH / Serving Layer: SQLite → BigQuery / Snowflake / Redshift / ClickHouse
- Orchestration: GitHub Actions (CI only) → Apache Airflow (or Prefect) for DAGs, retries, and dependencies
- Data Validation: add Great Expectations / Pandera
- Visualization: built-in charts → Tableau (business dashboards)
- Monitoring: logs → Grafana (+ Prometheus metrics and alerts)

# 5. Proposed ETL Architecture
Model: Bronze → Silver → Gold + Orchestrator (Airflow)
## I. Bronze (Raw / Ingestion)
- Ingest raw data (files / API / stream) → store in S3 in Parquet/Delta format.
- Partition data by date (year=YYYY/month=MM/week=WW).
- Add metadata cataloging (AWS Glue / Unity Catalog, depending on the platform).
## II. Silver (Cleansed / Normalized)
- Perform data cleaning, type casting, deduplication, and normalization of fields such as timestamp, client_type, symbol, and side.
- Export cleansed data back to S3 in Parquet/Delta format, preserving the same partition structure.
## III. Gold (Aggregations / Serving)
- Aggregate data by week_start_date, client_type, user_id, and symbol.
- Calculate key metrics: total_volume, trade_count, avg_price, and total_pnl.
- Load the aggregated data into a DWH (BigQuery / Snowflake / Redshift / ClickHouse) for BI and analytics.
- Tableau connects to the Gold layer (or directly to the DWH) for interactive dashboards.
## IV. Orchestration and Scheduling
- Airflow DAG: orchestrates Bronze → Silver → Gold tasks, manages retries, SLA checks, and emits metrics.
- Supports incremental processing — only new or updated partitions are processed (via watermarking).
## V. Reporting and Delivery
- Static exports: store in S3 or shared storage and provide direct links.
- Dashboards: hosted on Tableau Server/Cloud with scheduled extract refresh.

# 6. ETL Monitoring Metrics
Collect metrics (via Prometheus exporter or Airflow → StatsD/Prometheus) and visualize them in Grafana.
## I. Data Quality:
- % of null values in key columns (timestamp, user_id, symbol, price, quantity)
- % of invalid data types / parsing errors
- % of late events and data freshness lag (max(event_time) vs. now)
- Share of dropped rows at each processing stage
- Sudden drops or spikes in total_volume, trade_count, or total_pnl
- Missing data for expected partitions, clients, or symbols
## II. Performance:
- Task duration (end-to-end and per stage), including P50/P95/P99 latency percentiles
- Throughput: rows/sec and bytes/sec
- Resource utilization: CPU and memory usage — peak and average values
## III. Reliability:
- Error rate per task or stage
- Number of retries and failed runs
- SLA breaches and missed schedules
- DAG / task status: success or failure indicators
  
```plaintext
In Grafana: configure alert thresholds (errors, null value rate, freshness) and automatic notifications via Slack or Email.
```

# 7. Data Storage — Input and Output
## I. Locally (MVP version):
- Input: data/*.csv
- Output: output/*.db, output/*.csv, output/*.png
## II. Production version:
- Data Lake: S3 / GCS / Azure Blob — Parquet or Delta format with partitioning
- DWH / Serving Layer: BigQuery / Snowflake / Redshift / ClickHouse — gold layer, source for Tableau dashboards
- Catalogs / Metadata: AWS Glue / Unity Catalog / Data Catalog (if needed)
- Report Artifacts: stored in S3 or shared storage + Tableau dashboards with scheduled refresh
