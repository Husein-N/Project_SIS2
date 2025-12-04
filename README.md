
# Data_collection_project_SIS2
# Technology Stocks ETL Pipeline (Airflow + Playwright + SQLite)

A simple end-to-end ETL pipeline that:
- Scrapes Technology Stock data from Investing.com using **Playwright**
- Cleans and transforms the dataset with **Pandas**
- Loads the processed data into a **SQLite** database
- Automates the entire workflow with **Apache Airflow**


## 📦 Project Structure
project/
│ README.md
│ requirements.txt
│ airflow_dag.py
│
├── src/
│ ├── scraper.py
│ ├── cleaner.py
│ └── loader.py
│
└── data/
└── output.db



#### Install dependencies
pip install -r requirements.txt
playwright install chromium

####🛠️ Tech Stack

Python

Playwright

Pandas

SQLite

Apache Airflow

✔️ Summary

This project demonstrates a real-world ETL pipeline with:

Dynamic web scraping

Data cleaning

Database storage

Workflow automation

