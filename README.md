# Azure Data Engineering Pipeline

## Overview

This project demonstrates an end-to-end Azure Data Engineering pipeline that ingests raw customer and order data, performs data transformation using Azure Databricks (PySpark), and loads curated data into Azure Synapse Analytics.

## Architecture

Azure Blob Storage
        ↓
Azure Data Factory
        ↓
Azure Databricks (PySpark)
        ↓
Delta Tables
        ↓
Azure Synapse Analytics

## Technologies Used

- Azure Data Factory
- Azure Databricks
- PySpark
- Azure Blob Storage
- Azure Synapse Analytics
- SQL
- Delta Lake

## Project Flow

1. Load CSV files into Azure Blob Storage.
2. Trigger pipeline using Azure Data Factory.
3. Process raw data in Azure Databricks.
4. Cleanse and transform data using PySpark.
5. Load curated data into Azure Synapse Analytics.
6. Validate data using SQL.

## Dataset

olist_products_dataset.csv
olist_orders_dataset.csv
olist_order_reviews_dataset.csv
olist_order_items_dataset.csv
olist_geolocation_dataset.csv
olist_customers_dataset.csv
olist_sellers_dataset.csv

## Future Enhancements

- Incremental Data Loading
- Delta MERGE
- Error Logging
- Pipeline Monitoring
