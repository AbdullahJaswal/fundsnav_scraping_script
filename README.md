# FundsNav Scraping Script

## Overview
AWS Lambda function designed for scraping mutual fund data from [MUFAP](https://www.mufap.com.pk/) every 24 hours. It extracts details like fund names, categories, asset management companies (AMCs), and market capitalization, updating this data into a PostgreSQL database.

## Prerequisites
- AWS Lambda environment setup.
- Python 3.8 or higher.
- Necessary Python libraries including `requests`, `beautifulsoup4`, `psycopg2`, etc.
