
# Auction Tracker

[![Python Version](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 🚀 Project Overview
This project automates monitoring auction data and updating a Google Sheet with the scraped results. It consists of two main scripts:
- `main.py` — scrapes auction listings, and save data to google sheets
- `sheet_automation.py` — get updates from gmail and and add the Sold to 3rd Party Properties with Final Bid and Surplus Amount

## 📥 Clone the Repository
```bash
git clone https://github.com/hassaanmustafavi/auction_tracker
cd auction_tracker
```

## 🧾 Prerequisites
- Python 3.10+
- Access to the required secrets files (provided via Upwork)
- Working internet connection for scraping and Google Sheets API

## 🛠 Install UV (Package Manager)
If you do not have `uv` installed, install it with:
```bash
pip install uv
```

## 📦 Install Dependencies
Once `uv` is installed, sync the environment:
```bash
uv sync
```

## 🔐 Add Required Secrets
Create a folder named `secrets` in the root of the project (parallel to the `src` folder).

Add the following two files inside `secrets` (shared via Upwork):
- `auction_accounts.json`
- `sheets_credentials.json`

Your project structure should look like:
```
auction_tracker/
│
├── src/
│   ├── auction_scraper.py
│   └── sheet_updater.py
├── secrets/
│   ├── auction_accounts.json
│   └── sheets_credentials.json
└── README.md
```

## 📄 Usage

### ✅ Run Auction Scraper
```bash
uv run python src/auction_scraper.py
```

### ✅ Run Google Sheets Updater
```bash
uv run python src/sheet_updater.py
```

> ⚠️ Ensure both secret files are present and dependencies have been installed before running.
