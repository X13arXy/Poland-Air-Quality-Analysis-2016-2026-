# 🌪️ Poland Air Quality Analysis (2016-2026)
### 🚀 End-to-End Data Analytics Project: From Raw Data to Power BI Insights

> **"Does rain wash away smog? Is wind more important than temperature?"** > This project answers these questions using 10 years of meteorological data from major Polish cities.

---

## 📊 Dashboard Preview
*(Add a screenshot of your main dashboard page here)*
![Dashboard Screenshot](03_Visualization/screenshot_main.png)

👉 **[Interact with the Live Report]( https://app.powerbi.com/groups/me/reports/5884a3c3-73ce-44ef-8cae-406ff0f2336e/dbe81537da3fc3a4a028?experience=power-bi)**

---

## 🎯 Project Overview
The goal of this project was to analyze the correlation between weather conditions (wind, rain, temperature) and air pollution (PM10) in Poland. Instead of using a ready-made dataset, I built a full data pipeline.

### Key Features:
* **Data Collection:** Custom Python scraper fetching historical weather & pollution data via API.
* **ETL Pipeline:** Data cleaning, handling missing values, and Feature Engineering (classifying weather scenarios) using **Pandas**.
* **Visualization:** Interactive **Power BI** report with dynamic DAX measures and a dark-mode UI.

---

## 💡 Key Insights (What data tells us?)
1.  **Wind is King:** Ventilation is the #1 factor in reducing smog. Wind speeds above **16 km/h** reduce PM10 levels by over **60%**, even during freezing temperatures.
2.  **The "Hockey Stick" Effect:** Pollution levels skyrocket once the daily average temperature drops below **10°C** (start of the heating season).
3.  **The Weekend Effect:** Air quality improves by **~15%** on Saturdays and Sundays due to reduced traffic and industrial activity.
4.  **"New Year's Eve" Anomaly:** A massive spike in PM10 is detected every year on Jan 1st at 00:00 due to fireworks.

---

## 🛠️ Tech Stack & Tools

| Category | Tools Used |
|----------|------------|
| **Data Extraction** | Python (`requests`, `datetime`) |
| **Data Transformation** | Python (`pandas`, `numpy`) |
| **Visualization** | Power BI (DAX, Bookmarks, Page Navigation) |
| **Version Control** | Git & GitHub |

---

## 📂 Repository Structure

```text
├── 01_Data_Scraping/          # Python scripts for fetching data from Open-Meteo API
│   ├── data_scraper.py
│   └── requirements.txt
│
├── 02_Data_Cleaning_ETL/      # Data processing & Feature Engineering
│   ├── etl_pipeline.py        # Logic for weather scenarios & aggregations
│   └── requirements.txt
│
├── 03_Visualization/          # Power BI files & Screenshots
│   ├── dashboard_air_quality.pbix
│   ├── screenshot_main.png
│   
│
└── README.md                  # Project Documentation
