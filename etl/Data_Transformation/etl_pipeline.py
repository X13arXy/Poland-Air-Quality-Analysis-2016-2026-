import pandas as pd
import numpy as np
import os

# ==========================================
# 1. SETUP & DATA LOADING
# ==========================================
file_path = 'dane_polska_2004_2026.csv'

# Check if the dataset exists to prevent runtime errors
if not os.path.exists(file_path):
    raise FileNotFoundError(f"File not found: {file_path}")

# Load raw meteorological and pollution data
df = pd.read_csv(file_path)
df['time'] = pd.to_datetime(df['time'])

# Filter data: Focus on the last decade (2016-2026) for relevant trend analysis
df = df[df['time'].dt.year >= 2016].copy()

# Feature Engineering: Extract temporal attributes for aggregation
df['Year'] = df['time'].dt.year
df['Month'] = df['time'].dt.month
df['Month_Name'] = df['time'].dt.month_name()
df['Day_Name'] = df['time'].dt.day_name()
df['Day_of_Week'] = df['time'].dt.dayofweek  # 0=Monday, 6=Sunday

print(f"Data loaded successfully: {len(df)} rows.")

# ==========================================
# 2. PAGE 1: OVERVIEW & TRENDS (ETL)
# ==========================================

# A. Yearly Trend Analysis
# Purpose: Check if air quality is improving over the years
yearly_trend = df.groupby('Year')['pm10'].mean().reset_index()
yearly_trend.columns = ['Year', 'Avg_PM10']
yearly_trend.to_csv('pbi_trend_yearly.csv', index=False)

# B. Monthly Seasonality
# Purpose: Identify the "Heating Season" pattern (Winter vs Summer)
monthly_trend = df.groupby(['Month', 'Month_Name'])['pm10'].mean().reset_index()
monthly_trend.columns = ['Month_Num', 'Month_Name', 'Avg_PM10']
monthly_trend = monthly_trend.sort_values('Month_Num')
monthly_trend.to_csv('pbi_seasonality.csv', index=False)

# C. Top 10 Polluted Cities Ranking
# Logic: Count days where PM10 exceeded the WHO/EU threshold (> 50 µg/m³)
city_stats = df[df['pm10'] > 50].groupby('city').size().reset_index(name='Dni_Smogowe')
city_stats = city_stats.sort_values('Dni_Smogowe', ascending=False).head(10)
city_stats.to_csv('pbi_ranking_cities.csv', index=False)

# ==========================================
# 3. PAGE 2: PHYSICS & WEATHER FACTORS (ETL)
# ==========================================

# A. Wind Curve Analysis
# Hypothesis: Stronger wind disperses smog (Ventilation effect)
df['Wind_Bin'] = df['wind_speed_10m_max'].round().astype(int)
wind_curve = df.groupby('Wind_Bin')['pm10'].mean().reset_index()
wind_curve = wind_curve[wind_curve['Wind_Bin'] <= 60]  # Filter out extreme outliers (hurricanes)
wind_curve.columns = ['Wind_Speed_kmh', 'Avg_PM10']
wind_curve.to_csv('pbi_page2_wind_curve.csv', index=False)

# B. Temperature Critical Point Analysis
# Hypothesis: Identify the temperature threshold where heating starts (Hockey Stick effect)
df['Temp_Bin'] = df['temperature_2m_mean'].round().astype(int)
temp_curve = df.groupby('Temp_Bin')['pm10'].mean().reset_index()
temp_curve = temp_curve[(temp_curve['Temp_Bin'] >= -20) & (temp_curve['Temp_Bin'] <= 30)]
temp_curve.columns = ['Temperature_C', 'Avg_PM10']
temp_curve.to_csv('pbi_page2_temp_curve.csv', index=False)

# C. Rain Effect Analysis (Wet vs Dry)
# Logic: Compare PM10 levels during rain vs no rain (Wet Deposition)
df['Rain_Condition'] = np.where(df['precipitation_sum'] > 0.1, 'Deszcz (Mokro)', 'Brak Deszczu (Sucho)')
rain_effect = df.groupby('Rain_Condition')['pm10'].mean().reset_index()
rain_effect.to_csv('pbi_page2_rain_effect.csv', index=False)

# D. Scenario Duel: Temperature vs Wind
# Logic: Prove that wind is more critical than temperature by comparing specific conditions
def classify_scenario(row):
    t = row['temperature_2m_mean']
    w = row['wind_speed_10m_max']
    
    # 1. Cold & Calm -> Smog accumulation (The worst case)
    if t < 5 and w < 10: return "1. ZIMNO I BEZWIETRZNIE"
    # 2. Cold & Windy -> Ventilation (Clean air despite cold)
    if t < 5 and w > 20: return "2. ZIMNO Z WIATREM"
    # 3. Summer -> Background baseline
    if t > 15: return "3. LATO"
    return "4. INNE"

df['Scenario'] = df.apply(classify_scenario, axis=1)
scenarios = df.groupby('Scenario')['pm10'].mean().reset_index()
scenarios = scenarios[scenarios['Scenario'] != "4. INNE"] # Remove noise/background data
scenarios.to_csv('pbi_page2_scenarios_clean.csv', index=False)

# ==========================================
# 4. PAGE 3: ANOMALIES & PATTERNS (ETL)
# ==========================================
print("Generating files for Page 3 (Anomalies)...")

# A. Weekend Effect
# Hypothesis: Lower industrial/traffic activity on weekends leads to cleaner air
weekend_effect = df.groupby(['Day_of_Week', 'Day_Name'])['pm10'].mean().reset_index()
weekend_effect['Typ_Dnia'] = np.where(weekend_effect['Day_of_Week'] >= 5, 'Weekend', 'Dzień Roboczy')
weekend_effect.to_csv('pbi_page3_weekend.csv', index=False)

# B. New Year's Eve Effect (Fireworks)
# Logic: Isolate the specific period around Dec 31 to detect pollution spikes
def get_newyear_label(row):
    m, d = row['Month'], row['time'].day
    if m == 12 and d == 29: return "1. 29 Grudnia"
    if m == 12 and d == 30: return "2. 30 Grudnia"
    if m == 12 and d == 31: return "3. SYLWESTER"
    if m == 1 and d == 1:   return "4. NOWY ROK"
    if m == 1 and d == 2:   return "5. 2 Stycznia"
    if m == 1 and d == 3:   return "6. 3 Stycznia"
    return None

df['NewYear_Label'] = df.apply(get_newyear_label, axis=1)
new_year_stats = df.dropna(subset=['NewYear_Label']).groupby('NewYear_Label')['pm10'].mean().reset_index()
new_year_stats.to_csv('pbi_page3_newyear_ready.csv', index=False)

# C. Heating Season Impact
# Logic: Compare Heating Season (Oct-Mar) vs Non-Heating Season
df['Sezon'] = np.where(df['Month'].isin([10, 11, 12, 1, 2, 3]), "1. SEZON GRZEWCZY", "2. POZA SEZONEM")
season_stats = df.groupby('Sezon')['pm10'].mean().reset_index()
season_stats.to_csv('pbi_page3_heating_season.csv', index=False)

print("✅ SUCCESS! All CSV files for Power BI have been generated.")