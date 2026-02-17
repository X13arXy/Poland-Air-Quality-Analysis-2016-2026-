import requests
import pandas as pd
from datetime import date
import time

# ==========================================
# CONFIGURATION
# ==========================================
START_DATE = "2016-01-01"
END_DATE = str(date.today())
OUTPUT_FILENAME = 'dane_polska_2004_2026.csv' # Keeping filename consistent with ETL script requirements

# List of major Polish cities with coordinates
CITIES = {
    "Warszawa": {"lat": 52.23, "lon": 21.01},
    "Kraków": {"lat": 50.06, "lon": 19.94},
    "Łódź": {"lat": 51.75, "lon": 19.46},
    "Wrocław": {"lat": 51.11, "lon": 17.03},
    "Poznań": {"lat": 52.41, "lon": 16.92},
    "Gdańsk": {"lat": 54.35, "lon": 18.65},
    "Szczecin": {"lat": 53.43, "lon": 14.55},
    "Bydgoszcz": {"lat": 53.12, "lon": 18.00},
    "Lublin": {"lat": 51.25, "lon": 22.57},
    "Białystok": {"lat": 53.13, "lon": 23.16},
    "Katowice": {"lat": 50.26, "lon": 19.02},
    "Gdynia": {"lat": 54.52, "lon": 18.53},
    "Częstochowa": {"lat": 50.81, "lon": 19.12},
    "Radom": {"lat": 51.40, "lon": 21.15},
    "Toruń": {"lat": 53.01, "lon": 18.60},
    "Sosnowiec": {"lat": 50.28, "lon": 19.10},
    "Kielce": {"lat": 50.87, "lon": 20.63},
    "Rzeszów": {"lat": 50.04, "lon": 21.99},
    "Gliwice": {"lat": 50.29, "lon": 18.67},
    "Zabrze": {"lat": 50.32, "lon": 18.79}
}

def safe_request(url, retries=5, wait_time=10):
    """
    Robust request function with retry logic and error handling.
    """
    for attempt in range(retries):
        try:
            # Increased timeout for large data requests
            response = requests.get(url, timeout=90)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429: 
                print(f"⚠️ API Rate Limit hit! Waiting {wait_time}s...")
                time.sleep(wait_time)
            elif response.status_code >= 500: 
                print(f"⚠️ Server error. Attempt {attempt+1}/{retries}...")
                time.sleep(wait_time)
            else:
                print(f"⚠️ HTTP Error {response.status_code}")
                time.sleep(2)
                
        except Exception as e:
            print(f"⚠️ Network Error (Attempt {attempt+1}/{retries}): {e}")
            time.sleep(wait_time)
            
    return None

def get_historical_data(city_name, lat, lon):
    """
    Fetches weather and air quality data for a specific city.
    Merges daily weather data with aggregated hourly air quality data.
    """
    print(f"⏳ {city_name}: Fetching data...", end=" ")
    
    # 1. Weather API URL (Daily metrics)
    weather_url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}&start_date={START_DATE}&end_date={END_DATE}"
        f"&daily=temperature_2m_mean,wind_speed_10m_max,precipitation_sum&timezone=Europe%2FWarsaw"
    )
    
    # 2. Air Quality API URL (Hourly metrics)
    air_url = (
        f"https://air-quality-api.open-meteo.com/v1/air-quality?"
        f"latitude={lat}&longitude={lon}&start_date={START_DATE}&end_date={END_DATE}"
        f"&hourly=pm10,pm2_5&timezone=Europe%2FWarsaw"
    )

    # Fetch Weather Data
    w_data = safe_request(weather_url)
    if not w_data:
        print("❌ Weather API Error")
        return pd.DataFrame()

    time.sleep(1) # Respect API rate limits

    # Fetch Air Quality Data
    a_data = safe_request(air_url)
    if not a_data:
        print("❌ Air Quality API Error")
        return pd.DataFrame()

    try:
        # Process Weather (Already Daily)
        df_weather = pd.DataFrame(w_data['daily'])

        # Process Air Quality (Hourly -> Resample to Daily Mean)
        if 'hourly' not in a_data:
            print("❌ Missing hourly data")
            return pd.DataFrame()
            
        df_air_hourly = pd.DataFrame(a_data['hourly'])
        df_air_hourly['time'] = pd.to_datetime(df_air_hourly['time'])
        
        # Resample hourly data to daily averages
        df_air_daily = df_air_hourly.set_index('time').resample('D').mean().reset_index()
        df_air_daily['time'] = df_air_daily['time'].dt.strftime('%Y-%m-%d')

        # Merge datasets
        final_df = pd.merge(df_weather, df_air_daily, on='time')
        final_df['city'] = city_name

        print(f"✅ Success ({len(final_df)} records)")
        return final_df

    except Exception as e:
        print(f"❌ Data Processing Error: {e}")
        return pd.DataFrame()

def run_scraper():
    print(f"🚀 STARTING DATA EXTRACTION ({START_DATE} - {END_DATE})")
    print("☕ This may take a few minutes...")
    
    all_data = []

    for i, (city, coords) in enumerate(CITIES.items(), 1):
        print(f"[{i}/{len(CITIES)}]", end=" ")
        df = get_historical_data(city, coords['lat'], coords['lon'])
        if not df.empty:
            all_data.append(df)
        
        # Pause to be polite to the API
        time.sleep(2) 

    if all_data:
        total_df = pd.concat(all_data, ignore_index=True)
        
        # Export to CSV
        total_df.to_csv(OUTPUT_FILENAME, index=False, encoding='utf-8-sig')
        
        print("\n" + "="*40)
        print(f"🏆 PROCESS COMPLETED!")
        print(f"📊 Total Rows Fetched: {len(total_df)}")
        print(f"💾 File Saved: {OUTPUT_FILENAME}")
        print("="*40)
    else:
        print("\n❌ Failed to fetch data.")

if __name__ == "__main__":
    run_scraper()