"""
EXERCICE 9 : Récupération de séries historiques longues
"""

import requests
import json
from kafka import KafkaProducer
import time
from datetime import datetime, timedelta

def get_historical_weather(city, country, start_date, end_date):
    """Récupère des données historiques"""
    # Géocoding
    geo_url = "https://geocoding-api.open-meteo.com/v1/search"
    geo_params = {"name": city, "count": 1}
    
    geo_response = requests.get(geo_url, params=geo_params)
    geo_data = geo_response.json()
    
    if not geo_data.get("results"):
        print(f"Ville non trouvée: {city}")
        return []
    
    lat = geo_data["results"][0]["latitude"]
    lon = geo_data["results"][0]["longitude"]
    
    # Données historiques
    history_url = "https://archive-api.open-meteo.com/v1/archive"
    history_params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_max,temperature_2m_min,windspeed_10m_max",
        "timezone": "auto"
    }
    
    response = requests.get(history_url, params=history_params)
    return response.json()

def main():
    print("=" * 80)
    print("EXERCICE 9 : SÉRIES HISTORIQUES")
    print("=" * 80)
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Exemple: 10 ans de données pour Paris
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365*10)).strftime("%Y-%m-%d")
    
    print(f"Période: {start_date} à {end_date}")
    print("Récupération des données...")
    
    data = get_historical_weather("Paris", "France", start_date, end_date)
    
    if 'daily' in data:
        daily = data['daily']
        
        for i in range(len(daily['time'])):
            message = {
                "type": "historical",
                "date": daily['time'][i],
                "city": "Paris",
                "country": "France",
                "temp_max": daily['temperature_2m_max'][i],
                "temp_min": daily['temperature_2m_min'][i],
                "wind_max": daily['windspeed_10m_max'][i],
                "source": "archive-api"
            }
            
            producer.send('weather_history_raw', message)
            print(f"Envoi: {message['date']}")
            time.sleep(0.1)  # Pour ne pas surcharger
        
        producer.flush()
        print(f"✅ {len(daily['time'])} jours de données envoyés")
    
    producer.close()

if __name__ == "__main__":
    main()