"""
EXERCICE 6 : Producteur avec géocoding
"""

from kafka import KafkaProducer
import requests
import json
import time
import sys

def get_coordinates(city, country):
    """Récupère les coordonnées d'une ville"""
    url = "https://geocoding-api.open-meteo.com/v1/search"
    params = {"name": city, "count": 1, "format": "json"}
    
    try:
        response = requests.get(url, params=params, timeout=5)
        data = response.json()
        
        if data.get("results"):
            result = data["results"][0]
            return result["latitude"], result["longitude"], result.get("country", country)
        
        # Fallback pour villes connues
        cities = {
            "paris": (48.8566, 2.3522, "France"),
            "lyon": (45.7640, 4.8357, "France"),
            "marseille": (43.2965, 5.3698, "France"),
            "toulouse": (43.6047, 1.4442, "France"),
            "london": (51.5074, -0.1278, "United Kingdom"),
            "berlin": (52.5200, 13.4050, "Germany"),
            "madrid": (40.4168, -3.7038, "Spain")
        }
        
        return cities.get(city.lower(), (48.8566, 2.3522, "France"))
        
    except:
        return 48.8566, 2.3522, "France"

def main():
    if len(sys.argv) < 2:
        print("Usage: python exercice6_extended.py <ville> [pays] [interval]")
        print("Exemples:")
        print("  python exercice6_extended.py Paris")
        print("  python exercice6_extended.py London UK 45")
        print("  python exercice6_extended.py Berlin Germany 30")
        sys.exit(1)
    
    city = sys.argv[1]
    country = sys.argv[2] if len(sys.argv) > 2 else "France"
    interval = int(sys.argv[3]) if len(sys.argv) > 3 else 60
    
    print("=" * 80)
    print(f"PRODUCTEUR AVEC GÉOCODING: {city}, {country}")
    print("=" * 80)
    
    # Récupération coordonnées
    lat, lon, detected_country = get_coordinates(city, country)
    print(f"📍 Coordonnées: {lat}, {lon}")
    print(f"🇺🇳 Pays détecté: {detected_country}")
    print(f"⏱️ Intervalle: {interval} secondes")
    
    # Producteur Kafka
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    message_count = 0
    
    try:
        while True:
            # Données météo
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": lat,
                "longitude": lon,
                "current_weather": "true",
                "windspeed_unit": "ms"
            }
            
            response = requests.get(url, params=params)
            data = response.json()
            
            if 'current_weather' in data:
                current = data['current_weather']
                
                message = {
                    "city": city,
                    "country": detected_country,
                    "latitude": lat,
                    "longitude": lon,
                    "temperature": current['temperature'],
                    "windspeed": current['windspeed'],
                    "winddirection": current['winddirection'],
                    "weathercode": current['weathercode'],
                    "event_time": current['time'],
                    "timestamp": time.time(),
                    "source": "open-meteo-geocoded"
                }
                
                producer.send('weather.stream', message)
                producer.flush()
                
                message_count += 1
                
                print(f"\n[{time.strftime('%H:%M:%S')}] Message #{message_count}")
                print(f"  🌆 {city}, {detected_country}")
                print(f"  🌡️ {message['temperature']}°C")
                print(f"  💨 {message['windspeed']} m/s")
                print(f"  🧭 Direction: {message['winddirection']}°")
                print("-" * 40)
            
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print(f"\nArrêt. Total messages: {message_count}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()