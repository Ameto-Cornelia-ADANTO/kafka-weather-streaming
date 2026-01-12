from kafka import KafkaProducer # type: ignore
import requests # type: ignore
import json
import time
import sys
from datetime import datetime

def get_weather_data(lat, lon):
    """Récupère les données météo depuis Open-Meteo API"""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current_weather": "true",
        "windspeed_unit": "ms",  # m/s comme demandé dans les exercices
        "timezone": "auto"
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"⚠️ Erreur API: {e}")
        return None

def main(latitude, longitude, interval=60):
    print("╔" + "═" * 68 + "╗")
    print("║" + " " * 68 + "║")
    print("║" + "EXERCICE 3 : STREAMING DE DONNÉES MÉTÉO EN DIRECT".center(68) + "║")
    print("║" + " " * 68 + "║")
    print("╚" + "═" * 68 + "╝")
    print()
    
    print("🎯 OBJECTIF:")
    print("   Interroger l'API Open-Meteo et envoyer les données à Kafka")
    print()
    print("📍 POSITION:")
    print(f"   Latitude: {latitude}")
    print(f"   Longitude: {longitude}")
    print()
    print("⚙️  CONFIGURATION:")
    print(f"   Intervalle: {interval} secondes")
    print(f"   API: Open-Meteo (https://api.open-meteo.com)")
    print(f"   Topic Kafka: weather.stream")
    print(f"   Broker: localhost:29092")
    print()
    print("=" * 70)
    print("DÉMARRAGE DU STREAMING... (Ctrl+C pour arrêter)")
    print("=" * 70)
    
    try:
        # Initialisation du producteur Kafka
        producer = KafkaProducer(
            bootstrap_servers=['localhost:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(2, 0, 2),
            acks='all',
            retries=3
        )
        
        message_count = 0
        
        while True:
            try:
                # Récupération des données météo
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Requête API...")
                weather_data = get_weather_data(latitude, longitude)
                
                if weather_data and 'current_weather' in weather_data:
                    current = weather_data['current_weather']
                    
                    # Construction du message
                    message = {
                        'exercise': 3,
                        'timestamp': time.time(),
                        'event_time': current.get('time', ''),
                        'latitude': latitude,
                        'longitude': longitude,
                        'temperature': current.get('temperature'),
                        'windspeed': current.get('windspeed'),  # en m/s
                        'winddirection': current.get('winddirection'),
                        'weathercode': current.get('weathercode'),
                        'is_day': current.get('is_day', 1),
                        'source': 'open-meteo',
                        'city': 'Paris' if abs(latitude-48.8566) < 1 else 'Autre',
                        'country': 'France'
                    }
                    
                    # Envoi à Kafka
                    future = producer.send('weather.stream', message)
                    metadata = future.get(timeout=10)
                    
                    message_count += 1
                    
                    # Affichage des résultats
                    print("✅ DONNÉES ENVOYÉES:")
                    print("   " + "-" * 40)
                    print(f"   🌡️ Température: {message['temperature']}°C")
                    print(f"   💨 Vent: {message['windspeed']} m/s")
                    print(f"   🧭 Direction: {message['winddirection']}°")
                    print(f"   ☁️ Code météo: {message['weathercode']}")
                    print("   " + "-" * 40)
                    print(f"   📊 Kafka: Partition {metadata.partition}, Offset {metadata.offset}")
                    print(f"   📈 Total messages: {message_count}")
                    print("   " + "=" * 40)
                    
                else:
                    print("❌ Données météo non disponibles")
                
                # Attente avant la prochaine requête
                print(f"⏳ Prochaine requête dans {interval} secondes...")
                time.sleep(interval)
                
            except KeyboardInterrupt:
                raise  # Relancer l'interruption
            except Exception as e:
                print(f"⚠️ Erreur temporaire: {e}")
                time.sleep(10)  # Attendre 10 secondes en cas d'erreur
                
    except KeyboardInterrupt:
        print("\n" + "=" * 70)
        print("ARRÊT DU STREAMING")
        print("-" * 70)
        print(f"Total messages envoyés: {message_count}")
        print(f"Position: ({latitude}, {longitude})")
        print(f"Durée: Terminé à {datetime.now().strftime('%H:%M:%S')}")
        print("=" * 70)
        
        # Instructions pour la vérification
        print("\n➡️ POUR VÉRIFIER:")
        print("   Ouvrez une nouvelle fenêtre et exécutez:")
        print("   python exercise2.py weather.stream")
        
    except Exception as e:
        print(f"\n❌ ERREUR FATALE: {e}")
        print("Vérifiez votre connexion internet et Kafka")
        
    finally:
        try:
            producer.close()
        except:
            pass

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python exercise3.py <latitude> <longitude> [interval]")
        print()
        print("Exemples de villes:")
        print("   Paris:    python exercise3.py 48.8566 2.3522 30")
        print("   Lyon:     python exercise3.py 45.7640 4.8357 30")
        print("   Marseille: python exercise3.py 43.2965 5.3698 30")
        print("   Toulouse: python exercise3.py 43.6047 1.4442 30")
        print()
        print("Paramètre optionnel 'interval': temps entre les requêtes (défaut: 60s)")
        sys.exit(1)
    
    # Récupération des arguments
    lat = float(sys.argv[1])
    lon = float(sys.argv[2])
    interval = int(sys.argv[3]) if len(sys.argv) > 3 else 60
    
    main(lat, lon, interval)