"""
EXERCICE 5 : Agrégats avec Kafka Python (sans Spark)
"""

from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime, timedelta
import time

class WeatherAggregator:
    def __init__(self, window_minutes=5, slide_minutes=1):
        self.window_minutes = window_minutes
        self.slide_minutes = slide_minutes
        self.data = defaultdict(list)
        self.last_calculation = datetime.now()
    
    def add_message(self, message):
        """Ajoute un message aux données"""
        city = message.get('city', 'Unknown')
        country = message.get('country', 'Unknown')
        key = f"{city}_{country}"
        
        self.data[key].append({
            'timestamp': datetime.now(),
            'temperature': message.get('temperature', 0),
            'windspeed': message.get('windspeed', 0),
            'wind_alert': message.get('wind_alert_level', 'level_0'),
            'heat_alert': message.get('heat_alert_level', 'level_0')
        })
    
    def calculate_aggregates(self):
        """Calcule les agrégats pour la fenêtre"""
        now = datetime.now()
        window_start = now - timedelta(minutes=self.window_minutes)
        
        aggregates = {}
        
        for key, messages in self.data.items():
            # Filtrer messages dans la fenêtre
            window_messages = [
                m for m in messages 
                if m['timestamp'] >= window_start
            ]
            
            if window_messages:
                temps = [m['temperature'] for m in window_messages]
                winds = [m['windspeed'] for m in window_messages]
                wind_alerts = sum(1 for m in window_messages if m['wind_alert'] != 'level_0')
                heat_alerts = sum(1 for m in window_messages if m['heat_alert'] != 'level_0')
                
                aggregates[key] = {
                    'city_country': key,
                    'message_count': len(window_messages),
                    'avg_temperature': sum(temps) / len(temps) if temps else 0,
                    'min_temperature': min(temps) if temps else 0,
                    'max_temperature': max(temps) if temps else 0,
                    'avg_windspeed': sum(winds) / len(winds) if winds else 0,
                    'wind_alerts': wind_alerts,
                    'heat_alerts': heat_alerts,
                    'window_start': window_start.isoformat(),
                    'window_end': now.isoformat()
                }
        
        # Nettoyer anciennes données
        self._clean_old_data()
        
        return aggregates
    
    def _clean_old_data(self):
        """Nettoie les données trop anciennes"""
        cutoff = datetime.now() - timedelta(minutes=self.window_minutes + 5)
        for key in list(self.data.keys()):
            self.data[key] = [
                m for m in self.data[key] 
                if m['timestamp'] > cutoff
            ]
            if not self.data[key]:
                del self.data[key]

def main():
    print("=" * 80)
    print("EXERCICE 5 : AGRÉGATS KAFKA (SANS SPARK)")
    print("=" * 80)
    
    # Initialiser l'agrégateur
    aggregator = WeatherAggregator(window_minutes=5, slide_minutes=1)
    
    # Consumer pour weather_transformed
    consumer = KafkaConsumer(
        'weather_transformed',
        bootstrap_servers=['localhost:29092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='aggregator_group'
    )
    
    print("✅ Agrégateur démarré")
    print("   Fenêtre: 5 minutes, glissement: 1 minute")
    print("   Lecture depuis: weather_transformed")
    print("\nCalcul des agrégats toutes les 30 secondes...")
    
    try:
        for message in consumer:
            # Ajouter message
            aggregator.add_message(message.value)
            
            # Calculer agrégats toutes les 30 secondes
            if (datetime.now() - aggregator.last_calculation).total_seconds() >= 30:
                aggregates = aggregator.calculate_aggregates()
                aggregator.last_calculation = datetime.now()
                
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] AGRÉGATS:")
                print("-" * 60)
                
                for key, agg in aggregates.items():
                    print(f"📍 {agg['city_country']}:")
                    print(f"   📊 Messages: {agg['message_count']}")
                    print(f"   🌡️ Temp: {agg['avg_temperature']:.1f}°C "
                          f"(min:{agg['min_temperature']:.1f}, max:{agg['max_temperature']:.1f})")
                    print(f"   💨 Vent: {agg['avg_windspeed']:.1f} m/s")
                    print(f"   ⚠️ Alertes: Vent={agg['wind_alerts']}, Chaleur={agg['heat_alerts']}")
                    print()
                
                print("-" * 60)
            
    except KeyboardInterrupt:
        print("\nArrêt de l'agrégateur")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()