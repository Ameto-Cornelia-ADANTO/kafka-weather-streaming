"""
EXERCICE 10 : Détection des records climatiques locaux
Objectif : Analyser l'historique pour trouver les records
"""

import json
from datetime import datetime
from collections import defaultdict
from kafka import KafkaProducer

class ClimateRecordsDetector:
    def __init__(self):
        self.records = defaultdict(lambda: {
            'hottest': {'temp': -100, 'date': None},
            'coldest': {'temp': 100, 'date': None},
            'windiest': {'speed': 0, 'date': None},
            'rainiest_period': {'days': 0, 'start': None, 'end': None}
        })
    
    def analyze_historical_data(self, city, country, historical_data):
        """Analyse les données historiques d'une ville"""
        city_key = f"{city}_{country}"
        
        current_rain_streak = 0
        current_start = None
        max_rain_streak = 0
        max_start = None
        max_end = None
        
        for day_data in historical_data:
            date = day_data.get('date')
            temp_max = day_data.get('temp_max')
            temp_min = day_data.get('temp_min')
            wind_max = day_data.get('wind_max')
            precipitation = day_data.get('precipitation', 0)
            
            # Records température
            if temp_max > self.records[city_key]['hottest']['temp']:
                self.records[city_key]['hottest'] = {'temp': temp_max, 'date': date}
            
            if temp_min < self.records[city_key]['coldest']['temp']:
                self.records[city_key]['coldest'] = {'temp': temp_min, 'date': date}
            
            # Record vent
            if wind_max > self.records[city_key]['windiest']['speed']:
                self.records[city_key]['windiest'] = {'speed': wind_max, 'date': date}
            
            # Période pluvieuse
            if precipitation > 5:  # Plus de 5mm de pluie
                if current_rain_streak == 0:
                    current_start = date
                current_rain_streak += 1
            else:
                if current_rain_streak > max_rain_streak:
                    max_rain_streak = current_rain_streak
                    max_start = current_start
                    max_end = date
                current_rain_streak = 0
        
        if current_rain_streak > max_rain_streak:
            max_rain_streak = current_rain_streak
            max_start = current_start
            max_end = historical_data[-1]['date'] if historical_data else None
        
        self.records[city_key]['rainiest_period'] = {
            'days': max_rain_streak,
            'start': max_start,
            'end': max_end
        }
    
    def get_records(self, city, country):
        """Retourne les records pour une ville"""
        city_key = f"{city}_{country}"
        return self.records.get(city_key, {})

def exercice10():
    print("=" * 80)
    print("EXERCICE 10 : DÉTECTION DES RECORDS CLIMATIQUES")
    print("=" * 80)
    
    # Simuler des données historiques (en pratique, lire depuis HDFS/Kafka)
    detector = ClimateRecordsDetector()
    
    # Données simulées pour Paris (10 jours)
    paris_data = [
        {'date': '2025-01-01', 'temp_max': 8.5, 'temp_min': 2.1, 'wind_max': 12.3, 'precipitation': 0},
        {'date': '2025-01-02', 'temp_max': 10.2, 'temp_min': 3.4, 'wind_max': 15.6, 'precipitation': 2},
        {'date': '2025-01-03', 'temp_max': 9.8, 'temp_min': 4.2, 'wind_max': 18.9, 'precipitation': 15},
        {'date': '2025-01-04', 'temp_max': 7.3, 'temp_min': 1.8, 'wind_max': 22.4, 'precipitation': 20},
        {'date': '2025-01-05', 'temp_max': 6.5, 'temp_min': 0.5, 'wind_max': 25.1, 'precipitation': 18},
        {'date': '2025-01-06', 'temp_max': 5.8, 'temp_min': -1.2, 'wind_max': 20.3, 'precipitation': 12},
        {'date': '2025-01-07', 'temp_max': 12.3, 'temp_min': 5.6, 'wind_max': 14.7, 'precipitation': 5},
        {'date': '2025-01-08', 'temp_max': 15.6, 'temp_min': 8.9, 'wind_max': 10.2, 'precipitation': 0},
        {'date': '2025-01-09', 'temp_max': 18.2, 'temp_min': 10.4, 'wind_max': 8.5, 'precipitation': 0},
        {'date': '2025-01-10', 'temp_max': 20.5, 'temp_min': 12.1, 'wind_max': 7.3, 'precipitation': 0},
    ]
    
    detector.analyze_historical_data("Paris", "France", paris_data)
    records = detector.get_records("Paris", "France")
    
    print("📊 RECORDS CLIMATIQUES - PARIS")
    print("-" * 60)
    print(f"🌡️ Jour le plus chaud:")
    print(f"   {records['hottest']['temp']}°C le {records['hottest']['date']}")
    
    print(f"❄️ Jour le plus froid:")
    print(f"   {records['coldest']['temp']}°C le {records['coldest']['date']}")
    
    print(f"💨 Rafale de vent la plus forte:")
    print(f"   {records['windiest']['speed']} m/s le {records['windiest']['date']}")
    
    print(f"🌧️ Période la plus pluvieuse:")
    print(f"   {records['rainiest_period']['days']} jours")
    print(f"   Du {records['rainiest_period']['start']} au {records['rainiest_period']['end']}")
    print("-" * 60)
    
    # Publication dans Kafka
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    message = {
        'city': 'Paris',
        'country': 'France',
        'records': records,
        'detection_time': datetime.now().isoformat(),
        'analysis_period': '10 days sample'
    }
    
    producer.send('weather_records', message)
    producer.flush()
    
    print("✅ Records publiés dans Kafka (topic: weather_records)")
    
    return records

if __name__ == "__main__":
    exercice10()