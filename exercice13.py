"""
EXERCICE 13 : Détection d'anomalies climatiques
Objectif : Comparer données temps réel avec profils historiques
"""

import json
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
import time

class AnomalyDetector:
    def __init__(self, historical_profiles):
        self.profiles = historical_profiles
        self.anomaly_thresholds = {
            'temperature': 5.0,  # °C d'écart
            'windspeed_std': 2.0,  # 2 écarts-types
            'alert_frequency': 0.3  # 30% d'écart
        }
    
    def detect_anomalies(self, realtime_data, city, country, month):
        """Détecte les anomalies par rapport au profil historique"""
        city_key = f"{city}_{country}"
        profile = self.profiles.get(city_key, {}).get(month, {})
        
        if not profile:
            return None
        
        anomalies = []
        
        # 1. Anomalie température
        current_temp = realtime_data.get('temperature')
        historical_avg_temp = profile.get('avg_temperature')
        
        if current_temp is not None and historical_avg_temp is not None:
            temp_diff = abs(current_temp - historical_avg_temp)
            if temp_diff > self.anomaly_thresholds['temperature']:
                anomalies.append({
                    'type': 'temperature_anomaly',
                    'variable': 'temperature',
                    'observed': current_temp,
                    'expected': historical_avg_temp,
                    'deviation': temp_diff,
                    'threshold': self.anomaly_thresholds['temperature'],
                    'severity': 'high' if temp_diff > 10 else 'medium'
                })
        
        # 2. Anomalie vent (écart-type)
        current_wind = realtime_data.get('windspeed')
        historical_avg_wind = profile.get('avg_windspeed')
        historical_std_wind = profile.get('temperature_stats', {}).get('std', 1.0)
        
        if current_wind is not None and historical_avg_wind is not None:
            wind_zscore = abs(current_wind - historical_avg_wind) / historical_std_wind
            if wind_zscore > self.anomaly_thresholds['windspeed_std']:
                anomalies.append({
                    'type': 'wind_anomaly',
                    'variable': 'windspeed',
                    'observed': current_wind,
                    'expected': historical_avg_wind,
                    'z_score': wind_zscore,
                    'threshold': self.anomaly_thresholds['windspeed_std'],
                    'severity': 'high' if wind_zscore > 3 else 'medium'
                })
        
        # 3. Anomalie fréquence alertes
        current_has_alert = realtime_data.get('has_alert', False)
        historical_alert_prob = profile.get('alert_probability', 0)
        
        if current_has_alert:
            alert_diff = 1.0 - historical_alert_prob  # 100% vs probabilité historique
            if alert_diff > self.anomaly_thresholds['alert_frequency']:
                anomalies.append({
                    'type': 'alert_frequency_anomaly',
                    'variable': 'alert',
                    'observed': 1.0,  # 100% car alerte présente
                    'expected': historical_alert_prob,
                    'deviation': alert_diff,
                    'threshold': self.anomaly_thresholds['alert_frequency'],
                    'severity': 'high' if alert_diff > 0.5 else 'medium'
                })
        
        return anomalies

def exercice13():
    print("=" * 80)
    print("EXERCICE 13 : DÉTECTION D'ANOMALIES CLIMATIQUES")
    print("=" * 80)
    
    # Charger les profils historiques
    profile_file = "hdfs-data/France/Paris/seasonal_profile_enriched/2025/profile.json"
    
    try:
        with open(profile_file, 'r') as f:
            data = json.load(f)
        historical_profiles = {'Paris_France': data.get('enriched_profile', {})}
        print("✅ Profils historiques chargés")
    except:
        print("⚠️ Profils non trouvés, utilisation de profils simulés")
        historical_profiles = {'Paris_France': {
            1: {'avg_temperature': 5.0, 'avg_windspeed': 12.0, 'alert_probability': 0.1},
            7: {'avg_temperature': 20.0, 'avg_windspeed': 8.0, 'alert_probability': 0.05}
        }}
    
    # Initialiser le détecteur
    detector = AnomalyDetector(historical_profiles)
    
    # Consumer pour weather_transformed
    consumer = KafkaConsumer(
        'weather_transformed',
        bootstrap_servers=['localhost:29092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='anomaly_detector_group'
    )
    
    # Producer pour weather_anomalies
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print("🔍 Détecteur d'anomalies démarré")
    print("   Source: weather_transformed")
    print("   Destination: weather_anomalies")
    print("\nEn attente de données... (Ctrl+C pour arrêter)")
    print("-" * 80)
    
    anomaly_count = 0
    
    try:
        for message in consumer:
            data = message.value
            
            # Extraire informations
            city = data.get('city', 'Paris')
            country = data.get('country', 'France')
            month = datetime.now().month  # Mois actuel
            
            # Préparer données pour détection
            realtime_data = {
                'temperature': data.get('temperature'),
                'windspeed': data.get('windspeed'),
                'has_alert': data.get('wind_alert_level') != 'level_0' or data.get('heat_alert_level') != 'level_0'
            }
            
            # Détection d'anomalies
            anomalies = detector.detect_anomalies(realtime_data, city, country, month)
            
            if anomalies:
                for anomaly in anomalies:
                    anomaly_count += 1
                    
                    # Construire message d'anomalie
                    anomaly_message = {
                        'city': city,
                        'country': country,
                        'event_time': datetime.now().isoformat(),
                        'variable': anomaly['variable'],
                        'observed_value': anomaly['observed'],
                        'expected_value': anomaly['expected'],
                        'anomaly_type': anomaly['type'],
                        'severity': anomaly.get('severity', 'unknown'),
                        'deviation': anomaly.get('deviation', anomaly.get('z_score')),
                        'threshold': anomaly['threshold'],
                        'source_data': data
                    }
                    
                    # Publier dans Kafka
                    producer.send('weather_anomalies', anomaly_message)
                    producer.flush()
                    
                    # Afficher
                    print(f"\n🚨 ANOMALIE #{anomaly_count} DÉTECTÉE!")
                    print(f"   📍 {city}, {country}")
                    print(f"   ⚠️ Type: {anomaly['type']}")
                    print(f"   📊 Observé: {anomaly['observed']}")
                    print(f"   📈 Attendu: {anomaly['expected']}")
                    print(f"   🎯 Écart: {anomaly.get('deviation', 'N/A')}")
                    print(f"   ⚡ Sévérité: {anomaly.get('severity', 'unknown')}")
                    print(f"   🕐 {datetime.now().strftime('%H:%M:%S')}")
                    print("-" * 50)
                    
                    # Sauvegarde dans HDFS simulé
                    import os
                    year = datetime.now().year
                    month_dir = f"hdfs-data/{country}/{city}/anomalies/{year}/{month:02d}/"
                    os.makedirs(month_dir, exist_ok=True)
                    
                    anomaly_file = os.path.join(month_dir, f"anomaly_{anomaly_count}.json")
                    with open(anomaly_file, 'w') as f:
                        json.dump(anomaly_message, f, indent=2)
                    
                    print(f"   💾 Sauvegardé dans: {anomaly_file}")
            
            # Attente courte pour éviter saturation
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        print(f"\nArrêt du détecteur. Anomalies détectées: {anomaly_count}")
    finally:
        consumer.close()
        producer.close()
        
        # Rapport final
        print("\n" + "=" * 80)
        print("RAPPORT FINAL DE DÉTECTION D'ANOMALIES")
        print("-" * 80)
        print(f"Anomalies détectées: {anomaly_count}")
        print(f"Profils utilisés: {len(historical_profiles)} ville(s)")
        print(f"Seuils appliqués:")
        print(f"  - Température: ±{detector.anomaly_thresholds['temperature']}°C")
        print(f"  - Vent: {detector.anomaly_thresholds['windspeed_std']} écarts-types")
        print(f"  - Alertes: {detector.anomaly_thresholds['alert_frequency']*100}% d'écart")
        print("=" * 80)

if __name__ == "__main__":
    # Créer les topics nécessaires
    import subprocess
    
    topics = ['weather_anomalies', 'weather_records', 'seasonal_profiles']
    for topic in topics:
        subprocess.run([
            "docker", "exec", "kafka",
            "kafka-topics", "--create", "--if-not-exists",
            "--topic", topic,
            "--bootstrap-server", "kafka:9092",
            "--partitions", "3",
            "--replication-factor", "1"
        ], capture_output=True)
    
    exercice13()