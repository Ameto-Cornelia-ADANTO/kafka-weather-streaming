"""
EXERCICE 4 : Transformation avec Kafka Python (sans Spark)
"""

from kafka import KafkaConsumer, KafkaProducer # type: ignore
import json
import time
from datetime import datetime

def transform_message(message):
    """Transforme un message avec calcul d'alertes"""
    data = message.value
    
    # Calcul des alertes
    windspeed = data.get('windspeed', 0)
    temperature = data.get('temperature', 0)
    
    # Alerte vent
    if windspeed < 10:
        wind_alert = "level_0"
    elif windspeed <= 20:
        wind_alert = "level_1"
    else:
        wind_alert = "level_2"
    
    # Alerte chaleur
    if temperature < 25:
        heat_alert = "level_0"
    elif temperature <= 35:
        heat_alert = "level_1"
    else:
        heat_alert = "level_2"
    
    # Message transformé
    transformed = {
        'event_time': data.get('event_time', datetime.now().isoformat()),
        'temperature': temperature,
        'windspeed': windspeed,
        'wind_alert_level': wind_alert,
        'heat_alert_level': heat_alert,
        'city': data.get('city', 'Unknown'),
        'country': data.get('country', 'Unknown'),
        'latitude': data.get('latitude'),
        'longitude': data.get('longitude'),
        'processing_time': datetime.now().isoformat(),
        'source_message': data
    }
    
    return transformed

def main():
    print("=" * 80)
    print("EXERCICE 4 : TRANSFORMATION KAFKA (SANS SPARK)")
    print("=" * 80)
    
    # Consumer pour lire weather.stream
    consumer = KafkaConsumer(
        'weather.stream',
        bootstrap_servers=['localhost:29092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='transformer_group'
    )
    
    # Producer pour écrire weather_transformed
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print("✅ Transformateur Kafka démarré")
    print("   Input: weather.stream")
    print("   Output: weather_transformed")
    print("\nEn attente de messages... (Ctrl+C pour arrêter)")
    
    message_count = 0
    
    try:
        for message in consumer:
            # Transformation
            transformed = transform_message(message)
            
            # Envoi vers weather_transformed
            producer.send('weather_transformed', transformed)
            producer.flush()
            
            message_count += 1
            
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Message #{message_count}")
            print(f"  Temp: {transformed['temperature']}°C → Alerte: {transformed['heat_alert_level']}")
            print(f"  Vent: {transformed['windspeed']} m/s → Alerte: {transformed['wind_alert_level']}")
            print(f"  Ville: {transformed['city']}, {transformed['country']}")
            
    except KeyboardInterrupt:
        print(f"\nArrêt. Total messages transformés: {message_count}")
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    # Créer le topic si nécessaire
    import subprocess
    subprocess.run([
        "docker", "exec", "kafka",
        "kafka-topics", "--create", "--if-not-exists",
        "--topic", "weather_transformed",
        "--bootstrap-server", "kafka:9092",
        "--partitions", "3",
        "--replication-factor", "1"
    ], capture_output=True)
    
    main()