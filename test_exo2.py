# test_exo2.py
from kafka import KafkaConsumer # type: ignore
import json

print("Test simple de lecture Kafka...")
print("Topic: weather.stream")

try:
    consumer = KafkaConsumer(
        'weather.stream',
        bootstrap_servers=['localhost:29092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=5000
    )
    
    print("Connexion établie. Lecture...")
    
    for msg in consumer:
        print(f"\nMessage trouvé:")
        print(f"  Partition: {msg.partition}")
        print(f"  Offset: {msg.offset}")
        print(f"  Données: {msg.value}")
    
    consumer.close()
    print("\nTest terminé.")
    
except Exception as e:
    print(f"Erreur: {e}")
    print("\nEssayez avec le port 9092...")
    
    try:
        consumer = KafkaConsumer(
            'weather.stream',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=5000
        )
        
        print("Connexion avec port 9092 établie. Lecture...")
        
        for msg in consumer:
            print(f"\nMessage trouvé:")
            print(f"  Partition: {msg.partition}")
            print(f"  Offset: {msg.offset}")
            print(f"  Données: {msg.value}")
        
        consumer.close()
        
    except Exception as e2:
        print(f"Erreur avec port 9092: {e2}")