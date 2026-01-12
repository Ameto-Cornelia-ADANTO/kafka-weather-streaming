from kafka import KafkaConsumer
import json
import sys
from datetime import datetime

def main(topic_name):
    print("=" * 70)
    print("EXERCICE 2 : ÉCRITURE D'UN CONSOMMATEUR KAFKA".center(70))
    print("=" * 70)
    
    # Configuration - utilisez 29092 (hôte) ou 9092 si ça ne marche pas
    bootstrap_servers = 'localhost:29092'
    
    print(f"📡 CONFIGURATION:")
    print(f"   Topic: {topic_name}")
    print(f"   Broker: {bootstrap_servers}")
    print(f"   Début: {datetime.now().strftime('%H:%M:%S')}")
    print(f"   Groupe: exercise2_group")
    print("-" * 70)
    
    try:
        # Créer le consommateur
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=[bootstrap_servers],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=15000,  # 15 secondes
            api_version=(2, 0, 2),
            group_id='exercise2_group'
        )
        
        print("✅ Connexion établie")
        print("👂 En écoute des messages... (15 secondes max, Ctrl+C pour arrêter)")
        print("-" * 70)
        
        message_count = 0
        
        try:
            for message in consumer:
                message_count += 1
                
                print(f"\n📨 MESSAGE #{message_count}")
                print("   " + "-" * 40)
                print(f"   Partition: {message.partition}")
                print(f"   Offset: {message.offset}")
                
                # Afficher le contenu formaté
                data = message.value
                if isinstance(data, dict):
                    for key, value in data.items():
                        print(f"   {key}: {value}")
                else:
                    print(f"   Données: {data}")
                
                print("   " + "-" * 40)
                
        except KeyboardInterrupt:
            print("\n⏹️ Arrêt manuel (Ctrl+C)")
        
        finally:
            consumer.close()
            
            print("\n" + "=" * 70)
            print("📊 RÉSUMÉ")
            print("-" * 70)
            print(f"   Messages lus: {message_count}")
            print(f"   Topic: {topic_name}")
            print(f"   Temps: {datetime.now().strftime('%H:%M:%S')}")
            print("=" * 70)
            
            if message_count == 0:
                print("\n⚠️ Aucun message trouvé.")
                print("   Vérifiez que des messages ont été envoyés.")
                print("   Test: python exercise1.py")
            
    except Exception as e:
        print(f"\n❌ ERREUR: {type(e).__name__}")
        print(f"   Message: {e}")
        print("\n🔧 ESSAYEZ:")
        print("   1. Changer le port: 'localhost:9092' au lieu de 'localhost:29092'")
        print("   2. Vérifier Kafka: docker-compose ps")
        print("   3. Test manuel: docker exec kafka kafka-console-consumer --topic weather.stream --bootstrap-server kafka:9092 --from-beginning --timeout-ms 3000")

# ⚠️ CETTE PARTIE EST TRÈS IMPORTANTE ⚠️
if __name__ == "__main__":
    # Vérifier les arguments
    if len(sys.argv) != 2:
        print("❌ Usage incorrect!")
        print("   Utilisation: python exercise2.py <topic_name>")
        print("   Exemple: python exercise2.py weather.stream")
        print()
        print("   Pour vérifier les topics disponibles:")
        print("   docker exec kafka kafka-topics --list --bootstrap-server kafka:9092")
        sys.exit(1)
    
    # Lancer le programme
    main(sys.argv[1])