from kafka import KafkaProducer # type: ignore
import json

def main():
    print("=" * 60)
    print("EXERCICE 1 : Mise en place de Kafka et d'un producteur simple")
    print("=" * 60)
    
    # Configuration pour la stack du professeur
    # Port 29092 pour l'hôte (depuis Windows)
    bootstrap_servers = 'localhost:29092'
    
    print(f"Connexion à Kafka sur: {bootstrap_servers}")
    print(f"Topic: weather.stream")
    print("-" * 60)
    
    try:
        # Créer le producteur
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(2, 0, 2),
            retries=3,
            request_timeout_ms=5000
        )
        
        # Message statique comme demandé dans l'exercice
        message = {"msg": "Hello Kafka"}
        
        print(f"Envoi du message: {message}")
        
        # Envoyer le message
        future = producer.send('weather.stream', message)
        
        # Attendre la confirmation (timeout 10 secondes)
        metadata = future.get(timeout=10)
        
        print("\n" + "✓" * 60)
        print("SUCCÈS ! Message envoyé avec succès.")
        print("-" * 60)
        print(f"Topic: {metadata.topic}")
        print(f"Partition: {metadata.partition}")
        print(f"Offset: {metadata.offset}")
        print(f"Message: {message}")
        print("✓" * 60)
        
        # Nettoyer
        producer.flush()
        producer.close()
        
        print("\n" + "=" * 60)
        print("INSTRUCTIONS POUR VÉRIFIER:")
        print("=" * 60)
        print("\n1. Avec Docker (dans ce terminal):")
        print("   docker exec kafka kafka-console-consumer \\")
        print("     --topic weather.stream \\")
        print("     --bootstrap-server kafka:9092 \\")
        print("     --from-beginning \\")
        print("     --timeout-ms 3000")
        
        print("\n2. Avec Python (Exercice 2 - nouvelle fenêtre):")
        print("   python exercise2.py weather.stream")
        
        print("\n3. Vérification du topic:")
        print("   docker exec kafka kafka-topics --describe --topic weather.stream --bootstrap-server kafka:9092")
        
    except Exception as e:
        print("\n" + "✗" * 60)
        print("ERREUR !")
        print("-" * 60)
        print(f"Message d'erreur: {type(e).__name__}: {e}")
        print("\nVÉRIFIEZ QUE:")
        print("1. Les conteneurs sont démarrés: docker-compose ps")
        print("2. Kafka est prêt (voir 'started' dans les logs)")
        print("3. Le topic existe: docker exec kafka kafka-topics --list --bootstrap-server kafka:9092")
        print("4. Vous utilisez le bon port: localhost:29092 (pas 9092 depuis l'hôte)")
        print("✗" * 60)
        
        # Suggestions de dépannage
        print("\n" + "-" * 60)
        print("DÉPANNAGE RAPIDE:")
        print("-" * 60)
        print("Test de connexion manuel:")
        print("  docker exec kafka bash -c \"echo '{\\\"test\\\":\\\"manual\\\"}' | kafka-console-producer --topic weather.stream --bootstrap-server kafka:9092\"")
        print("  docker exec kafka kafka-console-consumer --topic weather.stream --bootstrap-server kafka:9092 --from-beginning --timeout-ms 2000")

if __name__ == "__main__":
    main()