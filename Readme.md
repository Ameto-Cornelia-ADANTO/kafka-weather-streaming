# Projet Kafka - Streaming Météo

## Réalisation des exercices

### ✅ Exercices 1-3 COMPLÉTÉS ET FONCTIONNELS
- Exercice 1: Producteur simple ✓
- Exercice 2: Consommateur ✓  
- Exercice 3: Streaming météo en direct ✓

### 🔧 Exercices 4-13 - Scripts Prêts
Les scripts pour les exercices 4 à 13 sont fournis et prêts à l'exécution.
La stack Docker complète (Kafka, Spark, HDFS, Jupyter) est configurée.

## Installation
1. `docker-compose up -d`
2. `python exercice1.py` (test connexion)
3. `python exercice3.py 48.8566 2.3522 30` (streaming)

## Structure
- `exercice1.py` à `exercice3.py`: Fonctionnels
- `exercice4_spark.py` à `exercice13_anomalies.py`: Scripts Spark/Kafka
- `docker-compose.yml`: Configuration complète

# 1. Installer les dépendances
pip install -r requirements.txt

# 2. Démarrer la stack
docker-compose up -d

# 3. Exécuter dans l'ordre
python exercice1.py                      # Test connexion
python exercice3.py 48.8566 2.3522 60    # Streaming météo
python exercice4.py               # Transformation
python exercice6.py Paris      # Producteur géocoding
python exercice10.py            # Records historiques
python exercice11.py       # Profils saisonniers
python exercice12.py         # Validation
python exercice13.py          # Détection anomalies