# Exercice 8 : Visualisation et agrégation des logs météo

# 1. Importations
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import pandas as pd

# 2. Session Spark
spark = SparkSession.builder \
    .appName("WeatherVisualization") \
    .getOrCreate()

# 3. Lecture depuis HDFS
hdfs_path = "/hdfs-data/*/*/*.json"
df = spark.read.json(hdfs_path)

print("Données chargées depuis HDFS:")
df.printSchema()
print(f"Nombre d'alertes: {df.count()}")

# 4. Conversion en Pandas pour visualisation
pdf = df.toPandas()

# 5. Visualisations
fig, axes = plt.subplots(2, 2, figsize=(15, 10))

# Graphique 1: Évolution température
if not pdf.empty:
    pdf['event_timestamp'] = pd.to_datetime(pdf['event_timestamp'])
    pdf.sort_values('event_timestamp', inplace=True)
    
    axes[0, 0].plot(pdf['event_timestamp'], pdf['temperature'], 'b-')
    axes[0, 0].set_title('Évolution de la température')
    axes[0, 0].set_xlabel('Temps')
    axes[0, 0].set_ylabel('Température (°C)')
    axes[0, 0].grid(True)

# Graphique 2: Évolution vitesse vent
axes[0, 1].plot(pdf['event_timestamp'], pdf['windspeed'], 'g-')
axes[0, 1].set_title('Évolution de la vitesse du vent')
axes[0, 1].set_xlabel('Temps')
axes[0, 1].set_ylabel('Vitesse (m/s)')
axes[0, 1].grid(True)

# Graphique 3: Alertes par niveau
alert_counts = pdf.groupby(['wind_alert_level', 'heat_alert_level']).size().unstack()
if not alert_counts.empty:
    alert_counts.plot(kind='bar', ax=axes[1, 0])
    axes[1, 0].set_title('Nombre d\'alertes par niveau')
    axes[1, 0].set_xlabel('Niveau alerte vent')
    axes[1, 0].set_ylabel('Nombre')
    axes[1, 0].legend(title='Alerte chaleur')

# Graphique 4: Code météo par pays
weather_by_country = pdf.groupby(['country', 'weathercode']).size().unstack().fillna(0)
if not weather_by_country.empty:
    weather_by_country.sum(axis=1).plot(kind='pie', ax=axes[1, 1], autopct='%1.1f%%')
    axes[1, 1].set_title('Répartition par pays')

plt.tight_layout()
plt.savefig('/home/jovyan/work/weather_visualizations.png')
plt.show()

print("Visualisations sauvegardées dans weather_visualizations.png")