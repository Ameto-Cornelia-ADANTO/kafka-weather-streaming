"""
EXERCICE 7 : Stockage dans HDFS organisé
Objectif : Lire weather_transformed et sauvegarder dans HDFS
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def exercice7():
    print("=" * 80)
    print("EXERCICE 7 : STOCKAGE DANS HDFS ORGANISÉ")
    print("=" * 80)
    
    spark = SparkSession.builder \
        .appName("HDFSStorage") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
        .getOrCreate()
    
    # Lecture depuis Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "weather_transformed") \
        .load()
    
    # Schéma
    schema = StructType([
        StructField("event_timestamp", TimestampType()),
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("temperature", FloatType()),
        StructField("windspeed", FloatType()),
        StructField("wind_alert_level", StringType()),
        StructField("heat_alert_level", StringType())
    ])
    
    # Parsing
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Filtrer seulement les alertes (level_1 et level_2)
    alerts_df = parsed_df.filter(
        (col("wind_alert_level") != "level_0") | 
        (col("heat_alert_level") != "level_0")
    )
    
    # Écriture dans HDFS avec structure organisée
    query = alerts_df.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", "/hdfs-data/") \
        .option("checkpointLocation", "/tmp/exercice7") \
        .partitionBy("country", "city") \
        .start()
    
    print("✅ Stockage HDFS démarré")
    print("   Structure: /hdfs-data/(country)/(city)/")
    print("   Format: JSON")
    print("   Filtrage: Alertes level_1 et level_2 seulement")
    print("\nAccès HDFS:")
    print("   Web: http://localhost:9870")
    print("   Commande: hdfs dfs -ls /hdfs-data/")
    
    return query

if __name__ == "__main__":
    query = exercice7()
    query.awaitTermination()