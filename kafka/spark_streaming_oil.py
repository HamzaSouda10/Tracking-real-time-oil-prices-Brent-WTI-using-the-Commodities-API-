from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, window, avg, stddev, lag
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.window import Window

# Création de la Spark Session
spark = SparkSession.builder \
    .appName("OilPriceStreaming") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/oil.raw") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schéma des données envoyées par le Producer
schema = StructType() \
    .add("price", DoubleType()) \
    .add("currency", StringType()) \
    .add("symbol", StringType()) \
    .add("timestamp", StringType())  # On le convertira en TimestampType ensuite

# Lecture depuis Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "oil_prices") \
    .option("startingOffsets", "latest") \
    .load()

# Traitement : conversion JSON + ajout du timestamp d'arrivée
df_json = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        col("data.price").alias("price"),
        col("data.currency").alias("currency"),
        col("data.symbol").alias("symbol"),
        col("data.timestamp").cast(TimestampType()).alias("api_timestamp"),
        current_timestamp().alias("ingestion_time")
    )

# Écriture dans MongoDB (raw)
df_json.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/checkpoints/raw") \
    .option("database", "oil") \
    .option("collection", "raw") \
    .outputMode("append") \
    .start()

# Calcul KPIs : variation de prix sur 1h et 24h, volatilité
# Fenêtre de 1 heure glissante toutes les 10 minutes
df_kpi = df_json \
    .withWatermark("api_timestamp", "25 hours") \
    .groupBy(window("api_timestamp", "1 hour")) \
    .agg(
        avg("price").alias("avg_price"),
        stddev("price").alias("volatility")
    )

# Écriture des KPIs dans une autre collection MongoDB
df_kpi.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/checkpoints/kpis") \
    .option("database", "oil") \
    .option("collection", "kpis") \
    .outputMode("update") \
    .start()

spark.streams.awaitAnyTermination()
