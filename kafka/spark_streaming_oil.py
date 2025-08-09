from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, window, avg, stddev, first, last, expr, desc
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.window import Window


# Création de la Spark Session
spark = SparkSession.builder \
    .appName("OilPriceStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,""org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/oil") \
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
    .option("kafka.bootstrap.servers", "kafka:29092") \
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

# 🔹 Écriture des données brutes dans MongoDB
df_json.writeStream \
    .format("mongodb") \
    .option("uri", "mongodb://mongodb:27017/oil") \
    .option("checkpointLocation", "/tmp/checkpoints/raw") \
    .option("database", "oil") \
    .option("collection", "raw") \
    .outputMode("append") \
    .start()

# 🔹 Calcul des KPIs sur 1 heure glissante (toutes les 10 minutes)
df_kpi = df_json \
    .withWatermark("api_timestamp", "1 hour") \
    .groupBy(window("api_timestamp", "5 minutes")) \
    .agg(
        first("price").alias("first_price"),
        last("price").alias("last_price"),
        avg("price").alias("avg_price"),
        stddev("price").alias("volatility")
    ) \
    .withColumn("variation_1h_pct", expr("((last_price - first_price) / first_price) * 100")) \
    .withColumn("alert_1h", expr("CASE WHEN abs(variation_1h_pct) > 5 THEN 'ALERT' ELSE 'OK' END"))

# 🔹 Écriture des KPIs dans MongoDB
df_kpi.writeStream \
    .format("mongodb") \
    .option("uri", "mongodb://mongodb:27017/oil") \
    .option("checkpointLocation", "/tmp/checkpoints/kpis") \
    .option("database", "oil") \
    .option("collection", "kpis") \
    .outputMode("complete") \
    .start()

# 🔹 Dernier prix connu (échantillon de 1 ligne)
"""df_latest = df_json \
    .withWatermark("api_timestamp", "10 minutes") \
    .groupBy(window("api_timestamp", "1 minute")) \
    .agg(last("price").alias("last_price"), last("api_timestamp").alias("last_timestamp"))

df_latest.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/checkpoints/latest") \
    .option("database", "oil") \
    .option("collection", "latest_price") \
    .outputMode("update") \
    .start()
"""
spark.streams.awaitAnyTermination()
