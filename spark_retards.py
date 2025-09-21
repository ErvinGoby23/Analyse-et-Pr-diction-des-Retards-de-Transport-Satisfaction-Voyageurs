from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StringType

# 1. Créer la session Spark
spark = SparkSession.builder \
    .appName("AffichageEtSauvegardeRetardsSNCF") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Définir le schéma des messages JSON venant de Kafka
schema = StructType() \
    .add("creation_time", StringType()) \
    .add("situation_id", StringType()) \
    .add("participant", StringType()) \
    .add("progress", StringType()) \
    .add("summary", StringType()) \
    .add("description", StringType()) \
    .add("severity", StringType())

# 3. Lire le topic Kafka "retards"
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "retards") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parser le JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# 5. Nettoyage
df_clean = df_parsed \
    .dropna(how="all") \
    .withColumn("creation_time", to_timestamp("creation_time")) \
    .na.fill("N/A")

# 6a. Affichage en console
query_console = df_clean.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# 6b. Sauvegarde en JSON
query_json = df_clean.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "output_json/") \
    .option("checkpointLocation", "checkpoint_json/") \
    .start()

query_console.awaitTermination()
query_json.awaitTermination()
