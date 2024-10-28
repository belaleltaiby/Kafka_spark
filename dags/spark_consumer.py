from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType,IntegerType

# Define Spark session
spark = SparkSession.builder \
    .appName("KafkaCSVConsumer") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Define schema for the incoming Kafka messages
schema = StructType([
    StructField("Suburb", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("Rooms", IntegerType(), True),
    StructField("Type", StringType(), True),
    StructField("Price", IntegerType(), True),
    StructField("Method", StringType(), True),
    StructField("SellerG", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Postcode", IntegerType(), True),
    StructField("Regionname", StringType(), True),
    StructField("Propertycount", IntegerType(), True),
    StructField("Distance", IntegerType(), True),
    StructField("CouncilArea", StringType(), True)
    # Add fields as per your CSV structure
])

# Read data from Kafka topic 'csv_topic'
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "csv_topic") \
    .load()

# Parse Kafka message
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Transformation logic
# (e.g., filtering rows or adding new columns)
transformed_df = df.filter(col("column1") != "some_value")

# Save data locally (as a checkpoint for streaming)
query = transformed_df.writeStream \
    .format("csv") \
    .option("path", "F:\data_engineering_videos\My_kafka_spark\dags\data") \
    .option("checkpointLocation", "F:\data_engineering_videos\My_kafka_spark\dags\data") \
    .start()

query.awaitTermination()
