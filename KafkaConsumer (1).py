# pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7

# IMPORTING PACKAGES
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# KAFKA CONFIGURATIONS
kafka_topic_name = "streamapi"
kafka_bootstrap_servers = "kafkarunner3:9092"

# PRE-DEFINED VARIABLES
spark = (
    SparkSession.builder.appName("Python Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)
kafka_checkpoint = "/home/kafkarunner3/kafka_checkpoint/"
temporary_path = f"/home/kafkarunner3/kafka_data/{kafka_topic_name}_temp"
destination_path = f"/home/kafkarunner3/kafka_data/{kafka_topic_name}_data"

# FETCHING STREAMING DATA FROM KAFKA TOPIC
meetup_rsvp_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic_name)
    .option("includeHeaders", "true")
    .option("startingOffsets", "latest")
    .load()
)

# WRITING STREAMING DATA FROM KAFKA TOPIC TO A TEMPORARY DIRECTORY
df = meetup_rsvp_df.selectExpr("CAST(value AS STRING)")
df.writeStream.format("csv").outputMode("append").option(
    "checkpointLocation", kafka_checkpoint
).option("header", "true").start(path=temporary_path)

# WRITING STREAMING DATA FROM KAFKA TOPIC TO THE DESTINATION PATH
df_updated = (
    spark.read.csv(temporary_path)
    .coalesce(1)
    .write.csv(header=True, path=destination_path)
)
