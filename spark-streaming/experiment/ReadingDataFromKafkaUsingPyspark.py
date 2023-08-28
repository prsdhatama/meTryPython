from pyspark.sql import SparkSession
import os

# Define environment for Spark and Hadoop
os.environ['SPARK_HOME'] = r'D:\python\env\spark\spark-3.4.0-bin-hadoop3'
os.environ['HADOOP_HOME'] = r'D:\python\env\hadoop'

spark = SparkSession.builder \
    .appName("KafkaStreamReader") \
    .getOrCreate()

kafka_bootstrap_servers = "localhost:9094"
kafka_topic = "AirQuality"

kafka_data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

kafka_data.selectExpr("CAST(value AS STRING)").writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()
