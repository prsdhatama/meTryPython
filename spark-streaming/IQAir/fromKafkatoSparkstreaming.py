from confluent_kafka import Consumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import warnings
import os
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Define Kafka consumer configuration
kafka_bootstrap_servers = "localhost:9094"
kafka_topic = "AirQuality"

spark_version = '3.4.0'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:{},org.apache.spark:spark-sql-kafka-0-10_2.12:{} pyspark-shell'.format(spark_version,spark_version)

# Define environment for Spark and Hadoop
os.environ['SPARK_HOME'] = r'D:\python\env\spark\spark-3.4.0-bin-hadoop3'
os.environ['HADOOP_HOME'] = r'D:\python\env\hadoop'
# Create a Spark session
spark = SparkSession.builder.appName("AirQualityConsumer").getOrCreate()

# Print Spark version
print("Spark version:", spark.version)
# Configure Kafka connection parameters
kafka_conf = {
    'bootstrap.servers': kafka_bootstrap_servers,
    'group.id': 'spar_streaming_consuemr',
    'auto.offset.reset': 'earliest'
}
# Initialize Kafka consumer
consumer = Consumer(kafka_conf)
consumer.subscribe([kafka_topic])
# Read data from Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Process the Kafka data and print to the console
query = df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the query to terminate
query.awaitTermination()

