from pprint import pprint
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.streaming import StreamingContext
import requests
import json
import os
import warnings
import socket

# Suppress the warning messages
warnings.filterwarnings("ignore", category=DeprecationWarning)
# setting environment for SPARK_HOME
os.environ['SPARK_HOME'] = r'D:\python\env\spark\spark-3.4.0-bin-hadoop3'
# setting environment for HADOOP_HOME
os.environ['HADOOP_HOME'] = r'D:\python\env\hadoop'

# Create a socket for sending data to port 9999
send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
send_socket.connect(("localhost", 9999))

# Create a SparkSession
spark = SparkSession.builder.appName("AirQualityStreaming").getOrCreate()
# Create a StreamingContext with batch interval of your choice
ssc = StreamingContext(spark.sparkContext, batchDuration=15)


API_KEY = "84ceb3d0-e3d9-42b1-a418-266a29b7eb77"
# Base URL for IQAir API
base_url = "https://api.airvisual.com/v2/"
# Endpoint for city data
city_endpoint = "city"
# Specify the city and country
city = "Jakarta"
state = "Jakarta"
country = "Indonesia"
url = f"{base_url}{city_endpoint}?city={city}&state={state}&country={country}&key={API_KEY}"

# Function to fetch air quality data
def fetch_air_quality_data():
    response = requests.get(url)
    return response.json()

send_socket.send(fetch_air_quality_data().encode())  # Encode the string as bytes before sending

# Create a DStream that fetches data from the API at each interval
api_data_stream = ssc\
    .socketTextStream("localhost", 9999)\
    .map(lambda x: fetch_air_quality_data())


# Process and analyze the data
def process_air_quality_data(data):
    city = data.get('data', {}).get('city')
    country = data.get('data', {}).get('country')
    pollution = data.get('data', {}).get('current', {}).get('pollution', {})
    aqius = pollution.get('aqius')
    mainus = pollution.get('mainus')
    aqicn = pollution.get('aqicn')

    data_string=  f"Country: {country}, City: {city}, AQI US: {aqius}, Main US: {mainus}, AQI CN: {aqicn}"







