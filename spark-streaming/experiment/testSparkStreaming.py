import os
import findspark

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

import warnings
import requests

# Suppress the warning messages
warnings.filterwarnings("ignore", category=DeprecationWarning)

# setting environment for SPARK_HOME
os.environ['SPARK_HOME'] = r'D:\python\env\spark\spark-3.4.0-bin-hadoop3'
# setting environment for HADOOP_HOME
os.environ['HADOOP_HOME'] = r'D:\python\env\hadoop'

sc = SparkContext("local[2]", "SocketStreaming") # Use 2 local threads as the processing threads
ssc = StreamingContext(sc, 1)  # Batch interval of 1 second

lines = ssc.socketTextStream('localhost',9999) #using the port 9999 on localhost
# lines = ["a ayam", "b babi", "c curut"]
words = lines.flatMap(lambda line:line.split(' '))

# Print the words in each batch of data (for demonstration purposes)
words.pprint()

# Start the streaming context
ssc.start()

# Wait for the application to terminate (or press Ctrl+C to stop manually)
try:
    ssc.awaitTermination()
except KeyboardInterrupt:
    print("Application terminated manually")

# Stop the streaming context gracefully
ssc.stop()



