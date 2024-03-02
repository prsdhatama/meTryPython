from kafka import KafkaProducer
import json
from csv import DictReader
import time

# Required setting for Kafka Producer
bootstrap_servers = 'localhost:9094'
topicname = 'prsdhatama_olist_orders'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))


with open('D:/externalproject/pentahoecommerce/ecommerce/olist_orders_dataset.csv', 'r') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
        producer.send(topicname, value=row)
        print("Data sent to Kafka:", row)
        # Introduce a 5-second delay
        time.sleep(5)

# producer.send(topicname, value="boleh")
# Flush and close the producer
producer.flush()
producer.close()
# D:\python\meTryPython\csvpushtokafka\pushcsvtokafka.py