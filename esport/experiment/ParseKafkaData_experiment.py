from JsonDataProcessor_experiment import JsonDataProcessor
from confluent_kafka import Consumer, KafkaError
import fastavro
import io
import json
import warnings
import os
warnings.filterwarnings("ignore", category=DeprecationWarning)
class ParseKafkaData(JsonDataProcessor):

    def __init__(self, base_url, kafka_bootstrap_servers, kafka_topic, bearer_token, avro_schema, accept="application/json"):
        super().__init__(base_url, kafka_bootstrap_servers, kafka_topic, bearer_token, avro_schema, accept)
        # self.consumer_group = consumer_group
        # self.offset_reset = offset_reset

    def consume(self, keys_to_extract, consumer_group, offset_reset="earliest"):
        # Create a Kafka consumer     keys_to_extract = ["id", "name", "acronym", "location"]
        consumer_config = {
            'bootstrap.servers': self.kafka_bootstrap_servers,
            'group.id': consumer_group,
            'auto.offset.reset': offset_reset  # You can adjust this based on your requirements.
        }
        consumer = Consumer(consumer_config)

        # Subscribe to the Kafka topic
        consumer.subscribe([self.kafka_topic])

        while True:
            try:
                message = consumer.poll(2.0)  # Poll for messages with a timeout
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print(f"Reached end of partition {message.partition()}")
                    else:
                        print(f"Error while consuming message: {message.error()}")
                else:
                    # Assuming Avro data is in the message value
                    avro_data = message.value()

                    # Deserialize the Avro data using the Avro schema
                    avro_bytes_io = io.BytesIO(avro_data)
                    avro_reader = fastavro.reader(avro_bytes_io, self.avro_schema)
                    json_data = list(avro_reader)
                    print(json_data)

                    # Process the JSON data as needed
                    # for item in json_data:
                    #     parsed_data = self.parse_json(item, keys_to_extract)
                        # Do something with the parsed_data (e.g., save to a file, print, etc.)

            except KeyboardInterrupt:
                break

        consumer.close()


    # def parse_json(self, keys_to_extract):
    #     parsed_data = []  # Create an empty list called 'parsed_data' to store the extracted data.
    #     for item in self.data_array:  # Iterate through each item in the 'data_array' (which contains JSON data).
    #         extracted_data = {}  # Create an empty dictionary called 'extracted_data' to store the extracted values.
    #         for key in keys_to_extract:  # Iterate through each key specified in 'keys_to_extract.'
    #             extracted_data[key] = item.get(
    #                 key)  # Extract the value associated with the current 'key' from the current 'item' and store it in 'extracted_data.'
    #         parsed_data.append(extracted_data)  # Add the 'extracted_data' dictionary to the 'parsed_data' list.
    #         # print(key)
    #     return parsed_data  # Return the list of extracted data.