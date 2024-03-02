import json
import time

from JsonDataProcessor_experiment import JsonDataProcessor
from confluent_kafka import Consumer, KafkaError, TopicPartition
from flatten_json import flatten
import fastavro
import io
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)
class ParseKafkaData(JsonDataProcessor):

    def __init__(self, base_url, kafka_bootstrap_servers, kafka_topic, bearer_token, avro_schema, accept="application/json"):
        super().__init__(base_url, kafka_bootstrap_servers, kafka_topic, bearer_token, avro_schema, accept)

    def consume(self, consumer_group, offset_reset="latest", keys_to_extract=None):
        max_consecutive_none = 5

        consumer_config = {
            'bootstrap.servers': self.kafka_bootstrap_servers,
            'group.id': consumer_group,
            'auto.offset.reset': offset_reset  # You can adjust this based on your requirements.
        }
        consumer = Consumer(consumer_config)
        # Subscribe to the Kafka topic
        consumer.subscribe([self.kafka_topic])
        print(f"Start consume topic {self.kafka_topic}")

        messages = []
        consecutive_none_count = 0
        # last_message_time = time.time()
        while True:
            try:
                message = consumer.poll(2.0)
                if message is None:
                    # last_message_time = time.time()
                    consecutive_none_count += 1
                    print(f"No message for {consecutive_none_count * 2}s")
                    if consecutive_none_count >= max_consecutive_none:
                        break  # Exit the loop after hitting max_consecutive_none consecutive None messages
                    continue
                else:
                    consecutive_none_count = 0
                # If data is empty -> continue
                if message and message.error():
                    if message.error().code() == KafkaError.PARTITION_EOF:
                        # End of partition event
                        print(f"Reached end of partition {message.partition()}")
                    else:
                        print(f"Error while consuming message: {message.error()}")
                else:

                    # Uncomment code below if using avro
                    avro_data = message.value()
                    avro_bytes_io = io.BytesIO(avro_data)
                    avro_reader = fastavro.reader(avro_bytes_io, self.avro_schema)
                    ###
                    # Uncomment code below if using json
                    # avro_data = message.value()
                    # Parse the Avro data as JSON directly
                    # json_data = json.loads(avro_data)
                    ###

                    if keys_to_extract is not None:
                        for avro_record in avro_reader:
                            extracted_data = self.parse_json_flatten(avro_record, keys_to_extract)
                            messages.append(extracted_data)
                            # Do something with the extracted data, e.g., store it in a database or process it further.
                            print(f"Extracted data: {extracted_data}")
                        # Exit the consumer loop
            except KeyboardInterrupt:
                break
        print(f"Consume topic {self.kafka_topic} ended")
        consumer.close()
        return messages


    def parse_json(self, json_data, keys_to_extract):
        extracted_data = {}  # Create an empty dictionary called 'extracted_data' to store the extracted values.
        for key in keys_to_extract:  # Iterate through each key specified in 'keys_to_extract.'
            extracted_data[key] = json_data.get(key)  # Extract the value associated with the current 'key' from the current 'json_data' and store it in 'extracted_data.'
        return extracted_data

    def extract_data(self, json_data, keys_to_extract):
        extracted_data = {}  # Create an empty dictionary called 'extracted_data' to store the extracted values.
        for key in keys_to_extract:
            current_data = json_data
            nested_keys = key.split('.')  # Split the key by '.' to handle nested keys
            for nested_key in nested_keys:
                if current_data is None:
                    break
                if isinstance(current_data, dict):
                    current_data = current_data.get(
                        nested_key)  # Get the value for the current key if it's a dictionary
                else:
                    current_data = None  # Set current_data to None if it's not a dictionary
            extracted_data[key] = current_data  # Store the extracted value in 'extracted_data'
        return extracted_data

    def parse_json_flatten(self, json_data, keys_to_extract):
        flattened_data = flatten(json_data, separator='.')
        extracted_data = {}  # Create an empty dictionary called 'extracted_data' to store the extracted values.

        for key in keys_to_extract:
            if key in flattened_data:
                extracted_data[key] = flattened_data[key]
            else:
                extracted_data[key] = None  # Set to None if the key is not found in flattened data

        return extracted_data


