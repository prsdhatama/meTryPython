from JsonDataProcessor_experiment import JsonDataProcessor
from confluent_kafka import Consumer, KafkaError, TopicPartition
import fastavro
import io
import json
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
class ParseKafkaData(JsonDataProcessor):

    def __init__(self, base_url, kafka_bootstrap_servers, kafka_topic, bearer_token, avro_schema, accept="application/json"):
        super().__init__(base_url, kafka_bootstrap_servers, kafka_topic, bearer_token, avro_schema, accept)

    def consume(self, consumer_group, offset_reset="latest", keys_to_extract=None):

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
                message = consumer.poll(2.0)
                # If data is empty -> continue
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print(f"Reached end of partition {message.partition()}")
                    else:
                        print(f"Error while consuming message: {message.error()}")
                else:
                    avro_data = message.value()
                    avro_bytes_io = io.BytesIO(avro_data)
                    avro_reader = fastavro.reader(avro_bytes_io, self.avro_schema)
                    json_data_list = list(avro_reader)

                    if keys_to_extract is not None:
                        row_list = []
                        for row in json_data_list:
                            parsed_data_list = self.parse_json(row, keys_to_extract)
                            row_list.append(parsed_data_list)
                        return row_list
                        # print(row_list)

            except KeyboardInterrupt:
                break
        consumer.close()

    def parse_json(self, json_data, keys_to_extract):
        extracted_data = {}  # Create an empty dictionary called 'extracted_data' to store the extracted values.
        for key in keys_to_extract:  # Iterate through each key specified in 'keys_to_extract.'
            extracted_data[key] = json_data.get(key)  # Extract the value associated with the current 'key' from the current 'json_data' and store it in 'extracted_data.'
        return extracted_data