import requests
import warnings
from confluent_kafka import Producer
import fastavro
import io

warnings.filterwarnings("ignore", category=DeprecationWarning)

class JsonDataProcessor:
    def __init__(self, base_url,
                 kafka_bootstrap_servers,
                 kafka_topic,
                 bearer_token,
                 avro_schema,
                 accept="application/json",
                 ):
        self.base_url = base_url
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.bearer_token = bearer_token
        self.accept = accept
        self.headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "Accept": self.accept
        }
        self.producer_config = {'bootstrap.servers': self.kafka_bootstrap_servers}
        self.producer = Producer(self.producer_config)
        self.avro_schema = avro_schema
        self.data_array = []

    def fetch_url(self, url, page_number=1):

        while True:
            page_url = f"{url}/?page[number]={page_number}"
            response = requests.get(page_url, headers=self.headers)

            if response.status_code == 200:
                page_data = response.json()

                if not page_data:
                    break

                self.data_array.extend(page_data)
                print(f"Fetched data from page {page_number}")
                print(page_url)
                print(response.json())
                page_number += 1

            else:
                print(f"Error fetching data from page {page_number}: {response.status_code}")
                break

        return self.data_array
    def produce_to_kafka(self):

        try:
            # Serialize the data to Avro format
            avro_bytes_io = io.BytesIO()
            fastavro.writer(avro_bytes_io, self.avro_schema, self.data_array)
            avro_bytes = avro_bytes_io.getvalue()

            # Produce the Avro data to Kafka
            self.producer.produce(self.kafka_topic, value=avro_bytes)
            self.producer.flush()
            return True  # Successful Kafka produce

        except Exception as e:
            print(f"Error producing to Kafka: {str(e)}")
            return False  # Failed Kafka produce

    def api_to_kafka(self, url: object) -> object:
        try:
            self.fetch_url(url)
            success = self.produce_to_kafka()
            return success  # Return True if the process was successful

        except Exception as e:
            print(f"Error running the data processing: {str(e)}")
            return False  # Return False if an error occurred
