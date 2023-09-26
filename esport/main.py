# This is a sample Python script.
import os
import sys
sys.path.append('experiment')

from dotenv import load_dotenv

from experiment.JsonDataProcessor_experiment import JsonDataProcessor
from experiment.ParseKafkaData_experiment import ParseKafkaData

if __name__ == '__main__':
    base_url = "https://api.pandascore.co"
    kafka_bootstrap_servers = 'localhost:9094'
    kafka_topic = 'esport_dota2_team'
    avro_schema = {
        "type": "record",
        "namespace": "Dota2",
        "name": "Team",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "acronym", "type": ["null", "string"]},
            {"name": "location", "type": ["null", "string"]}
        ]
    }

    # avro_schema = {
    #                 "type": "record",
    #                 "namespace": "Dota2",
    #                 "name": "Team",
    #                 "fields": [
    #                     {"name": "id", "type": "int"},
    #                     {"name": "name", "type": "string"},
    #                     {"name": "acronym", "type": ["null", "string"]},
    #                     {"name": "location", "type": ["null", "string"]},
    #                     {
    #                         "name": "current_videogame",
    #                         "type": [
    #                             "null",
    #                             {
    #                                 "type": "record",
    #                                 "name": "VideoGame",
    #                                 "fields": [
    #                                     {"name": "id", "type": "int"},
    #                                     {"name": "name", "type": "string"},
    #                                     {"name": "slug", "type": "string"}
    #                                 ]
    #                             }
    #                         ]
    #                     }
    #                 ]
    #             }
    load_dotenv()
    bearer_token = os.environ.get("MY_AUTHORIZATION_HEADER")
    url_to_fetch = f"{base_url}/dota2/teams"

    data_processor = JsonDataProcessor(base_url, kafka_bootstrap_servers, kafka_topic, bearer_token, avro_schema)
    data_processor.run(url_to_fetch)

    # kafka_consumer = ParseKafkaData(base_url, kafka_bootstrap_servers, kafka_topic, bearer_token, avro_schema)
    # kafka_consumer.consume(keys_to_extract=["id", "name", "acronym", "location"], consumer_group="esport_consumer_group")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
