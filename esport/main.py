# This is a sample Python script.
import os
import sys

sys.path.append('experiment')

from dotenv import load_dotenv

from experiment.JsonDataProcessor_experiment import JsonDataProcessor
from experiment.ParseKafkaData_experiment import ParseKafkaData
from experiment.ToDatabase_experiment import Database

if __name__ == '__main__':
    base_url = "https://api.pandascore.co"
    kafka_bootstrap_servers = 'localhost:9094'
    kafka_topic = 'esport_dota2_team'
    avro_schema = {
        "type": "record",
        "name": "Dota2",
        "fields": [
            {"name": "acronym", "type": ["null", "string"]},
            {"name": "current_videogame", "type": ["null",
                                                   {
                                                       "type": "record",
                                                       "name": "VideoGame",
                                                       "fields": [
                                                           {"name": "id", "type": "int"},
                                                           {"name": "name", "type": "string"},
                                                           {"name": "slug", "type": ["null", "string"]}
                                                       ]
                                                   }
                                                   ]
             },
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "location", "type": ["null", "string"]},
            {"name": "image_url", "type": ["null", "string"]},
            {"name": "modified_at", "type": "string"},
        ]
    }
    load_dotenv()
    bearer_token = os.environ.get("MY_AUTHORIZATION_HEADER")
    url_to_fetch = f"{base_url}/dota2/teams"
    db_host = "localhost"
    db_port = "5432"
    db_name = "airflow"
    db_user = "airflow"
    db_password = "airflow"

    # data_processor = JsonDataProcessor(base_url, kafka_bootstrap_servers, kafka_topic, bearer_token, avro_schema)
    # data_processor = data_processor.api_to_kafka(url_to_fetch)

    kafka_consumer = ParseKafkaData(base_url, kafka_bootstrap_servers, kafka_topic, bearer_token, avro_schema)
    parsed_data = kafka_consumer.consume(keys_to_extract=["id", "name", "acronym", "location"],consumer_group="esport_consumer_group")

    db_connector = Database(db_host, db_port, db_name, db_user, db_password)
    connection = db_connector.connect_to_database()

    insert_processor = db_connector.insert_data_into_database(data=parsed_data,columns=["id","team_name","alias","country"],
                                                              schema_name="esport",table_name="dota2_team")

