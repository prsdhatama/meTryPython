# This is a sample Python script.
import os
import sys

sys.path.append('experiment')

from dotenv import load_dotenv

from experiment.JsonDataProcessor_experiment import JsonDataProcessor
from experiment.ParseKafkaData_experiment import ParseKafkaData
from experiment.ToDatabase_experiment import Database

if __name__ == '__main__':
    games = ["dota2",
             "valorant",
             # "codmw",
             # "csgo",
             # "fifa",
             # "lol",
             # "ow",
             # "pubg",
             # "r6siege",
             # "rl",
             # "kog"
             ]
    for game in games:

        # Kafka configuration
        kafka_bootstrap_servers = 'localhost:9094'
        kafka_topic = 'esport_leagues'
        avro_schema = {
                      "type": "record",
                      "name": "League",
                      "namespace": "com.example",
                      "fields": [
                        {"name": "id", "type": ["int", "null"]},
                        {"name": "image_url", "type": ["string", "null"]},
                        {"name": "modified_at", "type": ["string", "null"]},
                        {"name": "name", "type": ["string", "null"]},
                        {
                          "name": "series",
                          "type": {
                            "type": "array",
                            "items": {
                              "type": "record",
                              "name": "Series",
                              "fields": [
                                {"name": "begin_at", "type": ["string", "null"]},
                                {"name": "end_at", "type": ["string", "null"]},
                                {"name": "full_name", "type": ["string", "null"]},
                                {"name": "id", "type": ["int", "null"]},
                                {"name": "league_id", "type": ["int", "null"]},
                                {"name": "modified_at", "type": ["string", "null"]},
                                {"name": "name", "type": ["string", "null"]},
                                {"name": "season", "type": ["string", "null"]},
                                {"name": "slug", "type": ["string", "null"]},
                                {"name": "winner_id", "type": ["int", "null"]},
                                {"name": "winner_type", "type": ["string", "null"]},
                                {"name": "year", "type": ["int", "null"]}
                              ]
                            }
                          }
                        },
                        {"name": "slug", "type": ["string", "null"]},
                        {"name": "url", "type": ["string", "null"]},
                        {
                          "name": "videogame",
                          "type": {
                            "type": "record",
                            "name": "VideoGame",
                            "fields": [
                              {"name": "current_version", "type": ["string", "null"]},
                              {"name": "id", "type": ["int", "null"]},
                              {"name": "name", "type": ["string", "null"]},
                              {"name": "slug", "type": ["string", "null"]}
                            ]
                          }
                        }
                      ]
                    }

        load_dotenv()
        bearer_token = os.environ.get("MY_AUTHORIZATION_HEADER")
        # URL configuration
        base_url = "https://api.pandascore.co"
        game = game
        # https://api.pandascore.co/leagues/?page[number]=2
        segment = "leagues"
        url_to_fetch = f"{base_url}/{game}/{segment}"

        # DB configuration
        db_host = "localhost"
        db_port = "5432"
        db_name = "airflow"
        db_user = "airflow"
        db_password = "airflow"

        data_processor = JsonDataProcessor(base_url, kafka_bootstrap_servers, kafka_topic, bearer_token, avro_schema)
        data_fetcher = data_processor.api_to_kafka(url_to_fetch)

        kafka_consumer = ParseKafkaData(base_url, kafka_bootstrap_servers, kafka_topic, bearer_token, avro_schema)
        parsed_data = kafka_consumer.consume(keys_to_extract=["id", "name", "slug", "url"],consumer_group="esport_consumer_group")
        # print("success")
        db_connector = Database(db_host, db_port, db_name, db_user, db_password)
        connection = db_connector.connect_to_database()

        insert_processor = db_connector.insert_data_into_database(data=parsed_data,columns=["id","name","slug","url"],
                                                                  schema_name="esport",table_name=f"{game}_{segment}")

