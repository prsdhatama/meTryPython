import requests
import warnings
import time
import requests
from dotenv import load_dotenv
from confluent_kafka import Producer
import os
import json
import fastavro
warnings.filterwarnings("ignore", category=DeprecationWarning)


base_url = "https://api.pandascore.co"
kafka_bootstrap_servers = 'localhost:9094'
kafka_topic = 'AirQuality'
load_dotenv()
bearer_token = os.environ.get("MY_AUTHORIZATION_HEADER")
headers={
    "Authorization" : f"Bearer {bearer_token}",
    "Accept" : "application/json"
}
producer_config = {'bootstrap.servers': kafka_bootstrap_servers}
producer = Producer(producer_config)
dota2_team_url = f"{base_url}/dota2/teams"
dota2_teams_array = []
page_number = 1

while True:
    url = f"{dota2_team_url}/?page[number]={page_number}"
    # print(url)
    response = requests.get(url,headers=headers)

    if response.status_code == 200:
        page_data = response.json()

        if not page_data:
            break

        dota2_teams_array.extend(page_data)
        print(f"Fetched data from page {page_number}")
        print(page_data)
        page_number += 1

    else:
        print(f"Error fetching data from page {page_number}: {response.status_code}")
        break

def parse_dota2_team_json(array):
    dota2_team_list = []
    for item in array:
        id = item.get("id")
        name = item.get("name")
        acronym = item.get("acronym")
        location = item.get("location")

        # dota2_team_json = f"id : {id}, " \
        #                   f"name : {name}, " \
        #                   f"alias : {acronym}, " \
        #                   f"country : {location}, " \
        #
        dota2_team_item = {
            "id": id,
            "name": name,
            "alias": acronym,
            "country": location
        }
        dota2_team_list.append(dota2_team_item)

    dota2_team_json = json.dumps(dota2_team_list)
    print(dota2_team_json)

parse_dota2_team_json(dota2_teams_array)


# Serialize the data to Avro format
# avro_bytes_io = io.BytesIO()
# fastavro.writer(avro_bytes_io, avro_schema, data)
# avro_bytes = avro_bytes_io.getvalue()


# print(dota2_teams_array)
# producer.produce(kafka_topic, key=None, value=processed_data, callback=delivery_report)
# producer.flush()