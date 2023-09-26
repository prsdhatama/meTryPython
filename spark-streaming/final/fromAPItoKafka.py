from confluent_kafka import Producer
import requests
import warnings
import time
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Kafka producer configuration
kafka_bootstrap_servers = 'localhost:9094'
kafka_topic = 'AirQuality'

API_KEY = "84ceb3d0-e3d9-42b1-a418-266a29b7eb77"
base_url = "https://api.airvisual.com/v2/"
city_endpoint = "city"
city = "Jakarta"
state = "Jakarta"
country = "Indonesia"
url = f"{base_url}{city_endpoint}?city={city}&state={state}&country={country}&key={API_KEY}"

# Connect to Kafka producer
producer_config = {'bootstrap.servers': kafka_bootstrap_servers}
producer = Producer(producer_config)

# Function to fetch air quality data
def fetch_air_quality_data():
    response = requests.get(url)
    return response.json()


# Process and analyze the data
def process_air_quality_data(data):
    city = data.get('data', {}).get('city')
    country = data.get('data', {}).get('country')
    pollution = data.get('data', {}).get('current', {}).get('pollution', {})
    aqius = pollution.get('aqius')
    mainus = pollution.get('mainus')
    aqicn = pollution.get('aqicn')

    data_string = f"Country: {country}, City: {city}, AQI US: {aqius}, Main US: {mainus}, AQI CN: {aqicn}"
    return data_string

# Produce the processed data to Kafka
while True:
    # Fetch and process air quality data
    air_quality_data = fetch_air_quality_data()
    processed_data = process_air_quality_data(air_quality_data)

    # Produce the processed data to Kafka
    def delivery_report(err, msg):
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    producer.produce(kafka_topic, key=None, value=processed_data, callback=delivery_report)
    producer.flush()

    # Wait for 30 seconds before fetching and sending data again
    time.sleep(30)




