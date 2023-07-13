# from kafka import KafkaClient, KafkaProducer, KafkaConsumer
from pyhive import hive
from confluent_kafka import Consumer

# Kafka consumer configuration
kafka_bootstrap_servers = 'localhost:9094'
kafka_topic = 'your_topic_name'
kafka_group_id = 'your_group_id'

# Hive connection configuration
hive_host = 'localhost'
hive_port = 10000
hive_username = 'your_hive_username'
hive_password = 'your_hive_password'
hive_database = 'your_hive_database'
hive_table = 'your_hive_table'

# Connect to Kafka consumer
consumer_config = {
    'bootstrap.servers': kafka_bootstrap_servers,
    'group.id': kafka_group_id
}
consumer = Consumer(consumer_config)
consumer.subscribe([kafka_topic])

# Connect to Hive
hive_connection = hive.connect(
    host=hive_host,
    port=hive_port,
    username=hive_username,
    password=hive_password,
    database=hive_database
)
hive_cursor = hive_connection.cursor()

# Consume messages from Kafka and insert into Hive table
while True:
    messages = consumer.consume(num_messages=10, timeout=1.0)
    for message in messages:
        if message is None:
            continue
        if message.error():
            print(f"Error consuming message: {message.error()}")
            continue

        value = message.value().decode('utf-8')

        # Insert the message into Hive table
        query = f"INSERT INTO {hive_table} (message) VALUES ('{value}')"
        hive_cursor.execute(query)
        hive_connection.commit()
        print(f"Inserted message into Hive: {value}")

consumer.close()
hive_connection.close()
