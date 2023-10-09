from JsonDataProcessor_experiment import JsonDataProcessor
from confluent_kafka import Consumer, KafkaError, TopicPartition

# Kafka consumer configuration
kafka_bootstrap_servers = 'localhost:9094'
kafka_topic = 'esport_dota2_team'
kafka_group_id = 'esport_consumer'

# Hive connection configuration
# hive_host = 'localhost'
# hive_port = 10000
# hive_username = 'prsdhatama'
# hive_password = 'prsdhatama'
# hive_database = 'default'
# hive_table = 'office_employee'

# Connect to Kafka consumer
consumer_config = {
    'bootstrap.servers': kafka_bootstrap_servers,
    'group.id': kafka_group_id
}
consumer = Consumer(consumer_config)
consumer.subscribe([kafka_topic])

# Consume messages from Kafka and insert into Hive table
while True:
    try:
        message = consumer.poll(2.0)
        # print(message)
        # If data is empty -> continue
        if message is None:
            print("No new message")
            continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print(f"Reached end of partition {message.partition()}")
            else:
                print(f"Error while consuming message: {message.error()}")
        else:
        # value = message.value().decode('utf-8')
            print(f'Received message: {message}')

        # Insert the message into Hive table
        # query = f"INSERT INTO {hive_table} (name,age,company) VALUES ({value})"
        # hive_cursor.execute(query)
        # hive_connection.commit()
        # print(f"Inserted message into Hive: {value}")

    except KeyboardInterrupt:
        break
consumer.close()


