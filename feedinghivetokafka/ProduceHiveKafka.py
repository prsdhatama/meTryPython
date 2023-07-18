from confluent_kafka import Producer
import puretransport
from pyhive import hive

# Kafka producer configuration
kafka_bootstrap_servers = 'localhost:9094'
kafka_topic = 'OfficeEmployeeProduce'

# Hive connection configuration
hive_host = 'localhost'
hive_port = 10000
hive_username = 'prsdhatama'
hive_password = 'prsdhatama'
hive_database = 'default'
hive_table = 'office_employee'

# Connect to Kafka producer
producer_config = {
    'bootstrap.servers': kafka_bootstrap_servers
}
producer = Producer(producer_config)

# Transport
transport = puretransport.transport_factory(host=hive_host,
                                            port=hive_port,
                                            username=hive_username,
                                            password=hive_password)

# Connect to Hive
hive_connection = hive.connect(
    # host=hive_host,
    # port=hive_port,
    # auth='CUSTOM',
    username=hive_username,
    # password=hive_password,
    database=hive_database,
    thrift_transport=transport
)
hive_cursor = hive_connection.cursor()

# Retrieve data from Hive and produce messages to Kafka (All Row)
# query = f"SELECT name, age, company FROM {hive_table}"
# hive_cursor.execute(query)
# rows = hive_cursor.fetchall()
# message = '\n'.join([f"Name: {name}, Age: {age}, Company: {company}" for name, age, company in rows])
# producer.produce(kafka_topic, value=message)
# producer.flush()
# print(f"Produced message to Kafka:\n{message}")


# Retrieve data from Hive and produce messages to Kafka
query = f"SELECT name, age, company FROM {hive_table}"
hive_cursor.execute(query)
for row in hive_cursor.fetchall():
    name, age, company = row
    message = f"Name: {name}, Age: {age}, Company: {company}"
    producer.produce(kafka_topic, value=message)
    producer.flush()
    print(f"Produced message to Kafka: {message}")

# Close connections
# producer.close()
# hive_connection.close()
