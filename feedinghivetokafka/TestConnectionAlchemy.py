from confluent_kafka import Consumer
from sqlalchemy import create_engine
import puretransport
# Kafka Consumer configuration
bootstrap_servers = 'localhost:9094'
group_id = 'hive-yudi'
topic = 'OfficeEmployee'

# Hive configuration
hive_host = 'localhost'
hive_port = 10000
hive_database = 'default'
hive_table = 'office_employee'

# Connect to Kafka
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id
}
consumer = Consumer(consumer_config)
consumer.subscribe([topic])

# Transport
transport = puretransport.transport_factory(host='localhost',
                                            port=10000,
                                            username='prsdhatama',
                                            password='prsdhatama')

# Connect to Hive using SQLAlchemy
# hive_url = f'hive://{hive_host}:{hive_port}/{hive_database}'
# engine = create_engine(hive_url)

engine = create_engine('hive://localhost/default',
                       connect_args={'thrift_transport': transport})

# Consume Kafka messages and insert into Hive
while True:
    message = consumer.poll(1.0)  # Poll for new messages

    if message is None:
        continue

    if message.error():
        print(f'Error: {message.error()}')
        continue

    value = message.value().decode('utf-8')
    print(f'Received message: {value}')

    # Insert message into Hive table
    query = f"INSERT INTO {hive_table} (name,age,company) VALUES ({value})"
    with engine.connect() as connection: #this error happen cause sasl module has no attribute Client
        connection.execute(query)
