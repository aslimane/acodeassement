from kafka import KafkaClient, KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
import datetime, uuid, random
from json import dumps
from time import sleep

SSL_CA_FILE_PATH='D:\\Dev\\aiven\\ca.pem'
SSL_CERT_FILE_PATH='D:\\Dev\\aiven\\service.cert'
SSL_KEY_FILE_PATH='D:\\Dev\\aiven\\service.key'

def list_kafka_topics(bservers):
    client = KafkaClient(
    bootstrap_servers=bservers,
    security_protocol="SSL",
    ssl_cafile=SSL_CA_FILE_PATH,
    ssl_certfile=SSL_CERT_FILE_PATH,
    ssl_keyfile=SSL_KEY_FILE_PATH)
    future = client.cluster.request_update()
    client.poll(future=future)
    metadata = client.cluster
    print(metadata.topics())

def create_kafka_topic(bservers,topic_name):
    aclient = KafkaAdminClient(
    bootstrap_servers=bservers,
    security_protocol="SSL",
    ssl_cafile=SSL_CA_FILE_PATH,
    ssl_certfile=SSL_CERT_FILE_PATH,
    ssl_keyfile=SSL_KEY_FILE_PATH)
    topic_list = []
    topic_list.append(NewTopic(name=topic_name, num_partitions=1, replication_factor=1))
    try:
        aclient.create_topics(new_topics=topic_list, validate_only=False)
    except Exception as e:
        print(f"Topic {topic_name} Already Exist")

def generate_json_message(topic_name):
    prod = KafkaProducer(
    bootstrap_servers=bservers,
    security_protocol="SSL",
    ssl_cafile=SSL_CA_FILE_PATH,
    ssl_certfile=SSL_CERT_FILE_PATH,
    ssl_keyfile=SSL_KEY_FILE_PATH, value_serializer=lambda x:
                         dumps(x).encode('utf-8'))
    for i in range(4000):
        data = {'truck_id': str(uuid.uuid4()),
                'timestamp': datetime.datetime.now().isoformat(),
                'fridge_data':
                    {'m1': random.randint(50, 400),
                     'm2': random.randint(1000, 2000)
                     }
                }
        prod.send(topic_name, value=data)
        sleep(4)

bservers="kafka-15609353-amine-d877.aivencloud.com:15716"
tname="tracking"
filtered_tname="tracking_error"
filtered_ok_tname="tracking_ok"

#list_kafka_topics(bservers)
#create_kafka_topic(bservers,tname)
#create_kafka_topic(bservers,filtered_tname)
#create_kafka_topic(bservers,filtered_ok_tname)
#list_kafka_topics(bservers)
generate_json_message(tname)




