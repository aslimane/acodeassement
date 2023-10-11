from kafka import KafkaConsumer
from json import loads

SSL_CA_FILE_PATH='D:\\Dev\\aiven\\ca.pem'
SSL_CERT_FILE_PATH='D:\\Dev\\aiven\\service.cert'
SSL_KEY_FILE_PATH='D:\\Dev\\aiven\\service.key'

def read_json_message(bservers,tname):
    consumer = KafkaConsumer(
        tname,
        bootstrap_servers=bservers,
        client_id="CONSUMER_CLIENT_ID",
        group_id="CONSUMER_GROUP_ID",
        security_protocol="SSL",
        ssl_cafile=SSL_CA_FILE_PATH,
        ssl_certfile=SSL_CERT_FILE_PATH,
        ssl_keyfile=SSL_KEY_FILE_PATH
    )

    while True:
        for message in consumer.poll().values():
            print("Got message using SSL: " + message[0].value.decode('utf-8'))

bservers="kafka-15609353-amine-d877.aivencloud.com:15716"
tname="tracking"
read_json_message(bservers,tname)