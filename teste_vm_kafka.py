from kafka import KafkaProducer
import logging
logging.basicConfig(level=logging.DEBUG)
import datetime

producer = KafkaProducer(bootstrap_servers=['node1'])
for _ in range(10):
    producer.send('testtopic', )
producer.flush()
