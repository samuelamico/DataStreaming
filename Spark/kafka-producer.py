"""Producer base-class providing common utilites and functionality"""
import logging
import time
import json
import asyncio

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer

logger = logging.getLogger(__name__)

BROKER_URL_LIST = "PLAINTEXT://localhost:9092" #,"PLAINTEXT://localhost:9093","PLAINTEXT://localhost:9094"]
SCHEMA_REGISTRY_URL = "http://localhost:8081"

class KafkaProducer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas


        self.broker_properties = {
            "bootstrap.servers": BROKER_URL_LIST
        }

        self.producer = Producer(self.broker_properties)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        logger.info("beginning topic creation for %s", self.topic_name)

        client = AdminClient({"bootstrap.servers":BROKER_URL_LIST})
        client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                )
            ]
        )


    async def running_file(self,file):
        with open(file, 'r') as outfile:
            data = json.load(outfile)
            for features in data:
                self.producer.produce(self.topic_name , json.dumps(features))

    def close(self):
        self.producer.flush()
        logger.info("producer close incomplete - skipping")



if __name__ == "__main__":

    file_json = 'data/uber.json'
    uberEvent =  KafkaProducer(topic_name='com.udacity.uber')
    uberEvent.create_topic()
    try:
        asyncio.run(uberEvent.running_file(file_json))
    except KeyboardInterrupt as e:
        print("shutting down")
    