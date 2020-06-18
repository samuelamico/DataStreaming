from confluent_kafka.admin import AdminClient
import asyncio
import faker.providers 
from faker import Faker
from dataclasses import asdict, dataclass, field
import json
from io import BytesIO
from confluent_kafka import avro, Consumer, Producer
from fastavro import parse_schema, writer


""" I use the avro version: avro-python3==1.8.2
    command: python3.7 -m pip install avro-python3==1.8.2"""


BROKER_URL = "localhost:9092"
TOPIC_NAME = "access.web.avro"

fake = Faker()


@dataclass
class WebVisit:
    """ Initial Param """
    # Profile
    ipv4: str = field(default_factory=fake.ipv4)
    hostname: str = field(default_factory=fake.hostname)
    ipv4_class: str = field(default_factory=fake.ipv4_network_class)
    pageUri: str = field(default_factory=fake.uri)
    timestamp: str = field(default_factory=fake.iso8601)
    browser: str = field(default_factory=fake.ios_platform_token)

    ## Create Avro Schema - to send to schema registry
    schema = parse_schema(
        {
            "type": "record",
            "name": "web.user",
            "fields": [
                {"name":"ipv4","type":"string"},
                {"name":"hostname","type":"string"},
                {"name":"ipv4_class","type":"string"},
                {"name":"pageUri","type":"string"},
                {"name":"timestamp","type":"string"},
                {"name":"browser","type":"string"},
            ]
        }
    )

    def serialize(self):
        """Serializes the ClickEvent for sending to Kafka"""
        out = BytesIO()
        writer(out, WebVisit.schema, [asdict(self)])
        return out.getvalue()



async def produce(topic_name):
    print("-- Starting Producing --")
    producer_avro = Producer({
        "bootstrap.servers": BROKER_URL,
        "client.id": "access.web"
    })

    while True:
        producer_avro.produce(topic_name, WebVisit().serialize())
        await asyncio.sleep(1.0)



def main():
    try:
        asyncio.run(produce(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")



if __name__ == "__main__":
    main()