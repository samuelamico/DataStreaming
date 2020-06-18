from confluent_kafka.admin import AdminClient, NewTopic
import asyncio
from confluent_kafka import Consumer, Producer
import faker.providers 
from faker import Faker
from faker_wifi_essid import WifiESSID
from dataclasses import asdict, dataclass, field
import json

BROKER_URL = "localhost:9092"
TOPIC_NAME = "person.web.json"

fake = Faker(['en_US'])
fake.add_provider(WifiESSID)



async def produce(topic_name):
    #See: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    p = Producer({   
        "bootstrap.servers":BROKER_URL,
        "client.id": "person.web",
        "batch.num.messages": "100",
        })

    while True:
        p.produce(topic_name, Person().serialize())
        await asyncio.sleep(1.0)


def main():
    try:
        asyncio.run(produce(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")


@dataclass
class Person:
    """ Initial Param """
    # Profile
    first_name: str = field(default_factory=fake.first_name)
    last_name: str = field(default_factory=fake.first_name)
    # Location
    latitude: float = float(fake.location_on_land()[0])
    longitude: float = float(fake.location_on_land()[1])
    city: str = fake.location_on_land()[2]
    country: str = fake.location_on_land()[3]
    continent: str =  fake.location_on_land()[4].split("/")[0]
    # Internet
    ipv4: str = field(default_factory=fake.ipv4)
    uri: str = field(default_factory=fake.uri)
    mac_address: str = field(default_factory=fake.mac_address)
    wifi_essid : str = field(default_factory=fake.wifi_essid)
    # Job
    job: str = field(default_factory=fake.job)

    def serialize(self):
        return json.dumps(
            {"first_name":self.first_name,"last_name":self.last_name,"latitude":self.latitude,"longitude":self.longitude,"city":self.city,"country":self.country,
            "continent":self.continent,"ipv4":self.ipv4,"uri":self.uri,"mac_address":self.mac_address,"wifi_essid":self.wifi_essid,"job":self.job}
        )

if __name__ == "__main__":
    main()













