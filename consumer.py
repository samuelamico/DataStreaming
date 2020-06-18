
import asyncio

from confluent_kafka import Consumer, Producer

TOPIC_NAME = 'person.web.json'
BROKER_URL = 'node1'

async def consumer(topic_name):

    consumer_kafka = Consumer({"bootstrap.servers":BROKER_URL,"group.id":"c1"})
    consumer_kafka.subscribe([topic_name])

    while True:
        message = consumer_kafka.poll(1.0)

        if message is None:
            print(f"No Message recived!")
        elif message.error() is not None:
            print(f"Erro detect = {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")


        await asyncio.sleep(1)


def main():
    try:
        asyncio.run(consumer(TOPIC_NAME))
    except KeyboardInterrupt:
        print("shutting down")
        


if __name__ == "__main__":
    main()