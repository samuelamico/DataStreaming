
import asyncio

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "my-first-python-topic"


async def produce(topic_name):
    p = Producer({"bootstrap.servers":BROKER_URL})

    curr_iteration = 0
    while True:
        p.produce(TOPIC_NAME, f"iteration {curr_iteration}".encode("utf-8"))

        curr_iteration += 1
        await asyncio.sleep(1)


async def consume(topic_name):
    c = Consumer({"bootstrap.servers":BROKER_URL,"group.id":"first-group"})

    c.subscribe([TOPIC_NAME])

    while True:

        message = c.poll(1.0)

        if message is None:
            print(f"No Message recived!")
        elif message.error() is not None:
            print(f"Erro detect = {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")

        await asyncio.sleep(1)


async def produce_consume():
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(TOPIC_NAME))
    t2 = asyncio.create_task(consume(TOPIC_NAME))
    await t1
    await t2


def main():

    client = AdminClient({"bootstrap.servers":BROKER_URL})

    topic = NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)


    client.create_topics([topic])

    try:
        asyncio.run(produce_consume())
    except KeyboardInterrupt as e:
        print("shutting down")
    finally:
        client.delete_topics([topic])
        pass


if __name__ == "__main__":
    main()