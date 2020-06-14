from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "my-test-topic"
""" Test Creatin New Topic """


def main():
    client = AdminClient({"bootstrap.servers":BROKER_URL})

    topic = NewTopic(
        TOPIC_NAME,
        replication_factor =  1,
        num_partitions = 1
    )

    client.create_topics([topic])



if __name__ == "__main__":
    main()












