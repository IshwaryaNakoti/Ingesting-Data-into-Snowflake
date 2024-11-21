import os
import logging
import sys
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

logging.basicConfig(level=logging.INFO)

# Update your local Kafka broker address
kafka_brokers = "127.0.0.1:9092"  # Default local Kafka broker address
topic_name = "my_topic"  # Replace with your desired topic name


def create_topic():
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_brokers, client_id='publish_data')
        topic_metadata = admin_client.list_topics()
        if topic_name not in topic_metadata:
            logging.info(f"Creating topic: {topic_name}")
            topic = NewTopic(name=topic_name, num_partitions=10, replication_factor=1)
            admin_client.create_topics(new_topics=[topic], validate_only=False)
        else:
            logging.info(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        logging.error(f"Failed to create topic: {e}")


def get_kafka_producer():
    logging.info("Connecting to Kafka")
    return KafkaProducer(bootstrap_servers=kafka_brokers)


if __name__ == "__main__":
    producer = get_kafka_producer()
    create_topic()
    for message in sys.stdin:
        if message.strip():
            try:
                producer.send(topic_name, value=message.strip().encode('utf-8'))
                logging.info(f"Sent message: {message.strip()}")
            except Exception as e:
                logging.error(f"Failed to send message: {e}")
        else:
            break
    producer.flush()
