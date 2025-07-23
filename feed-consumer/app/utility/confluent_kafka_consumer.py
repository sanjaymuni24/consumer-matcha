from confluent_kafka import Consumer, KafkaException, KafkaError, Producer
import os
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

def consume_messages(brokers, group_id, topic):
    """
    Consumes messages from a Kafka topic using Confluent Kafka Consumer.

    :param brokers: Kafka broker(s) (e.g., 'localhost:9092')
    :param group_id: Consumer group ID
    :param topic: Kafka topic to consume messages from
    """
    # Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': brokers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # Start reading at the earliest message
        'enable.auto.commit': True       # Automatically commit offsets
    }

    # Create a Kafka consumer
    consumer = Consumer(consumer_config)

    try:
        # Subscribe to the topic
        consumer.subscribe([topic])
        print(f"Subscribed to topic: {topic}")

        while True:
            # Poll for a message
            msg = consumer.poll(timeout=1.0)  # Timeout in seconds

            if msg is None:
                # No message available within the timeout
                continue

            if msg.error():
                # Handle Kafka errors
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f"Reached end of partition: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Successfully received a message
                print(f"Received message: {msg.value().decode('utf-8')} from topic: {msg.topic()}")

    except KeyboardInterrupt:
        print("Consumer interrupted by user")
    finally:
        # Close the consumer to commit final offsets and clean up resources
        consumer.close()
        print("Consumer closed")



def check_kafka_connection(brokers, principal, keytab, service_name, topic):
    """
    Checks the Kafka connection using Kerberos authentication with a keytab file.

    :param brokers: Kafka broker(s) (e.g., 'localhost:9092')
    :param principal: Kerberos principal (e.g., 'user@REALM')
    :param keytab: Path to the keytab file
    :param service_name: Kerberos service name (e.g., 'kafka')
    :param topic: Kafka topic to check
    :return: True if the connection is successful, False otherwise
    """
    # Set the Kerberos environment variables
    os.environ['KRB5_CLIENT_KTNAME'] = keytab

    # Kafka consumer configuration with Kerberos authentication
    consumer_config = {
        'bootstrap.servers': brokers,
        'sasl.mechanism': 'GSSAPI',
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.kerberos.service.name': service_name,
        'sasl.kerberos.principal': principal,
        'group.id': 'connection_test_group',
        'auto.offset.reset': 'earliest',
    }

    # Create a Kafka consumer
    consumer = Consumer(consumer_config)

    try:
        # Subscribe to the topic
        consumer.subscribe([topic])
        print(f"Checking connection to topic: {topic}")

        # Poll for a message to test the connection
        msg = consumer.poll(timeout=5.0)  # Timeout in seconds

        if msg is None:
            print("Connection successful, but no messages available.")
            return True

        if msg.error():
            print(f"Error while checking connection: {msg.error()}")
            return False

        print("Connection successful and message received.")
        return True

    except KafkaException as e:
        print(f"Kafka connection failed: {e}")
        return False

    finally:
        # Close the consumer
        consumer.close()
        print("Connection test completed.")

# Example usage
if __name__ == "__main__":
    brokers = "localhost:9092"  # Replace with your Kafka broker(s)
    principal = "user@REALM"  # Replace with your Kerberos principal
    keytab = "/path/to/keytab.file"  # Replace with the path to your keytab file
    service_name = "kafka"  # Replace with your Kerberos service name
    topic = "example_topic"  # Replace with your Kafka topic

    if check_kafka_connection(brokers, principal, keytab, service_name, topic):
        print("Kafka connection is valid.")
    else:
        print("Kafka connection failed.")