from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import os

def consume_messages():
    """
    Consume messages from the Kafka topic and print them to the terminal.
    """
    # Kafka configuration
    kafka_config = {
        'bootstrap.servers': os.getenv('KAFKA_BROKERS', 'localhost:9092'),
        'group.id': 'monitor-consumer-group',
        'auto.offset.reset': 'latest',  # Start consuming from the latest messages
    }

    topic = os.getenv('KAFKA_TOPIC', 'qualified_events')  # Default topic name

    # Initialize Kafka consumer
    consumer = Consumer(kafka_config)

    try:
        # Subscribe to the topic
        consumer.subscribe([topic])
        print(f"Subscribed to topic: {topic}")

        while True:
            # Poll for messages
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue  # No message received, continue polling

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Message successfully received
                message_value = msg.value().decode('utf-8')
                print(f"Received message from topic '{msg.topic()}': {json.dumps(json.loads(message_value), indent=4)}")

    except KeyboardInterrupt:
        print("Consumer interrupted by user. Exiting...")
    except Exception as e:
        print(f"Error consuming messages: {e}")
    finally:
        # Close the consumer
        consumer.close()
        print("Kafka consumer closed.")

if __name__ == "__main__":
    consume_messages()