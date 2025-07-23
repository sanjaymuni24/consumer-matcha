import threading
import json
from confluent_kafka import Consumer, Producer
from app.utility.database_consumer import DatabaseConsumer  # Assuming you have a database consumer utility
import traceback
from dotenv import load_dotenv
import os
from app.utility.operators import FormulaInterpreter
from py_expression_eval import Parser  # For evaluating filter expressions
import redis  # Redis client library
from datetime import datetime
import re
load_dotenv()


class CampaignEvaluator:
    def __init__(self, datasource, campaigns, kafka_config, db_config):
        """
        Initialize the CampaignEvaluator.

        Args:
            datasource (dict): Details of the datasource.
            campaigns (list): List of campaigns associated with the datasource.
            kafka_config (dict): Kafka configuration for consuming and producing messages.
            db_config (dict): Database configuration for storing qualified events.
        """
        self.datasource = datasource
        self.campaigns = campaigns
        self.kafka_config = {
            'brokers': os.getenv('KAFKA_BROKERS', 'localhost:9092'),
            'group_id': os.getenv('KAFKA_GROUP_ID', f"campaign-evaluator-{self.datasource['internal_name']}"),
            'auto_offset_reset': os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
        }
        self.db_config = db_config
        self.stop_event = threading.Event()
        self.parser = Parser()  # Initialize the expression parser

        # Initialize Redis connection
        self.redis_client = redis.StrictRedis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=int(os.getenv('REDIS_DB', 0)),
            decode_responses=True
        )

    def consume_topic(self, topic_name):
        """
        Consume messages from the specified Kafka topic.

        Args:
            topic_name (str): The name of the Kafka topic to consume from.
        """
        try:
            consumer = Consumer({
                'bootstrap.servers': self.kafka_config['brokers'],
                'group.id': self.kafka_config['group_id'],
                'auto.offset.reset': self.kafka_config['auto_offset_reset'],
                'session.timeout.ms': 6000,  # Consumer session timeout
            'max.poll.interval.ms': 300000,  # Maximum time between polls
                'enable.auto.commit': False,
            })
            consumer.subscribe([topic_name])

            print(f"Consuming messages from topic: {topic_name}")
            while not self.stop_event.is_set():
                # print("Waiting for messages...")
                messages = consumer.consume(num_messages=10, timeout=1.0)  # Fetch up to 10 messages at a time
                if not messages:
                    continue

                for msg in messages:
                    if msg.error():
                        print(f"Consumer error: {msg.error()}")
                        continue

                    message = json.loads(msg.value().decode('utf-8'))
                    print(f"Received message: {message}")
                    self.evaluate_campaigns(message)

            consumer.close()
        except Exception as e:
            print(f"Error consuming topic {topic_name}: {e}")
            traceback.print_exc()

    def evaluate_campaigns(self, message):
        """
        Evaluate all campaigns for the given message.

        Args:
            message (dict): The message consumed from the Kafka topic.
        """
        for campaign in self.campaigns:
            try:
                profile_filter = campaign['profile_filter_expression']
                trigger_filter = campaign['trigger_filter_expression']
                print(f"Evaluating campaign: {campaign['name']}")
                print(f"Profile filter: {profile_filter}")
                print(f"Trigger filter: {trigger_filter}")

                # Replace $keys in the profile filter
                profile_filter = self.replace_keys_with_values(profile_filter, message)

                # Replace $keys in the trigger filter
                trigger_filter = self.replace_keys_with_values(trigger_filter, message)
                print(f"Processed Profile filter: {profile_filter}")
                print(f"Processed Trigger filter: {trigger_filter}")

                # Evaluate profile filter
                profile_qualified = self.parser.parse(profile_filter).evaluate({})

                # Evaluate trigger filter
                trigger_qualified = self.parser.parse(trigger_filter).evaluate({})

                if profile_qualified and trigger_qualified:
                    print(f"Qualified event for campaign {campaign['name']}: {message}")
                    self.trigger_event_action(message, campaign['actions'])
                    self.update_aggregate_store(campaign['id'])
                else:
                    print(f"Event did not qualify for campaign {campaign['name']}: {message}")
            except Exception as e:
                print(f"Error evaluating campaign {campaign['name']}: {e}")
                traceback.print_exc()
    def replace_keys_with_values(self, expression, data):
        """
        Replace $keys in the expression with their corresponding values from the data.

        Args:
            expression (str): The filter expression containing $keys.
            data (dict): The data dictionary to fetch values from.

        Returns:
            str: The updated expression with $keys replaced by their values.
        """
        matches=re.findall(r'(\$[a-zA-Z0-9_\.]+)', expression)
        print(f"Matches found in expression: {matches}")
        print(f"Data available for replacement: {data}")
        for match in matches:
            key = match
            value = data.get(key, '')
            print(f"Replacing {key} with value: {value}")
            if value is not None:
                # Convert value to string and escape single quotes
                value_str = str(value).replace("'", "\\'")
                expression = expression.replace(f"{key}", f"{value_str}")
            else:
                # If key not found, replace with empty string
                expression = expression.replace(f"{key}", "None")
        return expression
        
    def update_aggregate_store(self, campaign_id):
        """
        Update the aggregate Redis datastore with the qualified event.

        Args:
            campaign_id (int): The ID of the campaign for which the event was qualified.
        """
        try:
            # Get the current hour (till hour resolution)
            current_hour = datetime.now().strftime('%Y-%m-%d %H:00:00')

            # Create a unique key for the campaign and hour
            key = f"campaign:{campaign_id}:{current_hour}"

            # Increment the counter for the campaign and hour in Redis
            self.redis_client.incr(key)

            print(f"Updated Redis aggregate store: {key} -> {self.redis_client.get(key)}")
        except Exception as e:
            print(f"Error updating Redis aggregate store for campaign {campaign_id}: {e}")
            traceback.print_exc()

    def trigger_event_action(self, event_details, actions):
        """
        Trigger event actions for the qualified event.

        Args:
            event_details (dict): Details of the qualified event.
            actions (list): List of actions to execute.
        """
        for action in actions:
            try:
                for endpoint in action['endpoints']:
                    if endpoint['name'] == 'DB':
                        print(f"Storing event in the database for action {action['name']}...")
                        # self.store_in_database(event_details)
                    elif endpoint['name'] == 'Kafka':
                        print(f"Producing event to Kafka for action {action['name']}...")
                        # self.produce_to_kafka(event_details, action['name'])
                    else:
                        print(f"Unknown endpoint: {endpoint['name']}")
            except Exception as e:
                print(f"Error triggering event action for action {action['name']}: {e}")
                traceback.print_exc()

    def store_in_database(self, event_details):
        """
        Store the qualified event details in the database.

        Args:
            event_details (dict): Details of the qualified event.
        """
        try:
            db_consumer = DatabaseConsumer(self.db_config)
            db_consumer.store_event(event_details)
            print(f"Event stored in the database: {event_details}")
        except Exception as e:
            print(f"Error storing event in the database: {e}")
            traceback.print_exc()

    def produce_to_kafka(self, event_details, action_name):
        """
        Produce the qualified event details to a Kafka topic.

        Args:
            event_details (dict): Details of the qualified event.
            action_name (str): Name of the action for Kafka topic.
        """
        try:
            producer = Producer({'bootstrap.servers': self.kafka_config['brokers']})
            topic = f"{self.datasource['internal_name']}_{action_name}"
            producer.produce(topic, json.dumps(event_details).encode('utf-8'))
            producer.flush()
            print(f"Event produced to Kafka topic '{topic}': {event_details}")
        except Exception as e:
            print(f"Error producing event to Kafka: {e}")
            traceback.print_exc()

    def start(self):
        """
        Start the campaign evaluator in a separate thread or process.
        """
        topic_name = f"application_{self.datasource['internal_name']}".upper()
        thread = threading.Thread(target=self.consume_topic, args=(topic_name,))
        thread.daemon = True
        thread.start()
        print(f"CampaignEvaluator started for topic: {topic_name}")

    def stop(self):
        """
        Stop the campaign evaluator.
        """
        self.stop_event.set()
        print("CampaignEvaluator stopped.")