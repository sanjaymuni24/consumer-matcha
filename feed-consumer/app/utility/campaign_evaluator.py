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
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, JSON, DateTime
from sqlalchemy.orm import sessionmaker
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
            'auto_offset_reset': os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest'),
            'topic': os.getenv('KAFKA_TOPIC', 'qualified_events')
        }
        self.db_config = db_config
        self.stop_event = threading.Event()
        self.parser = Parser()  # Initialize the expression parser
        # Initialize SQLAlchemy engine and session
        connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)
        self.metadata = MetaData()

        # Define the qualified_events table structure
        self.qualified_events_table = Table(
            'qualified_events',
            self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('campaign_id', Integer, nullable=False),
            Column('campaign_name', String(255), nullable=False),
            Column('event_id', Integer, nullable=False),
            Column('event_name', String(255), nullable=False),
            Column('message_datetime', DateTime, nullable=False),
            Column('message', JSON, nullable=False),
            Column('key_field', JSON, nullable=False)
        )
        # Initialize Redis connection
        self.redis_client = redis.StrictRedis(
            host=os.getenv('AGGREGATE_REDIS_HOST', 'localhost'),
            port=int(os.getenv('AGGREGATE_REDIS_PORT', 6379)),
            db=int(os.getenv('AGGREGATE_REDIS_DB', 0)),
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
                'enable.auto.commit': False,  # Keep auto-commit disabled for manual control
            })
            consumer.subscribe([topic_name])

            print(f"Consuming messages from topic: {topic_name}")
            while not self.stop_event.is_set():
                # print("Waiting for messages...")
                messages = consumer.consume(num_messages=10, timeout=1.0)  # Fetch up to 10 messages at a time
                if not messages:
                    continue

                messages_to_commit = []  # Track messages to commit after processing

                for msg in messages:
                    if msg.error():
                        print(f"Consumer error: {msg.error()}")
                        continue

                    try:
                        message = json.loads(msg.value().decode('utf-8'))
                        print(f"Received message: {message}")
                        
                        # Evaluate campaigns for the message
                        self.evaluate_campaigns(message)
                        
                        # Add message to commit list after successful processing
                        messages_to_commit.append(msg)
                        
                    except Exception as e:
                        print(f"Error processing message: {e}")
                        traceback.print_exc()
                        # Optionally, you can still commit the message to avoid reprocessing
                        # or handle it based on your error handling strategy
                        messages_to_commit.append(msg)

                # Commit all successfully processed messages
                if messages_to_commit:
                    try:
                        # Commit the last message in the batch (commits all previous messages)
                        consumer.commit(message=messages_to_commit[-1])
                        print(f"Committed {len(messages_to_commit)} messages")
                    except Exception as e:
                        print(f"Error committing messages: {e}")
                        traceback.print_exc()

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
                profile_filter = self.replace_keys_with_values(profile_filter, message).replace('AND','&').replace('OR','|')

                # Replace $keys in the trigger filter
                trigger_filter = self.replace_keys_with_values(trigger_filter, message).replace('AND','&').replace('OR','|')
                print(f"Processed Profile filter: {profile_filter}",eval(profile_filter))
                print(f"Processed Trigger filter: {trigger_filter}",eval(trigger_filter))

                # Evaluate profile filter
                profile_qualified = eval(profile_filter)#self.parser.parse(profile_filter).evaluate({})

                # Evaluate trigger filter
                trigger_qualified = eval(trigger_filter)#self.parser.parse(trigger_filter).evaluate({})
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if profile_qualified and trigger_qualified:
                    print(f"{current_time}:Qualified event for campaign {campaign['name']}: {message}")
                    self.trigger_event_action(message, campaign)
                    self.update_aggregate_store(campaign['id'])
                else:
                    
                    print(f"{current_time}:Event did not qualify for campaign {campaign['name']}: {message}")
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
        # print(f"Matches found in expression: {matches}")
        # print(f"Data available for replacement: {data}")
        for match in matches:
            key = match
            value = data.get(key, '')
            # print(f"Replacing {key} with value: {value}")
            if value is not None:
                if isinstance(value, str):
                    # If value is a string, escape single quotes
                     # Escape double quotes as well
                    value_str = f"'{value}'"  # Wrap in single quotes
                # Convert value to string and escape single quotes
                    # value_str = (value_str).replace("'", "\\'")
                elif isinstance(value, (int, float)):
                    value_str = str(value)
                elif isinstance(value, bool):
                    value_str = 'true' if value else 'false'
                elif isinstance(value, list):
                    # Convert list to a comma-separated string
                    value_str = ','.join([str(v).replace("'", "\\'") for v in value])
                elif isinstance(value, dict):
                    # Convert dict to a JSON string
                    value_str = json.dumps(value).replace("'", "\\'")
                else:
                    value_str = str(value)
                print(f"Replacing {key} with value: {value_str}")
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
            current_hour = datetime.now().strftime('%Y-%m-%d %H')

            # Create a unique key for the campaign and hour
            key = f"campaign_qualified_hourly_triggers:{campaign_id}:{current_hour}"

            # Increment the counter for the campaign and hour in Redis
            self.redis_client.incr(key)

            print(f"Updated Redis aggregate store: {key} -> {self.redis_client.get(key)}")
        except Exception as e:
            print(f"Error updating Redis aggregate store for campaign {campaign_id}: {e}")
            traceback.print_exc()

    def trigger_event_action(self, event_details, campaign):
        """
        Trigger event actions for the qualified event.

        Args:
            event_details (dict): Details of the qualified event.
            actions (list): List of actions to execute.
        """
        for action in campaign['actions']:
            try:
                for endpoint in action['endpoints']:
                    if endpoint['name'] == 'DB':
                        print(f"Storing event in the database for action {action['name']}...")
                        profile_attributes = action.get('profile_attributes', [])
                        feed_attributes = action.get('feed_attributes', [])
                        message = {}
                        message['profile_attributes']={key:value for key, value in event_details.items() if key  in profile_attributes}
                        message['feed_attributes']={key:value for key, value in event_details.items() if key  in feed_attributes}
                        self.store_in_database(message,campaign,action)
                        self.produce_to_kafka(message,campaign,action)

                    elif endpoint['name'] == 'Kafka':
                        print(f"Producing event to Kafka for action {action['name']}...")
                        # self.produce_to_kafka(event_details, action['name'])
                    else:
                        print(f"Unknown endpoint: {endpoint['name']}")
            except Exception as e:
                print(f"Error triggering event action for action {action['name']}: {e}")
                traceback.print_exc()

    def store_in_database(self, message,campaign,action):
        """
        Store the qualified event details in the database.

        Args:
            event_details (dict): Details of the qualified event.
        """
        try:
            session = self.Session()
            insert_data = {
                'campaign_id': campaign['id'],
                'campaign_name': campaign['name'],
                'event_id': action['id'],
                'event_name': action['name'],
                'message_datetime': datetime.now(),
                'message': message,
                'key_field': campaign['datastore_key'] if 'datastore_key' in campaign else {}
            }
            insert_statement = self.qualified_events_table.insert().values(**insert_data)
            session.execute(insert_statement)
            session.commit()
            print(f"Event stored in the database: {message}")
        except Exception as e:
            print(f"Error storing event in the database: {e}")
            traceback.print_exc()
        finally:
            session.close()

    def produce_to_kafka(self,message,campaign,action):
        """
        Produce the qualified event details to a Kafka topic.

        Args:
            event_details (dict): Details of the qualified event.
            action_name (str): Name of the action for Kafka topic.
        """
        try:

            producer = Producer({'bootstrap.servers': self.kafka_config['brokers']})
            topic =  self.kafka_config['topic']
            data = {
            'campaign_id': campaign['id'],
            'campaign_name': campaign['name'],
            'event_id': action['id'],
            'event_name': action['name'],
            'message_datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'message': message,
            'key_field': campaign['datastore_key'] if 'datastore_key' in campaign else {}
        }
            producer.produce(topic, json.dumps(data).encode('utf-8'))
            producer.flush()
            print(f"Event produced to Kafka topic '{topic}': {data}")
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