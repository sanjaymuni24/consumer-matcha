from confluent_kafka import Consumer, TopicPartition, KafkaException, KafkaError, DeserializingConsumer,Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from app.utility.data_source_connection import map_avro_type, infer_json_schema
from app.utility.operators import FormulaInterpreter
from app.utility.datastore_operations import get_datastore_redis_connection,get_aggregate_redis_connection,get_redis_data,get_redis_datastore_full_data
import json
import threading
import time
import queue
import logging
from datetime import datetime
import atexit
import signal
import os,traceback
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaAvroConsumer:
  
    def __init__(self, brokers, group_id, topic, schema_registry_url, num_threads=4,datasource=None):

        # Set all the instance variables
        print("Initializing KafkaAvroConsumer with parameters:")
        print(f"  Brokers: {brokers}")
        print(f"  Group ID: {group_id}")
        print(f"  Topic: {topic}")
        print(f"  Schema Registry URL: {schema_registry_url}")
        print(f"  Number of Threads: {num_threads}")
        self.brokers = brokers
        self.group_id = group_id
        self.topic = topic
        self.schema_registry_url = schema_registry_url
        self.num_threads = min(num_threads, 16)
        self.use_avro = False
        self.schema = None
        self.stop_event = None
        self.threads = []
        self.message_queue = queue.Queue(maxsize=1000)
        self.lock = threading.Lock()
        self.datasource=datasource
        self.output_topic= 'APPLICATION_'+datasource.internal_name.upper()
        
        atexit.register(self.cleanup)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        # Store process ID for checking if we're in the same process
        self.pid = os.getpid()
        # Initialize consumer - this must create self.consumer
        # self._initialize_consumer()
        logger.info(f"Initialized Kafka consumer for topic: {self.topic}, group: {self.group_id}")
    def _initialize_consumer(self):
        """Initialize the Kafka consumer with appropriate serializers based on schema registry."""
        consumer_config = {
            'bootstrap.servers': self.brokers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'auto.commit.interval.ms': 5000,  # Auto commit every 5 seconds
            'session.timeout.ms': 30000,  # 30 seconds
            'max.poll.interval.ms': 300000,  # 5 minutes
            # 'debug': 'consumer,cgrp,topic,fetch'  # Add debugging
        }
        
        # Check if schema registry is available and topic has Avro schema
        if self.schema_registry_url:
            try:
                schema_registry_client = SchemaRegistryClient({'url': self.schema_registry_url})
                subjects = schema_registry_client.get_subjects()
                
                if f"{self.topic}-value" in subjects:
                    self.use_avro = True
                    self.schema = self.get_avro_schema()
                    
                    avro_deserializer = AvroDeserializer(schema_registry_client)
                    key_deserializer = StringDeserializer('utf_8')
                    value_deserializer = avro_deserializer
                    
                    consumer_config.update({
                        'key.deserializer': key_deserializer,
                        'value.deserializer': value_deserializer,
                    })
                    logger.info(f"Using Avro deserialization for topic {self.topic}")
            except Exception as e:
                logger.warning(f"Error connecting to schema registry: {e}. Falling back to string deserialization.")
                self.use_avro = False
        
        if not self.use_avro:
            # Use normal string deserialization if Avro is not available
            key_deserializer = StringDeserializer('utf_8')
            value_deserializer = StringDeserializer('utf_8')
            consumer_config.update({
                'key.deserializer': key_deserializer,
                'value.deserializer': value_deserializer,
            })
            logger.info(f"Using string deserialization for topic {self.topic}")
        
        self.consumer = DeserializingConsumer(consumer_config)
        self.consumer.subscribe([self.topic], on_assign=self.on_assign, on_revoke=self.on_revoke)
        # poll to trigger initial assignment
        # self.consumer.poll(0)
        
       
        try:
            metadata = self.consumer.list_topics(timeout=10)
            available_topics = list(metadata.topics.keys())
            logger.info(f"Available topics: {available_topics}")
            
            if self.topic not in available_topics:
                logger.error(f"Topic '{self.topic}' not found in available topics.")
            else:
                topic_metadata = metadata.topics[self.topic]
                partitions = topic_metadata.partitions
                logger.info(f"Topic '{self.topic}' has {len(partitions)} partitions: {list(partitions.keys())}")
        except Exception as e:
            logger.error(f"Error listing topics: {e}")
    def on_assign(self, consumer, partitions):
        """Callback for partition assignment."""
        logger.info(f"Partitions assigned: {[p.partition for p in partitions]}")
        consumer.assign(partitions)
        # # Set beginning offsets for initial assignment
        # for partition in partitions:
        #     # Only set to beginning if this is the first assignment and offset is invalid
        #     consumer.seek(partition)  # Seek according to auto.offset.reset policy
        
        # for p in partitions:
        #     committed = consumer.committed(p)
        #     logger.info(f"Committed offset for partition {p.partition}: {committed.offset}")
        #     p.offset = 0 
        # consumer.assign(partitions)

    def on_revoke(self, consumer, partitions):
        """Callback for partition revocation."""
        logger.info(f"Partitions revoked: {[p.partition for p in partitions]}")
        consumer.unassign()

    def get_avro_schema(self):
        """Get the Avro schema for a Kafka topic from the Schema Registry."""
        try:
            schema_registry_client = SchemaRegistryClient({'url': self.schema_registry_url})
            subject = f"{self.topic}-value"
            
            schema_metadata = schema_registry_client.get_latest_version(subject)
            schema_str = schema_metadata.schema.schema_str
            avro_schema = json.loads(schema_str)
            
            schema = {}
            if 'fields' in avro_schema:
                for field in avro_schema['fields']:
                    column_name = field['name']
                    column_type = map_avro_type(field['type'])
                    schema[column_name] = column_type
            
            return schema
        except Exception as e:
            logger.error(f"Error fetching schema for topic {self.topic}: {e}")
            return None

    def consume_partition(self, partitions):
        """Consume messages from a specific partition."""
        # self.consumer.assign([partition]    )
        logger.info(f"Started consuming from partition {partitions}")
        self.consumer.assign(partitions)
        try:
            while not self.stop_event.is_set():
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Error in partition {msg.partition}: {msg.error()}")
                    continue
                
                # Process the message
                self.message_queue.put(( msg,
                self.consumer)
                    , timeout=2)
                
                # Commit the offset
                # self.consumer.commit(msg)
        except queue.Full:
            logger.warning("Message queue is full. Consider increasing queue size or processing speed.")
              
        except Exception as e:
            logger.error(f"Error consuming from partition {partitions}: {e}")
        finally:
            logger.info(f"Stopped consuming from partition {partitions}")

    def process_message(self, msg):
        """Process a single Kafka message and add to queue for further processing."""
        try:
            key = msg.key()
            value = msg.value()
            
            # Transform message based on format
            processed_message = None
            if self.use_avro:
                processed_message = value  # Avro deserialized value is already a dictionary
            else:
                try:
                    processed_message = json.loads(value) if isinstance(value, str) else value
                except json.JSONDecodeError:
                    processed_message = {'message': value}
            
            # Add to processing queue with timeout to avoid blocking indefinitely
            
            
            with self.lock:
                logger.debug(f"Message added to queue from partition {msg.partition()}")
        
        except queue.Full:
            logger.warning("Message queue is full. Consider increasing queue size or processing speed.")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def message_processor(self):
        """Worker thread to process messages from the queue."""
        datastore_redis_conn=get_datastore_redis_connection()
        aggregate_redis_conn=get_aggregate_redis_connection()
        while not self.stop_event.is_set():
            try:
                # Get message with timeout to allow checking stop_event periodically
                msg,consumer = self.message_queue.get(timeout=1)
                # Deserialize the message key and value
                key = msg.key()
                value = msg.value()
                
                # Transform message based on format
                processed_message = None
                if self.use_avro:
                    processed_message = value  # Avro deserialized value is already a dictionary
                else:
                    try:
                        processed_message = json.loads(value) if isinstance(value, str) else value
                    except json.JSONDecodeError:
                        processed_message = {'message': value}

                parsed_message=self.parse_message(message=processed_message)

                

                enriched_message = self.enrich_message(datastore_redis_conn,aggregate_redis_conn,parsed_message)
                for field in self.datasource.enrichment_rejection_fields:
                    if enriched_message.get('$feed.'+field) is None or enriched_message.get('$feed.'+field) == '':
                        logger.warning(f"Enrichment rejected for field '{field}' in message: {enriched_message}")
                        logger.warning(f"Message rejected due to missing enrichment field '{field}' as it is {enriched_message.get('$feed.'+field)}")
                        self.consumer.commit(msg)  # Commit the offset even if message is rejected
                        self.message_queue.task_done()  # Mark task as done
                        return
                
                self.produce_message(enriched_message)
                transaction_time = datetime.fromisoformat(processed_message.get('transaction_time'))
                # Make both datetime objects timezone-consistent
                if transaction_time.tzinfo is not None:
                    # If transaction_time has timezone, use UTC for current time
                    from datetime import timezone
                    current_time = datetime.now(timezone.utc)
                else:
                    # If transaction_time has no timezone, use naive current time
                    current_time = datetime.now()
                
                # Calculate lag
                lag = (current_time - transaction_time).total_seconds()
                lag_info = f"with lag {lag:.2f} seconds"
                # Process the message (replace with your actual processing logic)
                logger.info(f"Processing message from partition {msg.partition()}: {processed_message} {lag_info} at offset {msg.offset()}")
                self.consumer.commit(msg)  # Commit the offset after processing
                # Mark task as done
                self.message_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in message processor: {e}")
    def parse_message(self, message):
        """Parse the incoming message and return a structured dictionary.
        """
        input_schema =self.datasource.input_schema
        parsing_schema = self.datasource.parsing_schema
        input_message={}
        datatype_dict={
            'str': str,
            'int': int,
            'float': float,
            'bool': bool,
            'date': lambda x: datetime.strptime(x, '%Y-%m-%d').date(),
            'datetime': lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ') if isinstance(x, str) else x
        }
        for key,data_type in input_schema.items():
            if key in message:
                value = message[key]

                if isinstance(value, datatype_dict[data_type]):
                    input_message[key] = value
                else:
                    try:
                        input_message[key] = data_type(value)  # Attempt to convert to expected type
                    except Exception as e:
                        logger.error(f"Failed to convert field '{key}' with value '{value}' to type {data_type}: {e}")
            else:
                logger.warning(f"Field '{key}' not found in message. Skipping.")
        parsed_message = {}
        # Apply parsing schema if available
        if parsing_schema:
            
            for key, value in parsing_schema.items():
                formula=value.get('formula', None)
                if formula:
                    try:
                        
                        # Evaluate the formula in the context of input_message
                        result_value,result_datatype = FormulaInterpreter.evaluate_formula(formula, input_message)
                        # # print(formula,result_value,result_datatype)
                        # print(f"Evaluating formula: {formula} with input_message: {input_message}")
                        # print(f"Result value: {result_value}, Result datatype: {result_datatype}")
                        parsed_message['$feed.'+key] = result_value
                    except Exception as e:
                        logger.error(f"Error evaluating formula '{formula}' for key '{key}': {e}")
        print('parsed_message',parsed_message)
        
        return parsed_message

    def enrich_message(self,datastore_redis_conn,aggregate_redis_conn, message):
        """Enrich the message with additional data or transformations."""
        # Placeholder for enrichment logic
        # For example, you can add static fields, fetch additional data, etc.
        try:
            enriching_schema = self.datasource.enrichment_schema
            aggregation_schema = self.datasource.aggregation_schema
            datasource_internal_name = self.datasource.internal_name
            datastore_values={}
            storeback_helper={}
             
            for datastore in self.datasource.datastores:
                
                datasource_key = datastore.get('datasource_key')
                datastore_internal_name=datastore.get('internal_name')
                profile_key=message.get('$feed.'+datasource_key,'')
                storeback_helper[datastore.get('internal_name')]={'profile_key':profile_key}
                print(datastore_internal_name,profile_key)
                data=get_redis_datastore_full_data(datastore_redis_conn,datastore_internal_name,profile_key)
                schema=datastore.get('schema',{})
                
                
                for key, value in schema.items():
                    if key in data:
                        datastore_values[f"$profile.{datastore_internal_name}.{key}"]=data[key]
                    else:
                        # logger.warning(f"Key '{key}' not found in data for datastore '{datastore_internal_name}' with profile key '{profile_key}'")
                        datastore_values[f"$profile.{datastore_internal_name}.{key}"]=None
               
                            
                
                # Enrich with linked datastores
                
                for linked_datastore in datastore.get('linked_datastores',[]):
                    
                    source_datastore_key = linked_datastore.get('source_column')
                    target_datastore_internal_name = linked_datastore.get('internal_name')
                    
                    link_profile_key=data.get(source_datastore_key)
                    linked_datastore_data=get_redis_datastore_full_data(datastore_redis_conn,target_datastore_internal_name,link_profile_key)
                    schema=linked_datastore.get('schema',{})
                    storeback_helper[target_datastore_internal_name]={'profile_key':link_profile_key}
                    for key, value in schema.items():
                        if key in linked_datastore_data:
                            
                            datastore_values[f"$profile.{target_datastore_internal_name}.{key}"]=linked_datastore_data[key]
                        else:
                            # logger.warning(f"Key '{key}' not found in linked datastore data for '{target_datastore_internal_name}' with profile key '{link_profile_key}'")
                            datastore_values[f"$profile.{target_datastore_internal_name}.{key}"]=None
                    # else:
                    #     logger.warning(f"No data found for linked datastore '{target_datastore_internal_name}' with profile key '{link_profile_key}'")
            
            # Enrich with additional fields based on enriching schema

            
            print( message|datastore_values)
            parsed_message_ds = message|datastore_values
            # print(f"Parsed message with datastore values: {parsed_message_ds}")
            aggregation_message = {}
            if aggregation_schema:
                
                for key, value in aggregation_schema.items():
                    print(f"Processing aggregation for key: {key}, value: {value}")
                    formula=value.get('formula', None)
                    group_by=value.get('group_by_fields', [])
                    aggregate_type= value.get('aggregate_type','sum')
                    print('group_by',group_by)
                    group_by_values=''
                    for field in group_by:
                        print(field)
                        print(parsed_message_ds[field])
                        if not parsed_message_ds.get(field,None):
                            aggregation_message[f'$aggregation.{key}']= None
                            continue

                        

                    group_by_values=':'.join([ parsed_message_ds[field] for field in group_by if field in parsed_message_ds])
                    # if any 
                    print(f"Group by values: {group_by_values} , group_by: {group_by}")
                    print(f"Aggregate type: {aggregate_type}")
                    print("profile", f"{datasource_internal_name}:{key}")
                    result_value,result_datatype = FormulaInterpreter.evaluate_formula(formula,parsed_message_ds)
                    print(formula,result_value,result_datatype,parsed_message_ds)
                    print(aggregate_redis_conn.hgetall(f"{datasource_internal_name}:{key}"))
                    print('before agg',float(aggregate_redis_conn.hget(f"{datasource_internal_name}:{key}",group_by_values ) or 0.0))
                    aggregate_redis_conn.hincrbyfloat(f"{datasource_internal_name}:{key}",group_by_values, result_value)
                    data=(float(aggregate_redis_conn.hget(f"{datasource_internal_name}:{key}",group_by_values ) or 0.0))
                    print('after agg',data)
                    aggregation_message[f'$aggregation.{key}']= data



            enriched_message = {}
            if enriching_schema:
                for key, value in enriching_schema.items():
                    formula=value.get('formula', None)
                    storeback_table=value.get('storeback_table', None)
                    # print(f"Processing enrichment for key: {key}, formula: {formula}, storeback_table: {storeback_table}")
                    if formula:
                        try:
                            
                            # Evaluate the formula in the context of input_message
                            # print(f"Evaluating formula: {formula} with input_message: {message|datastore_values}")
                            result_value,result_datatype = FormulaInterpreter.evaluate_formula(formula, message|datastore_values)
                            # print(formula,result_value,result_datatype)
                            # print(f"Result value: {result_value}, Result datatype: {result_datatype}")
                            if storeback_table:
                                # Store the result back to Redis if storeback_table is specified
                                profile_key = storeback_helper.get(storeback_table, {}).get('profile_key', None)
                                if profile_key:
                                    datastore_redis_conn.hset(f"{storeback_table}:{profile_key}", key, result_value)
                                    logger.info(f"Stored value '{result_value}' for key '{key}' in Redis under '{storeback_table}:{profile_key}'")
                                else:
                                    logger.warning(f"No profile key found for storeback_table '{storeback_table}'")
                            enriched_message['$feed.'+key] = result_value
                        except Exception as e:
                            logger.error(f"Error evaluating formula '{formula}' for key '{key}': {e}")

            

            print(f"Enriched message: {enriched_message}")
            print(f"Aggregation message: {aggregation_message}")
            print(f"Datastore values: {datastore_values}")
            return(enriched_message | aggregation_message | datastore_values)
            
        except Exception as e:
            logger.error(f"Error enriching message: {e}")
            logger.error(traceback.format_exc())
            # return message
            raise RuntimeError(f"Error enriching message: {e}")
                
        # for 
        # for key
    def produce_message(self, message):
        try:
            # Get Kafka broker details from the .env file
            kafka_broker = os.getenv("KAFKA_BROKER_URL", "localhost:9092")

            # Initialize Kafka producer
            producer_config = {
                'bootstrap.servers': kafka_broker,
                'linger.ms': 10,  # Reduce latency by batching messages
                'acks': 'all',    # Ensure all replicas acknowledge the message
            }
            producer = Producer(producer_config)

            # Serialize the message to JSON
            message_json = json.dumps(message)

            # Produce the message to the Kafka topic
            producer.produce(
                topic=self.output_topic,
                key=None,  # You can set a key if needed
                value=message_json,
                callback=self.delivery_report
            )

            # Wait for the message to be delivered
            producer.flush()
            logger.info(f"Produced message to topic '{self.output_topic}': {message_json}")

        except Exception as e:
            logger.error(f"Error producing message to topic '{self.output_topic}': {e}")

    @staticmethod
    def delivery_report(err, msg):
        """
        Callback function to report the delivery status of a message.

        Args:
            err (KafkaError): Error object if the message delivery failed.
            msg (Message): The message that was delivered or failed.
        """
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def start(self, stop_event):
        """Start consuming messages with improved threading."""
        self.stop_event = stop_event
        logger.info(f"Starting consumer for topic: {self.topic}")
        self._initialize_consumer()
        time.sleep(5)
        
        # Fetch sample messages to infer schema if needed
        # self._fetch_sample_messages()
        messages = []
        timeout = 1  # Timeout in seconds
        start_time = time.time()
        while time.time() - start_time < timeout:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # print(f"End of partition event: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                    break
                else:
                    raise KafkaException(msg.error())
            else:
                # Deserialize the message key and value
                # key = key_deserializer(msg.key(), None)
                
                # value = value_deserializer(msg.value(), None)
                # Deserialize the message key and value
                key = msg.key()
                value = msg.value()
                # print(f"Key: {key}")
                # print(f"Value: {msg.value()}")
                # print(f"Received message: key={key}, value={value}")
                try:
                    if self.use_avro:
                        messages.append(value)  # Avro deserialized value is already a dictionary
                    else:
                        # print("messages:", messages)
                        if  messages:
                            schema= infer_json_schema(messages[0])
                        messages.append(json.loads(value))  # Parse JSON string into a dictionary
                except json.JSONDecodeError:
                    messages.append({'message': value})
        # Wait for partition assignment with retries
        # print(messages)
        max_retries = 5
        for attempt in range(max_retries):
            time.sleep(5)  # Increased wait time
            
            partitions = self.consumer.assignment()
            if partitions:
                logger.info(f"Assigned partitions: {[p.partition for p in partitions]}")
                break
            else:
                logger.warning(f"No partitions assigned (attempt {attempt+1}/{max_retries}). Retrying...")
        
        if not partitions:
            logger.error("Failed to get partition assignments after multiple attempts. Ensure the topic exists and has data.")
            return
        # self.consumer.close()
        
        # logger.info(f"Assigned partitions: {[p.partition for p in partitions]}")
        
        # Start message processor threads
        for _ in range(3):  # Start 2 message processor threads
            processor_thread = threading.Thread(target=self.message_processor)
            processor_thread.daemon = True
            processor_thread.start()
            self.threads.append(processor_thread)
        
        # Start partition consumer threads
        # for partition in partitions:

        consumer_thread = threading.Thread(target=self.consume_partition, args=(partitions,))
        consumer_thread.daemon = True
        consumer_thread.start()
        self.threads.append(consumer_thread)
        
        # Keep the main thread alive
        try:
            while not self.stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt detected. Shutting down...")
            self.stop()
    def _signal_handler(self, signum, frame):
        """Handle termination signals to clean up resources."""
        sig_name = signal.Signals(signum).name
        logger.info(f"Received signal {sig_name} in process {os.getpid()}. Cleaning up...")
        self.cleanup()
        # Let the default signal handler take over after cleanup
        signal.default_int_handler(signum, frame)
    
    def cleanup(self):
        """Clean up resources when the process is terminating."""
        # Only clean up if we're in the same process that created this instance
        if self.pid != os.getpid():
            logger.warning(f"Cleanup called in different process ({os.getpid()} vs {self.pid}). Skipping...")
            return
            
        logger.info(f"Cleaning up KafkaAvroConsumer in process {os.getpid()}...")
        try:
            # Stop all threads
            if hasattr(self, 'stop_event') and self.stop_event is not None:
                self.stop_event.set()
            
            # Wait for threads with timeout
            if hasattr(self, 'threads'):
                for thread in self.threads:
                    if thread.is_alive():
                        logger.info(f"Waiting for thread {thread.name} to finish...")
                        thread.join(timeout=2)
            
            # Close Kafka consumer
            if hasattr(self, 'consumer'):
                try:
                    logger.info("Closing Kafka consumer...")
                    self.consumer.close()
                    logger.info("Kafka consumer closed")
                except Exception as e:
                    logger.error(f"Error closing Kafka consumer: {e}")
            
            logger.info("Cleanup complete")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    # Replace the existing stop method with this improved version
    def _fetch_sample_messages(self):
        """Fetch sample messages to infer schema for non-Avro messages."""
        if self.use_avro:
            return  # No need to infer schema for Avro
        
        logger.info("Fetching sample messages to infer schema...")
        messages = []
        timeout = 5
        start_time = time.time()
        
        while time.time() - start_time < timeout and len(messages) < 5:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            
            if msg.error():
                continue
            
            try:
                value = msg.value()
                if isinstance(value, str):
                    value = json.loads(value)
                messages.append(value)
            except Exception:
                continue
        
        if messages:
            # Infer schema from sample messages
            try:
                self.schema = infer_json_schema(messages[0])
                logger.info(f"Inferred schema: {self.schema}")
            except Exception as e:
                logger.warning(f"Failed to infer schema: {e}")

    def stop(self):
        """Stop the Kafka consumer and all threads."""
        logger.info(f"Stopping Kafka consumer in process {os.getpid()}...")
        
        if not self.stop_event:
            logger.warning("Stop event not initialized, nothing to stop")
            return
        
        # Set the stop event to signal threads to stop
        self.stop_event.set()
        
        # Wait for threads to finish with timeout
        active_threads = []
        for thread in self.threads:
            if thread.is_alive():
                logger.info(f"Waiting for thread {thread.name} to finish...")
                thread.join(timeout=2)
                if thread.is_alive():
                    active_threads.append(thread.name)
        
        if active_threads:
            logger.warning(f"Some threads didn't terminate: {active_threads}")
        
        # Close the consumer
        try:
            if hasattr(self, 'consumer') and self.consumer is not None:
                self.consumer.close()
                logger.info("Kafka consumer closed")
        except Exception as e:
            logger.error(f"Error closing Kafka consumer: {e}")