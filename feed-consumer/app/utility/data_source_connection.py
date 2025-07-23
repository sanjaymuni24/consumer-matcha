import json,os
from sqlalchemy import create_engine
from sqlalchemy.sql import text 
from confluent_kafka import Consumer, KafkaException, KafkaError,DeserializingConsumer,Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer



import pandas as pd
import time
import  traceback
import json


def connect_to_data_source(data_source):
    """
    Connect to the specified data source based on its type and connection parameters.
    """
    datasource_type = data_source.datasource_type
    connection_params = data_source.connection_params

    if datasource_type == 'Postgres':
        # Build the connection string for Postgres
        connection_string = f"postgresql://{connection_params['username']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}"
        print("Postgres connection string:", connection_string)
        connection= create_engine(connection_string).connect()
        connection.execute(text("SELECT 1"))  # Test the connection
        print("Postgres connection successful.")
        connection.close()  # Close the connection
        return connection

    elif datasource_type == 'Mysql':
        # Build the connection string for MySQL
        connection_string = f"mysql+pymysql://{connection_params['username']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}"
        connection= create_engine(connection_string).connect()
        connection.execute(text("SELECT 1"))  # Test the connection
        print("Postgres connection successful.")
        connection.close()  # Close the connection
        return connection

    elif datasource_type == 'Kafka':
        # Kafka connection logic (placeholder, as SQLAlchemy is not used for Kafka)
        # Kafka connection logic
        brokers = connection_params.get('brokers')
        topic = connection_params.get('topic')
        group_id = connection_params.get('group_id', 'test-group')  # Default group ID if not provided
        
        if not brokers or not topic:
            raise ValueError("Kafka connection requires 'brokers' and 'topic' parameters.")

        consumer_config = {
            'bootstrap.servers': brokers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',  # Start reading from the earliest message
            'enable.auto.commit': False      # Disable auto-commit
        }

        consumer = Consumer(consumer_config)

        try:
            # Subscribe to the topic
            consumer.subscribe([topic])
            # print(f"Subscribed to Kafka topic: {topic}")

            # Poll for a message to validate the connection
            msg = consumer.poll(timeout=5.0)  # Timeout in seconds

            if msg is None:
                print("Kafka connection successful, but no messages available.")
            elif msg.error():
                raise KafkaException(msg.error())
            else:
                print(f"Kafka connection successful. Received message: {msg.value()}")

        finally:
            # Close the consumer without committing offsets
            consumer.close()
            # print("Kafka consumer closed.")
        # raise NotImplementedError("Kafka connection is not implemented with SQLAlchemy.")

    elif datasource_type == 'RabbitMQ':
        # RabbitMQ connection logic (placeholder, as SQLAlchemy is not used for RabbitMQ)
        raise NotImplementedError("RabbitMQ connection is not implemented.")

    elif datasource_type == 'S3':
        # S3 connection logic (placeholder, as SQLAlchemy is not used for S3)
        raise NotImplementedError("S3 connection is not implemented with SQLAlchemy.")

    elif datasource_type == 'Hive':
        # Hive connection logic (placeholder, as SQLAlchemy is not used for Hive)
        raise NotImplementedError("Hive connection is not implemented with SQLAlchemy.")

    elif datasource_type == 'HDFS':
        # HDFS connection logic (placeholder, as SQLAlchemy is not used for HDFS)
        raise NotImplementedError("HDFS connection is not implemented with SQLAlchemy.")

    elif datasource_type == 'CSV':
        # CSV connection logic (placeholder, as SQLAlchemy is not used for CSV)
        csv_file_path = connection_params.get('file_path')

        if not csv_file_path or not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"CSV file not found at path: {csv_file_path}")

        try:
            # Read the CSV file using Pandas
            df = pd.read_csv(csv_file_path)
            print(f"CSV file loaded successfully with {len(df)} rows and {len(df.columns)} columns.")
            return df  # Return the DataFrame for further processing
        except Exception as e:
            raise ValueError(f"Error loading CSV file: {e}")

    else:
        raise ValueError(f"Unsupported data source type: {datasource_type}")

def connect_to_data_sink(data_sink):
    """
    Connect to the specified data sink based on its type and connection parameters.
    """
    datasink_type = data_sink.datasink_type
    connection_params = data_sink.connection_params
    print("Connection params:", connection_params)
    if datasink_type == 'Postgres':
        # Build the connection string for Postgres
        connection_string = f"postgresql://{connection_params['username']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}"
        print("Postgres connection string:", connection_string)
        connection = create_engine(connection_string).connect()
        connection.execute(text("SELECT 1"))  # Test the connection
        print("Postgres connection successful.")
        connection.close()  # Close the connection
        return connection

    elif datasink_type == 'Mysql':
        # Build the connection string for MySQL
        connection_string = f"mysql+pymysql://{connection_params['username']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}"
        connection = create_engine(connection_string).connect()
        connection.execute(text("SELECT 1"))  # Test the connection
        print("MySQL connection successful.")
        connection.close()  # Close the connection
        return connection

    elif datasink_type == 'Kafka':
        # Kafka connection logic
        brokers = connection_params.get('brokers')
        topic = connection_params.get('topic')
        group_id = connection_params.get('group_id', 'test-group')  # Default group ID if not provided

        if not brokers or not topic:
            raise ValueError("Kafka connection requires 'brokers' and 'topic' parameters.")

        producer_config = {
            'bootstrap.servers': brokers,
        }

        producer = Producer(producer_config)

        try:
            # Test Kafka connection by producing a test message
            producer.produce(topic, key="test-key", value="test-message")
            producer.flush()
            print(f"Kafka connection successful. Test message sent to topic: {topic}")
        except Exception as e:
            raise ValueError(f"Error connecting to Kafka: {e}")

        return producer

    # elif datasink_type == 'S3':
    #     # S3 connection logic
    #     bucket_name = connection_params.get('bucket_name')
    #     access_key = connection_params.get('access_key')
    #     secret_key = connection_params.get('secret_key')
    #     region = connection_params.get('region', 'us-east-1')  # Default region

    #     if not bucket_name or not access_key or not secret_key:
    #         raise ValueError("S3 connection requires 'bucket_name', 'access_key', and 'secret_key' parameters.")

    #     try:
    #         # Test S3 connection by listing buckets
    #         s3_client = boto3.client(
    #             's3',
    #             aws_access_key_id=access_key,
    #             aws_secret_access_key=secret_key,
    #             region_name=region
    #         )
    #         s3_client.list_buckets()
    #         print("S3 connection successful.")
    #     except Exception as e:
    #         raise ValueError(f"Error connecting to S3: {e}")

    #     return s3_client

    # elif datasink_type == 'CSV':
        # CSV connection logic
        file_path = connection_params.get('file_path')

        if not file_path or not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found at path: {file_path}")

        try:
            # Test CSV connection by reading the file
            df = pd.read_csv(file_path)
            print(f"CSV file loaded successfully with {len(df)} rows and {len(df.columns)} columns.")
            return df  # Return the DataFrame for further processing
        except Exception as e:
            raise ValueError(f"Error loading CSV file: {e}")

    else:
        raise ValueError(f"Unsupported data sink type: {datasink_type}")
def map_column_schema(dtypes):
    """
    Map Pandas DataFrame column dtypes to a schema with column names and datatypes.
    
    Args:
        dtypes (pd.Series): Pandas Series containing column names as index and dtypes as values.
    
    Returns:
        list: A list of dictionaries containing column names and their mapped datatypes.
    """
    datatype_mapping = {
        'object': 'str',
        'float64': 'float',
        'int64': 'int',
        'datetime64[ns]': 'datetime',
        'bool': 'boolean'
    }

    column_schema = {}
    for column, pandas_dtype in dtypes.items():
        mapped_dtype = datatype_mapping.get(str(pandas_dtype), 'unknown')  # Default to 'unknown' if no match
        column_schema[column]= mapped_dtype

    return column_schema
def get_avro_schema(schema_registry_url, topic):
    """
    Get the Avro schema for a Kafka topic from the Schema Registry.

    Args:
        schema_registry_url (str): URL of the Schema Registry.
        topic (str): Kafka topic name.

    Returns:
        dict: Schema with column names and their mapped datatypes.
    """
    schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
    subject = f"{topic}-value"  # Subject name for the topic's value schema

    try:
        # Get the latest version of the schema
        schema_metadata = schema_registry_client.get_latest_version(subject)
        schema_str = schema_metadata.schema.schema_str
        avro_schema = json.loads(schema_str)  # Parse the schema string into a dictionary
        schema = {}
        if 'fields' in avro_schema:
            for field in avro_schema['fields']:
                column_name = field['name']
                column_type = map_avro_type(field['type'])  # Use the mapping function
                schema[column_name] = column_type

        return schema
    except Exception as e:
        print(f"Error fetching schema for topic {topic}: {e}")
        return None
def get_kafka_schema(datasource):
    """
    Get the schema for a Kafka topic (JSON or Avro).
    
    Args:
        datasource (object): DataSource object containing connection parameters.
    
    Returns:
        dict: Schema with column names and their datatypes.
    """
    brokers = datasource.connection_params['brokers']
    topic = datasource.connection_params['topic']
    schema_registry_url = datasource.connection_params.get('schema_registry_url')

    # Fetch messages from Kafka
    consumer_config = {
        'bootstrap.servers': brokers,
        'group.id': 'schema_group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    }

    use_avro = False
    if schema_registry_url:
        schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
        subjects = schema_registry_client.get_subjects()
        if f"{topic}-value" in subjects:
            use_avro = True

    if use_avro:
        # Get Avro schema
        return get_avro_schema(schema_registry_url, topic)
    else:
        # Consume messages and infer JSON schema
        consumer = Consumer(consumer_config)
        consumer.subscribe([topic])

        messages = []
        for _ in range(5):  # Fetch 5 messages
            msg = consumer.poll(timeout=1.0)
            if msg and not msg.error():
                messages.append(json.loads(msg.value()))

        consumer.close()
        return infer_json_schema(messages)
def map_avro_type(avro_type):
    """
    Map Avro types to one of the desired types: str, int, float, bool, dict, list, unknown.

    Args:
        avro_type (str or dict or list): Avro type (e.g., "string", ["null", "int"], {"type": "array", "items": "string"}).

    Returns:
        str: Mapped type (e.g., "str", "int", "float", "bool", "dict", "list", "unknown").
    """
    if isinstance(avro_type, str):
        # Simple types
        avro_mapping = {
            "string": "str",
            "int": "int",
            "long": "int",
            "float": "float",
            "double": "float",
            "boolean": "bool",
            "bytes": "str",
            "null": "unknown"
        }
        return avro_mapping.get(avro_type, "unknown")

    elif isinstance(avro_type, list):
        # Union types (e.g., ["null", "string"])
        non_null_types = [t for t in avro_type if t != "null"]
        if len(non_null_types) == 1:
            return map_avro_type(non_null_types[0])
        return "unknown"  # Multiple types are ambiguous

    elif isinstance(avro_type, dict):
        # Complex types (e.g., arrays, maps)
        if avro_type.get("type") == "array":
            return "list"
        elif avro_type.get("type") == "map":
            return "dict"
        return "unknown"

    return "unknown"
def infer_json_schema(message):
    """
    Infer the schema from JSON messages.
    
    Args:
        messages (list): List of JSON messages (as dictionaries).
    
    Returns:
        dict: Schema with column names and their datatypes.
    """
    if not message:
        return {}

    schema = {}
    for key, value in message.items():  # Use the first message to infer schema
        if isinstance(value, str):
            schema[key] = 'str'
        elif isinstance(value, int):
            schema[key] = 'int'
        elif isinstance(value, float):
            schema[key] = 'float'
        elif isinstance(value, bool):
            schema[key] = 'bool'
        elif isinstance(value, dict):
            schema[key] = 'dict'
        elif isinstance(value, list):
            schema[key] = 'list'
        else:
            schema[key] = 'unknown'

    return schema

def query_dataset(datasource):
    """
    Query data from Postgres or Kafka based on the data source type and return the results.
    """
    
    
      # Fetch the data source from the database

    try:
        if datasource.datasource_type == 'Postgres':
            # Query data from Postgres
            connection_string = f"postgresql://{datasource.connection_params['username']}:{datasource.connection_params['password']}@{datasource.connection_params['host']}:{datasource.connection_params['port']}/{datasource.connection_params['database']}"
            engine = create_engine(connection_string)
            query = datasource.connection_params['query']  # Replace with your query
            if 'LIMIT' not in query.upper():
                query = f"{query.strip().rstrip(';')} LIMIT 5;"
            df = pd.read_sql_query(query, engine)  # Use Pandas to execute the query
            results = df.to_dict(orient='records')  # Convert the results to a list of dictionaries
            schema = map_column_schema(df.dtypes)

        elif datasource.datasource_type == 'Kafka':
            # Kafka consumer configuration
            brokers = datasource.connection_params['brokers']
            topic = datasource.connection_params['topic']
            group_id = datasource.connection_params.get('group_id', 'query_group')
            schema_registry_url = datasource.connection_params.get('schema_registry_url')
            if not brokers or not topic:
                raise ValueError("Kafka connection requires 'brokers' and 'topic' parameters.")

            consumer_config = {
                'bootstrap.servers': brokers,
                'group.id': group_id,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
            }
            # Check if Avro deserialization is needed
            # Use Avro deserializer if use_avro is True
            use_avro = False
            schema=''
            if schema_registry_url:
                schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
                subjects = schema_registry_client.get_subjects()
                # print(subjects)
                if f"{topic}-value" in subjects:
                    use_avro = True
                    # print(f"Using Avro deserializer for topic: {topic}")
            if use_avro:
                schema=get_avro_schema(schema_registry_url, topic)
                if not schema_registry_url:
                    raise ValueError("Schema Registry URL is required for Avro deserialization.")

                schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
                avro_deserializer = AvroDeserializer(schema_registry_client)

                key_deserializer = StringDeserializer('utf_8')
                value_deserializer = avro_deserializer
                
            else:
                # Use normal JSON deserialization
                key_deserializer = StringDeserializer('utf_8')
                value_deserializer = StringDeserializer('utf_8')
                consumer = Consumer(consumer_config)

            # Create the Kafka consumer
            # Create the DeserializingConsumer
            consumer_config.update({
                'key.deserializer': key_deserializer,
                'value.deserializer': value_deserializer,
            })
            consumer = DeserializingConsumer(consumer_config)
            consumer.subscribe([topic])

            messages = []
            timeout = 5  # Timeout in seconds
            start_time = time.time()

            while time.time() - start_time < timeout:
                msg = consumer.poll(timeout=1.0)
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
                        if use_avro:
                            messages.append(value)  # Avro deserialized value is already a dictionary
                        else:
                            # print("messages:", messages)
                            if  messages:
                                schema= infer_json_schema(messages[0])
                            messages.append(json.loads(value))  # Parse JSON string into a dictionary
                    except json.JSONDecodeError:
                        messages.append({'message': value})

            consumer.close()
            results = messages
        elif datasource.datasource_type == 'CSV':
            # Query data from CSV
            csv_file_path = datasource.connection_params.get('file_path')
            delimiter = datasource.connection_params.get('delimiter', ',')
            encoding = datasource.connection_params.get('encoding', 'utf-8')
            has_header = datasource.connection_params.get('has_header', None)
            
            header = None
            if has_header is not None:
                header = 0 if has_header=='True' else None  # Use 0 for header row, None for no header
            # Validate the CSV file path
             # Validate the CSV file path

            if not csv_file_path or not os.path.exists(csv_file_path):
                raise FileNotFoundError(f"CSV file not found at path: {csv_file_path}")

            try:
                # Read the CSV file using Pandas
                df = pd.read_csv(csv_file_path,delimiter=delimiter, encoding=encoding, header=header)
                results = df.to_dict(orient='records')  # Convert the results to a list of dictionaries
                schema = map_column_schema(df.dtypes)
            except Exception as e:
                raise ValueError(f"Error loading CSV file: {e}")

        else:
            return {'success': False, 'error': 'Unsupported data source type.'}
        stored_schema=None

        try:
            stored_schema=datasource.schema.input_schema or {}
        except:
            stored_shema={}
        try:
            parsing_schema=datasource.schema.parsing_schema or {}
        except:
            parsing_schema={}
        try:
            enrichment_schema=datasource.schema.enrichment_schema or {}
        except:
            enrichment_schema={}
        try:
            aggregation_schema=datasource.schema.aggregation_schema or {}
        except:
            aggregation_schema={}
        
        
            
        if schema!=stored_schema:
            print("Schema mismatch detected.")
            schema_changed= True
        else:
            print("Schema is up-to-date.")
            schema_changed= False
        return {'success': True, 'results': results,'schema': schema,"stored_schema":stored_schema,"schema_changed":schema_changed,'parsing_schema':parsing_schema, 'enrichment_schema':enrichment_schema, 'aggregation_schema': aggregation_schema}    

    except Exception as e:
        traceback.print_exc()
        return {'success': False, 'error': str(e)}

def generate_enricher_params(datasource):
    """
    Generate enrichment parameters for a given datasource, including optimum pod configurations.

    Args:
        datasource (object): DataSource object containing connection parameters.

    Returns:
        dict: Enrichment parameters including fine-tuning details and pod configurations.
    """
    enricher_params = {}

    try:
        if datasource.datasource_type == 'Kafka':
            # Kafka-specific enrichment parameters
            brokers = datasource.connection_params.get('brokers')
            topic = datasource.connection_params.get('topic')

            if not brokers or not topic:
                raise ValueError("Kafka connection requires 'brokers' and 'topic' parameters.")

            consumer_config = {
                'bootstrap.servers': brokers,
                'group.id': 'enrichment_group',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
            }

            consumer = Consumer(consumer_config)
            consumer.subscribe([topic])

            try:
                # Fetch metadata for the topic
                metadata = consumer.list_topics(topic, timeout=10)
                topic_metadata = metadata.topics[topic]

                # Number of partitions
                enricher_params['num_partitions'] = len(topic_metadata.partitions)

                # Estimate messages per second and size of messages
                messages = []
                start_time = time.time()
                for _ in range(10):  # Fetch 10 messages
                    msg = consumer.poll(timeout=1.0)
                    if msg and not msg.error():
                        messages.append(msg)
                end_time = time.time()

                if messages:
                    total_size = sum(len(msg.value()) for msg in messages)
                    enricher_params['avg_message_size_bytes'] = total_size / len(messages)
                    enricher_params['messages_per_second'] = len(messages) / (end_time - start_time)
                else:
                    enricher_params['avg_message_size_bytes'] = 0
                    enricher_params['messages_per_second'] = 0

                # Calculate optimum pod configurations
                total_message_size_per_second = enricher_params['messages_per_second'] * enricher_params['avg_message_size_bytes']
                enricher_params['min_pods'] = max(1, int(total_message_size_per_second / (1024 * 1024 * 10)))  # Assuming 10 MB per pod
                enricher_params['max_pods'] = enricher_params['min_pods'] + 2
                enricher_params['cpu_per_pod'] = 0.5  # Example CPU per pod
                enricher_params['memory_per_pod'] = 512  # Example memory per pod in MB
                enricher_params['computed_time']= time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

            finally:
                consumer.close()

        elif datasource.datasource_type in ['Postgres', 'Mysql']:
            # Database-specific enrichment parameters
            sqlalchemy_type = {'Postgres': 'postgresql', 'Mysql': 'mysql+pymysql'}[datasource.datasource_type]
            connection_string = f"{sqlalchemy_type}://{datasource.connection_params['username']}:{datasource.connection_params['password']}@{datasource.connection_params['host']}:{datasource.connection_params['port']}/{datasource.connection_params['database']}"
            print("Database connection string:", connection_string)
            engine = create_engine(connection_string)

            query = datasource.connection_params.get('query', 'SELECT * FROM table_name LIMIT 100')  # Replace with a default query
            start_time = time.time()
            df = pd.read_sql_query(query, engine)
            end_time = time.time()

            enricher_params['row_count'] = len(df)
            enricher_params['avg_row_size_kb'] = (df.memory_usage(deep=True).sum() / len(df) / 1024) if len(df) > 0 else 0
            enricher_params['query_execution_time_seconds'] = end_time - start_time
            enricher_params['default_schedule'] = {'frequency': 'daily', 'time_window': '12:00 AM - 6:00 AM'}

            # Calculate optimum pod configurations
            total_data_size_mb = (enricher_params['row_count'] * enricher_params['avg_row_size_kb']) / 1024  # Convert KB to MB
            enricher_params['min_pods'] = max(1, int(total_data_size_mb / 10))  # Assuming 10 MB per pod
            enricher_params['max_pods'] = enricher_params['min_pods'] + 1
            enricher_params['cpu_per_pod'] = 0.3  # Example CPU per pod
            enricher_params['memory_per_pod'] = 256  # Example memory per pod in MB
            enricher_params['computed_time']= time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        
        elif datasource.datasource_type == 'CSV':
            # CSV-specific enrichment parameters
            csv_file_path = datasource.connection_params.get('file_path')
            if not csv_file_path or not os.path.exists(csv_file_path):
                raise FileNotFoundError(f"CSV file not found at path: {csv_file_path}")

            total_records = 0
            total_size_kb = 0
            for file_name in os.listdir(os.path.dirname(csv_file_path)):
                file_path = os.path.join(os.path.dirname(csv_file_path), file_name)
                if os.path.isfile(file_path) and file_path.endswith('.csv'):
                    df = pd.read_csv(file_path)
                    total_records += len(df)
                    total_size_kb += os.path.getsize(file_path) / 1024  # Convert bytes to KB

            enricher_params['total_records'] = total_records
            enricher_params['total_size_kb'] = total_size_kb

            # Calculate optimum pod configurations
            total_data_size_mb = total_size_kb / 1024  # Convert KB to MB
            enricher_params['min_pods'] = max(1, int(total_data_size_mb / 10))  # Assuming 10 MB per pod
            enricher_params['max_pods'] = enricher_params['min_pods'] + 1
            enricher_params['cpu_per_pod'] = 0.2  # Example CPU per pod
            enricher_params['memory_per_pod'] = 128  # Example memory per pod in MB
            enricher_params['computed_time']= time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        else:
            raise ValueError(f"Unsupported datasource type: {datasource.datasource_type}")

    except Exception as e:
        print(f"Error generating enrichment parameters: {e}")
        enricher_params['error'] = str(e)

    return enricher_params