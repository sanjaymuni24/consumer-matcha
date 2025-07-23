import csv
import threading
import logging
import datetime
import traceback
import os
import json
from confluent_kafka import Producer
from app.utility.operators import FormulaInterpreter
from concurrent.futures import ThreadPoolExecutor
from app.utility.datastore_operations import get_datastore_redis_connection, get_aggregate_redis_connection, get_redis_data, get_redis_datastore_full_data
from dateutil.parser import parse
import redis
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CSVConsumer:
    def __init__(self, datasource, batch_size=100000, num_threads=3):
        """
        Initialize the CSVConsumer with the datasource details.
        Args:
            datasource (dict): Details of the datasource, including connection parameters and schema.
            batch_size (int): Number of records to process in each batch.
            num_threads (int): Number of threads to use for parallel processing.
        """
        self.datasource = datasource
        self.connection_params = datasource.connection_params
        self.file_path = self.connection_params.get('file_path')
        self.delimiter = self.connection_params.get('delimiter', ',')
        self.encoding = self.connection_params.get('encoding', 'utf-8')
        self.has_header = self.connection_params.get('has_header', True)
        self.batch_size = batch_size
        self.num_threads = num_threads
        self.output_topic = 'APPLICATION_' + datasource.internal_name.upper()
        self.running = True
        self.total_records = 0
        self.fieldnames = []
        self.processed_records = 0  # Track processed records
        self.execution_status = "RUNNING"  # Track execution status
    def store_execution_history(self, total_processed, status="SUCCESS", error_message=None):
        """
        Store execution history in Redis with status, maintaining last 30 executions.
        Args:
            total_processed (int): Total number of records processed.
            status (str): Execution status ('SUCCESS', 'FAILED', 'STOPPED')
            error_message (str): Error message if execution failed
        """
        try:
            # Get Redis connection details from environment
            redis_host = os.getenv("DATASTORE_REDIS_HOST", "localhost")
            redis_port = int(os.getenv("DATASTORE_REDIS_PORT", 6379))
            redis_db = int(os.getenv("DATASTORE_REDIS_DB", 0))
            redis_password = os.getenv("DATASTORE_REDIS_PASSWORD", None)

            # Create Redis connection
            redis_conn = redis.StrictRedis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                password=redis_password,
                decode_responses=True
            )

            # Create the history key
            history_key = f"application_registry:{self.datasource.internal_name}:history_execution"
            
            # Get current datetime
            current_datetime = datetime.datetime.now().isoformat()
            
            # Create execution record with status and details
            execution_record = {
                "total_processed": total_processed,
                "status": status,
                "timestamp": current_datetime,
                "source_type": "CSV",
                "file_path": os.path.basename(self.file_path) if self.file_path else "unknown",
                "execution_id": f"{self.datasource.internal_name}_{int(datetime.datetime.now().timestamp())}"
            }
            
            # Add error message if status is FAILED
            if status == "FAILED" and error_message:
                execution_record["error_message"] = str(error_message)[:500]  # Limit error message length
            
           # Push the new execution record to the Redis list
            redis_conn.lpush(history_key, json.dumps(execution_record))
            
            # Trim the list to keep only the last 30 entries
            redis_conn.ltrim(history_key, 0, 29)
            
            logger.info(f"Stored execution history in Redis: {history_key} -> Latest entry: {execution_record}")
            
            
            # Also store the latest execution status separately for quick access
            latest_status_key = f"application_registry:{self.datasource.internal_name}:latest_status"
            redis_conn.hset(latest_status_key, mapping=execution_record)
            
            # Store execution summary statistics
            summary_key = f"application_registry:{self.datasource.internal_name}:execution_summary"
            execution_history = [json.loads(entry) for entry in redis_conn.lrange(history_key, 0, -1)]
            summary_stats = self._calculate_execution_summary(execution_history)
            redis_conn.hset(summary_key, mapping=summary_stats)
            
        except Exception as e:
            logger.error(f"Error storing execution history in Redis: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
    def _calculate_execution_summary(self, execution_history):
        """
        Calculate summary statistics from execution history.
        Args:
            execution_history (list): List of execution records
        Returns:
            dict: Summary statistics
        """
        try:
            if not execution_history:
                return {
                    "total_executions": 0,
                    "success_count": 0,
                    "failed_count": 0,
                    "stopped_count": 0,
                    "success_rate": 0.0,
                    "total_records_processed": 0,
                    "avg_records_per_execution": 0.0,
                    "last_execution_time": None,
                    "last_successful_execution": None
                }
            
            total_executions = len(execution_history)
            success_count = sum(1 for exec_record in execution_history if exec_record.get('status') == 'SUCCESS')
            failed_count = sum(1 for exec_record in execution_history if exec_record.get('status') == 'FAILED')
            stopped_count = sum(1 for exec_record in execution_history if exec_record.get('status') == 'STOPPED')
            
            success_rate = (success_count / total_executions * 100) if total_executions > 0 else 0.0
            
            total_records_processed = sum(exec_record.get('total_processed', 0) for exec_record in execution_history)
            avg_records_per_execution = total_records_processed / total_executions if total_executions > 0 else 0.0
            
            last_execution_time = execution_history[0].get('timestamp') if execution_history else None
            
            # Find last successful execution
            last_successful_execution = None
            for exec_record in execution_history:
                if exec_record.get('status') == 'SUCCESS':
                    last_successful_execution = exec_record.get('timestamp')
                    break
            
            return {
                "total_executions": str(total_executions),
                "success_count": str(success_count),
                "failed_count": str(failed_count),
                "stopped_count": str(stopped_count),
                "success_rate": f"{success_rate:.2f}",
                "total_records_processed": str(total_records_processed),
                "avg_records_per_execution": f"{avg_records_per_execution:.2f}",
                "last_execution_time": last_execution_time or "None",
                "last_successful_execution": last_successful_execution or "None"
            }
            
        except Exception as e:
            logger.error(f"Error calculating execution summary: {e}")
            return {
                "total_executions": "0",
                "success_count": "0",
                "failed_count": "0",
                "stopped_count": "0",
                "success_rate": "0.0",
                "total_records_processed": "0",
                "avg_records_per_execution": "0.0",
                "last_execution_time": "None",
                "last_successful_execution": "None"
            }

# Add the same _calculate_execution_summary and get_execution_history methods as in DatabaseConsumer
# 
    def validate_file(self):
        """
        Validate the CSV file and extract metadata.
        """
        try:
            if not os.path.exists(self.file_path):
                raise FileNotFoundError(f"CSV file not found: {self.file_path}")
            
            if not os.access(self.file_path, os.R_OK):
                raise PermissionError(f"No read permission for file: {self.file_path}")
            
            logger.info(f"CSV file validated: {self.file_path}")
            return True
        except Exception as e:
            logger.error(f"Error validating CSV file: {e}")
            raise

    def query_total_records(self):
        """
        Query the total number of records in the CSV file.
        """
        try:
            self.validate_file()
            
            with open(self.file_path, 'r', encoding=self.encoding) as csv_file:
                reader = csv.reader(csv_file, delimiter=self.delimiter)
                
                # Get field names from header if present
                if self.has_header:
                    self.fieldnames = next(reader)
                    total_records = sum(1 for _ in reader)
                else:
                    # Generate generic field names
                    first_row = next(reader)
                    self.fieldnames = [f"column_{i}" for i in range(len(first_row))]
                    total_records = sum(1 for _ in reader) + 1  # +1 for the first row we read
                
                logger.info(f"Total records in CSV file: {total_records}")
                logger.info(f"Field names: {self.fieldnames}")
                self.total_records = total_records
                return total_records
                
        except Exception as e:
            logger.error(f"Error querying total records in CSV: {e}")
            raise

    def process_chunk(self, start_row):
        """
        Process a chunk of records starting from the given row.
        Args:
            start_row (int): The starting row for the chunk.
        """
        datastore_redis_conn=get_datastore_redis_connection()
        aggregate_redis_conn=get_aggregate_redis_connection()
        try:
            with open(self.file_path, 'r', encoding=self.encoding) as csv_file:
                reader = csv.reader(csv_file, delimiter=self.delimiter)
                
                # Skip header if present
                if self.has_header:
                    next(reader)
                
                # Skip to the starting row
                for _ in range(start_row):
                    try:
                        next(reader)
                    except StopIteration:
                        break
                
                # Read batch
                rows = []
                for i in range(self.batch_size):
                    try:
                        row = next(reader)
                        # Convert row to dictionary using fieldnames
                        row_dict = dict(zip(self.fieldnames, row))
                        rows.append(row_dict)
                    except StopIteration:
                        break
                
                if rows:
                    logger.info(f"Processing chunk with {len(rows)} records starting at row {start_row}...")
                    self.process_batch(datastore_redis_conn,aggregate_redis_conn,rows)
                
        except Exception as e:
            logger.error(f"Error processing chunk starting at row {start_row}: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")

    def process_batch(self, datastore_redis_conn,aggregate_redis_conn,rows):
        """
        Process a batch of records.
        Args:
            rows (list): List of records to process.
        """
        processed_count = 0
        failed_count = 0
        
        try:
            for row in rows:
                try:
                    parsed_message = self.parse(row)
                    if parsed_message:
                        enriched_message = self.enrich_message(datastore_redis_conn,aggregate_redis_conn,parsed_message)
                        if enriched_message:
                            self.produce_message(enriched_message)
                            processed_count += 1
                        else:
                            failed_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    logger.error(f"Error processing individual row: {e}")
                    failed_count += 1
            
            logger.info(f"Batch processing completed. Processed: {processed_count}, Failed: {failed_count}")
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")

    def parse(self, row):
        """
        Parse a single record based on input schema.
        Args:
            row (dict): Raw record from the CSV file.
        Returns:
            dict: Parsed record.
        """
        try:
            input_schema = self.datasource.input_schema or {}
            input_message = {}
            parsing_schema = self.datasource.parsing_schema or {}
            parsed_message = {}
            datatype_dict = {
                'str': str,
                'int': int,
                'float': float,
                'bool': bool,
                'date': lambda x: parse(x).date() if isinstance(x, str) else x,
                'datetime': lambda x: parse(x) if isinstance(x, str) else x
            }
            
            for key, data_type in input_schema.items():
                if key in row and row[key] is not None and row[key] != '':
                    value = row[key]
                    try:
                        if data_type in datatype_dict:
                            if data_type == 'bool':
                                # Handle boolean conversion
                                input_message[key] = str(value).lower() in ['true', '1', 'yes', 'on']
                            else:
                                input_message[key] = datatype_dict[data_type](value)
                        else:
                            input_message[key] = value
                    except Exception as e:
                        logger.warning(f"Failed to convert field '{key}' with value '{value}' to type {data_type}: {e}")
                        input_message[key] = value  # Keep original value if conversion fails
                else:
                    logger.debug(f"Field '{key}' not found in row or is empty. Skipping.")
            if parsing_schema:
                for key, value in parsing_schema.items():
                    formula=value.get('formula', None)
                    if formula:
                        try:
                            
                            # Evaluate the formula in the context of input_message
                            # print(f"Evaluating formula: {formula} with input_message: {input_message}")
                            result_value,result_datatype = FormulaInterpreter.evaluate_formula(formula, input_message)
                            # # print(formula,result_value,result_datatype)
                            # print(f"Evaluating formula: {formula} with input_message: {input_message}")
                            # print(f"Result value: {result_value}, Result datatype: {result_datatype}")
                            parsed_message['$feed.'+key] = result_value
                        except Exception as e:
                            logger.error(f"Error evaluating formula '{formula}' for key '{key}': {e}")
            # print('parsed_message',parsed_message)
            return parsed_message
            
        except Exception as e:
            logger.error(f"Error parsing row: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None

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
                    print('source_datastore_key',source_datastore_key)
                    print('data',data)
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

            
            # print( message|datastore_values)
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


            print(f'storeback_helper',storeback_helper)
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
                                    logger.warning(f"No profile key found for storeback_table '{storeback_table}' ")
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
    
    def produce_message(self, enriched_message):
        """
        Produce the enriched message to Kafka.
        Args:
            enriched_message (dict): Enriched message.
        """
        try:
            # Get Kafka broker details from environment
            kafka_broker = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
            
            # Initialize Kafka producer with optimized settings
            producer_config = {
                'bootstrap.servers': kafka_broker,
                'linger.ms': 10,
                'acks': 'all',
                # 'retries': 3,
                # 'retry.backoff.ms': 100,
                # 'batch.size': 16384,
                # 'compression.type': 'snappy'
            }
            producer = Producer(producer_config)
            
            # Serialize the message to JSON
            message_json = json.dumps(enriched_message, default=str)
            
            # Produce the message to the Kafka topic
            producer.produce(
                topic=self.output_topic,
                key=enriched_message.get('id', None),  # Use 'id' field as key if available
                value=message_json,
                callback=self.delivery_report
            )
            
            # Flush to ensure delivery
            producer.flush(timeout=1)
            logger.debug(f"Produced message to topic '{self.output_topic}'")
            
        except Exception as e:
            logger.error(f"Error producing message to topic '{self.output_topic}': {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")

    @staticmethod
    def delivery_report(err, msg):
        """
        Callback function to report the delivery status of a message.
        """
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    
    
    def start(self, stop_event):
        """
        Start the consumer to process the CSV file with parallel processing.
        """
        execution_successful = False
        error_message = None
        
        try:
            self.stop_event = stop_event
            logger.info("Starting CSVConsumer...")
            
            # Reset processed records counter
            self.processed_records = 0
            self.execution_status = "RUNNING"
            
            # Query total records and validate file
            total_records = self.query_total_records()
            
            if total_records == 0:
                logger.warning("No records found in CSV file.")
                # Store execution history even if no records
                self.store_execution_history(0, "SUCCESS")
                return
            
            # Calculate chunk offsets
            chunk_offsets = list(range(0, total_records, self.batch_size))
            logger.info(f"Processing {total_records} records in {len(chunk_offsets)} chunks using {self.num_threads} threads")
            
            # Process chunks in parallel
            with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
                futures = []
                for offset in chunk_offsets:
                    if self.stop_event.is_set():
                        break
                    future = executor.submit(self.process_chunk, offset)
                    futures.append(future)
                
                # Wait for all futures to complete
                execution_errors = []
                for future in futures:
                    if self.stop_event.is_set():
                        self.execution_status = "STOPPED"
                        break
                    try:
                        future.result(timeout=300)  # 5 minute timeout per chunk
                    except Exception as e:
                        logger.error(f"Error in chunk processing: {e}")
                        execution_errors.append(str(e))
                
                # Determine final execution status
                if self.stop_event.is_set():
                    self.execution_status = "STOPPED"
                    logger.info("CSV processing was stopped by user.")
                elif execution_errors:
                    self.execution_status = "FAILED"
                    error_message = "; ".join(execution_errors[:3])  # Limit to first 3 errors
                    logger.error(f"CSV processing completed with errors: {error_message}")
                else:
                    self.execution_status = "SUCCESS"
                    execution_successful = True
                    logger.info(f"CSV processing completed successfully. Total processed: {self.processed_records}")
            
            # Store execution history in Redis
            self.store_execution_history(
                self.processed_records, 
                self.execution_status, 
                error_message
            )
            
        except Exception as e:
            self.execution_status = "FAILED"
            error_message = str(e)
            logger.error(f"Error starting CSV consumer: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            # Store execution history even on error
            self.store_execution_history(
                getattr(self, 'processed_records', 0), 
                "FAILED", 
                error_message
            )
            raise

    def stop(self):
        """
        Stop the consumer gracefully.
        """
        logger.info("Stopping CSV consumer...")
        self.running = False
        self.execution_status = "STOPPED"
        
        if hasattr(self, 'stop_event'):
            self.stop_event.set()
        
        # Store execution history when stopped
        if hasattr(self, 'processed_records'):
            logger.info(f"Consumer stopped. Final processed count: {self.processed_records}")
            self.store_execution_history(
                self.processed_records, 
                "STOPPED", 
                "Consumer was stopped by user"
            )
        
        logger.info("CSV consumer stopped.")