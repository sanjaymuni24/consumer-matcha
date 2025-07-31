import os,sys,time,signal,atexit
import threading
from fastapi import FastAPI, BackgroundTasks
from app.routers import router as api_router
import requests
import json
from types import SimpleNamespace
from .utility.data_source_connection import connect_to_data_source, query_dataset
from app.utility.confluent_avro_consumer import  KafkaAvroConsumer
from app.utility.database_consumer import DatabaseConsumer
import psutil
import socket
import redis
from app.utility.csv_consumer import CSVConsumer
from app.utility.campaign_evaluator import CampaignEvaluator  # Import the CampaignEvaluator class

import traceback
import argparse


app = FastAPI()

# Environment variables for Django API
DJANGO_API_URL = os.getenv("DJANGO_API_URL", "http://127.0.0.1:8000/api")  # Default to Django service in OpenShift
AUTH_TOKEN = os.getenv("AUTH_TOKEN", "your_default_token")
# mode= os.getenv("APP_MODE", "feedEnricher")  # Default to feedEnricher mode
app.include_router(api_router)

# Global variables
stop_event = threading.Event()  # Event to signal threads to stop
kafka_consumer = None  # Reference to the Kafka consumer
datasource_details=None
csv_consumer = None 
health_threads = []  
# Add this function to handle different modes
# @app.on_event("startup")
def start_application_mode(mode):
    """
    Start the application in the specified mode.
    """
    # global mode
    if mode == "feedEnricher":
        print("Starting in feedEnricher mode...")
        fetch_datasource_details()
    elif mode == "campaignEvaluator":
        print("Starting in campaignEvaluator mode...")
        fetch_campaign_details()
    elif mode == "eventActuator":
        print("Starting in eventActuator mode...")
        start_event_actuator()
    else:
        print(f"Invalid mode: {mode}. Please choose from 'feedEnricher', 'campaignEvaluator', or 'eventActuator'.")
        exit(1)

def fetch_campaign_details():
    """
    Fetch campaign details from the Django server and start the CampaignEvaluator.
    """
    try:
        headers = {"Authorization": f"Bearer {AUTH_TOKEN}"}
        response = requests.get(f"{DJANGO_API_URL}/active-campaigns/", headers=headers)

        if response.status_code == 200:
            response_data = response.json()
            print("Campaign details fetched successfully:", response_data)

            # Extract datasources and campaigns
            datasources = response_data.get("datasources", [])
            for datasource in datasources:
                campaigns = datasource.get("campaigns", [])
                if campaigns:
                    print(f"Starting CampaignEvaluator for datasource: {datasource['name']} with {len(campaigns)} campaigns.")
                    
                    # Initialize and start the CampaignEvaluator
                    kafka_config = {
                        "brokers": os.getenv("KAFKA_BROKERS", "localhost:9092"),
                        "group_id": f"campaign-evaluator-{datasource['internal_name']}",
                        "auto_offset_reset": "earliest",
                        "topic": os.getenv("KAFKA_TOPIC", "qualified_events"),
                    }
                    db_config = {
                        "host": os.getenv("DB_HOST", "localhost"),
                        "port": os.getenv("DB_PORT", 5432),
                        "user": os.getenv("DB_USER", "pgadmin"),
                        "password": os.getenv("DB_PASSWORD", "adminpass"),
                        "database": os.getenv("DB_NAME", "postgres"),
                        "table": os.getenv("DB_TABLE", "qualified_events")
                    }

                    campaign_evaluator = CampaignEvaluator(
                        datasource=datasource,
                        campaigns=campaigns,
                        kafka_config=kafka_config,
                        db_config=db_config
                    )
                    campaign_evaluator.start()
                else:
                    print(f"No campaigns found for datasource: {datasource['name']}")
        else:
            print(f"Failed to fetch campaign details. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"Error fetching campaign details: {e}")
        traceback.print_exc()
        
def start_event_actuator():



    
    """
    Start the event actuator process.
    """
    print("Starting event actuator...")
    # Add your event actuator logic here
    try:
        # Example placeholder logic
        print("Event actuator is running...")
        time.sleep(5)  # Simulate some processing
        print("Event actuator completed.")
    except Exception as e:
        print(f"Error in event actuator: {e}")
        raise(e)

def dict_to_obj(d):
    if isinstance(d, dict):
        return SimpleNamespace(**{k: dict_to_obj(v) for k, v in d.items()})
    elif isinstance(d, list):
        return [dict_to_obj(i) for i in d]
    return d


@app.get("/")
def read_root():
    return {"message": "Welcome to the FastAPI project!"}


# @app.on_event("startup")
def fetch_datasource_details():
    """
    Fetch datasource details from the Django server when FastAPI starts.
    """
    global kafka_consumer,datasource_details
    try:
        headers = {"Authorization": f"Bearer {AUTH_TOKEN}"}
        response = requests.get(f"{DJANGO_API_URL}/get-next-datasource-and-related_details/", headers=headers)

        if response.status_code == 200:
            response = response.json()
            print("response:", response)
            datasource_details = response.get('data', {}).get('datasource', {})
            if not datasource_details:
                print("No datasource details found in the response.")
                sys.exit(0)
            print("Datasource details fetched successfully:", datasource_details)
            # Start consuming and enriching process
            start_consuming_and_enriching(datasource_details)
        else:
            print(f"Failed to fetch datasource details. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"Error fetching datasource details: {e}")
        raise(e)

def start_consuming_and_enriching(datasource_details):
    """
    Start the consuming and enriching process based on the datasource details.
    """
    global kafka_consumer

    datasource = SimpleNamespace(**datasource_details)
    # print(f"Datasource details: {datasource}")
    datasource_type = datasource.datasource_type
    print(f"Starting consumer for datasource : {datasource.name} type: {datasource_type} scaling {datasource.current_instance} instances out of {datasource.min_pods} minimum pods")
    # print(f"Datasource details: {datasource}")
    health_beacon(datasource_details)
    if datasource.datasource_type == "Kafka":
        # Generate a unique group ID to avoid conflicts
        # import uuid
        # unique_group_id = f"{datasource.connection_params.get('group_id', 'consumer-group')}-{uuid.uuid4().hex[:8]}"
        group_id=datasource.connection_params.get('group_id', datasource.connection_params.get('group_id', 'consumer-group'))
        print(f"Connecting to Kafka with brokers: {datasource.connection_params['brokers']}")
        print(f"Using topic: {datasource.connection_params['topic']}")
        print(f"Using group ID: {group_id}")
        
        # Validate topic exists before creating consumer
        try:
            from confluent_kafka.admin import AdminClient
            admin = AdminClient({'bootstrap.servers': datasource.connection_params['brokers']})
            topics = admin.list_topics(timeout=10)
            
            if datasource.connection_params['topic'] not in topics.topics:
                print(f"WARNING: Topic '{datasource.connection_params['topic']}' not found!")
                print(f"Available topics: {list(topics.topics.keys())}")
            else:
                print(f"Topic '{datasource.connection_params['topic']}' exists with {len(topics.topics[datasource.connection_params['topic']].partitions)} partitions")
        except Exception as e:
            print(f"Error validating topic: {e}")
        

        # Create and start the consumer
        kafka_consumer = KafkaAvroConsumer(
            brokers=datasource.connection_params['brokers'],
            group_id=group_id,
            topic=datasource.connection_params['topic'],
            schema_registry_url=datasource.connection_params.get('schema_registry_url', ''),
            num_threads=4,
            datasource=datasource,
        )
          # Initialize the consumer with debug info
        # Start with extended debug info
        print("Starting Kafka consumer...")
        kafka_consumer.start(stop_event)
        # health_beacon(datasource_details)
    elif datasource.datasource_type in ("Postgres","Mysql"):
        # Postgres-specific logic
        try:
            print("Connecting to Database...")
            
            database_consumer = DatabaseConsumer(datasource)
            database_consumer.start(stop_event)
        except Exception as e:
            print(f"Error connecting to Postgres: {e}")

    elif datasource.datasource_type == "CSV":
    # CSV-specific logic
        try:
            print("Processing CSV file...")
            
            # Validate required connection parameters
            connection_params = datasource.connection_params
            file_path = connection_params.get('file_path')
            if not file_path:
                raise ValueError("CSV file path is missing in connection parameters.")
            
            # Log CSV configuration
            delimiter = connection_params.get('delimiter', ',')
            encoding = connection_params.get('encoding', 'utf-8')
            has_header = connection_params.get('has_header', True)
            
            print(f"CSV Configuration:")
            print(f"  File path: {file_path}")
            print(f"  Delimiter: '{delimiter}'")
            print(f"  Encoding: {encoding}")
            print(f"  Has header: {has_header}")
            
            # Start the health beacon in a separate thread for CSV
            # try:
            #     print("Starting health beacon for CSV in a separate thread...")
            #     health_thread = threading.Thread(target=health_beacon, args=(datasource_details,))
            #     health_thread.daemon = True
            #     health_thread.start()
            #     health_threads.append(health_thread) 
            # except Exception as e:
            #     print(f"Error starting health beacon: {e}")
            
            # Create and start the CSV consumer
            csv_consumer = CSVConsumer(
                datasource=datasource,
                batch_size=50000,  # Adjust based on file size and memory
                num_threads=3      # Adjust based on system capabilities
            )
            
            print("Starting CSV consumer...")
            csv_consumer.start(stop_event)
            
        except FileNotFoundError as e:
            print(f"CSV file not found: {e}")
        except PermissionError as e:
            print(f"Permission denied accessing CSV file: {e}")
        except ValueError as e:
            print(f"Configuration error: {e}")
        except Exception as e:
            print(f"Error processing CSV file: {e}")
            print(f"Traceback: {traceback.format_exc()}")
    else:
        print(f"Unsupported datasource type: {datasource_type}")
@app.on_event("shutdown")
def shutdown():
    """
    Gracefully stop all threads and services during shutdown.
    """
    
    print("Shutting down services...")
    cleanup_resources()
    print("All services stopped.")

@app.post("/shutdown")
def shutdown_api(background_tasks: BackgroundTasks):
    """
    API to gracefully shut down the application.
    """
    print("Shutdown API called. Stopping services...")
    
    # Run shutdown in the background to allow response to be sent
    background_tasks.add_task(graceful_shutdown)
    
    return {"message": "Application is shutting down."}

def graceful_shutdown():
    """
    Perform shutdown and then exit the application gracefully.
    """
    shutdown()  # Call the shutdown logic
    time.sleep(2)  # Give some time for the response to be sent
    print("Exiting application gracefully...")
    
    # Get parent process ID (Uvicorn reloader process)
    ppid = os.getppid()
    pid = os.getpid()
    
    print(f"Terminating process {pid} and parent process {ppid}")
    
    # Send SIGTERM to both current and parent process
    try:
        os.kill(ppid, signal.SIGTERM)  # Kill parent (reloader) process
    except OSError:
        pass  # Ignore if parent process doesn't exist
    
    os.kill(pid, signal.SIGTERM)  # Kill current process

# Global variables
stop_event = threading.Event()  # Event to signal threads to stop
kafka_consumer = None  # Reference to the Kafka consumer
app_pid = os.getpid()  # Store the process ID when the module is loaded
def list_active_threads():
    print("Active threads:")
    for thread in threading.enumerate():
        print(f"Thread Name: {thread.name}, Daemon: {thread.daemon}, Alive: {thread.is_alive()}")
def cleanup_resources():
    """Clean up all resources when the application is shutting down."""
    global kafka_consumer, stop_event, datasource_details, health_threads

    # Only clean up in the process that initialized these resources
    if os.getpid() != app_pid:
        print(f"Cleanup called in different process ({os.getpid()} vs {app_pid}). Skipping...")
        return

    print(f"Cleaning up resources in process {os.getpid()}...")
    try:
        # Signal threads to stop
        stop_event.set()
        list_active_threads()

        # Stop Kafka consumer
        if kafka_consumer:
            kafka_consumer.stop()

        # Stop and join health beacon threadss
        for thread in health_threads:
            print(f"Joining thread: {thread.name}")
            thread.join(timeout=5)  # Wait for the thread to exit
            if thread.is_alive():
                print(f"Thread {thread.name} did not exit properly.")
        print("All health beacon threads stopped.")
        list_active_threads()
        # Remove Redis entries for the application_registry
        try:
            redis_host = os.getenv("DATASTORE_REDIS_HOST", "localhost")
            redis_port = int(os.getenv("DATASTORE_REDIS_PORT", 6379))
            redis_db = int(os.getenv("DATASTORE_REDIS_DB", 0))
            redis_password = os.getenv("DATASTORE_REDIS_PASSWORD", None)

            redis_conn = redis.StrictRedis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                password=redis_password,
                decode_responses=True
            )

            datasource_name = datasource_details.get("internal_name", "Unknown")
            profile_key = f"application_registry:{datasource_name}"
            instance_counter = datasource_details.get('current_instance', 0)  # Start with instance counter 1

            # Remove the instance entry
            instance_key = f"{profile_key}:instance_{instance_counter}"
            redis_conn.delete(instance_key)

            # If no more instances exist, remove the profile key
            remaining_instances = redis_conn.keys(f"{profile_key}:instance_*")
            if not remaining_instances:
                redis_conn.delete(profile_key)

            print(f"Removed Redis entries for {instance_key} and {profile_key} (if no instances remain).")
        
        except Exception as e:
            print(f"Error removing Redis entries: {e}")

        print("All resources cleaned up")
    except Exception as e:
        print(f"Error during cleanup: {e}")
atexit.register(cleanup_resources)
# Signal handler
def signal_handler(signum, frame):
    """Handle termination signals."""
    sig_name = signal.Signals(signum).name
    print(f"Received signal {sig_name} in process {os.getpid()}. Cleaning up...")
    cleanup_resources()
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)



def health_beacon(datasource_details):
    """
    Send the IP address, current CPU, memory utilization percentage, and current datasource
    to the Redis datastore.

    Args:
        datasource_details (dict): Details of the currently running datasource.
    """
    # Load Redis connection details from the environment
    redis_host = os.getenv("DATASTORE_REDIS_HOST", "localhost")
    redis_port = int(os.getenv("DATASTORE_REDIS_PORT", 6379))
    redis_db = int(os.getenv("DATASTORE_REDIS_DB", 0))
    redis_password = os.getenv("DATASTORE_REDIS_PASSWORD", None)
    print("health_beacon called with datasource_details")
    # Initialize Redis connection
    redis_conn = redis.StrictRedis(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        password=redis_password,
        decode_responses=True  # Ensures data is returned as strings
    )

    def send_health_data():
        instance_counter = datasource_details.get('current_instance')  # Start with instance counter 1
        while not stop_event.is_set():
            try:
                # Get system metrics
                try:
                    ip_address = socket.gethostbyname(socket.gethostname())
                except socket.gaierror:
                    # print("Failed to resolve hostname. Using 127.0.0.1 as fallback.")
                    ip_address = "127.0.0.1"  # Fallback to localhost

                cpu_utilization = psutil.cpu_percent(interval=1)  # CPU usage percentage
                memory_utilization = psutil.virtual_memory().percent  # Memory usage percentage

                # Prepare Redis keys
                datasource_name = datasource_details.get("internal_name", "Unknown")
                profile_key = f"application_registry:{datasource_name}"
                instance_key = f"{profile_key}:instance_{instance_counter}"

                # Prepare data to store in Redis
                data = {
                    "ip_address": ip_address,
                    "cpu_utilization": cpu_utilization,
                    "memory_utilization": memory_utilization,
                }

                # Store data in Redis
                redis_conn.hset(profile_key, mapping={"datasource_type": datasource_details.get("datasource_type", "Unknown")})
                redis_conn.hset(instance_key, mapping=data)

                # Increment the instance counter for the next instance
                # instance_counter += 1

                # print(f"Health beacon sexnt to Redis: {data}")

            except Exception as e:
                print(f"Error in health beacon: {e}")
                traceback.print_exc()
            # Wait for 1 second before sending the next beacon
            time.sleep(1)

    # Start the health beacon in a separate thread
    health_thread = threading.Thread(target=send_health_data)
    health_thread.daemon = True
    health_thread.start()
    health_threads.append(health_thread) 


if __name__ == "__main__":
    import uvicorn
    mode = os.getenv("APP_MODE", None)
    print('in main')
    if not mode:
        print("Error: APP_MODE environment variable is not set. Please set it to 'feedEnricher', 'campaignEvaluator', or 'eventActuator'.")
        exit(1)

    # Start the application in the specified mode
    

    
    # Define a function to handle SIGTERM
    def handle_sigterm(signum, frame):
        print("Received SIGTERM. Exiting...")
        exit(0)
    
    # Register the signal handler
    signal.signal(signal.SIGTERM, handle_sigterm)
    
    try:
        uvicorn.run(app, host="0.0.0.0", port=8000)
    except KeyboardInterrupt:
        print("KeyboardInterrupt detected. Shutting down...")
        shutdown()  # Call the shutdown logic
        exit(0)  # More graceful than sys.exit()
if __name__ != "__main__":
    import argparse
    print ('in main.py __name__ != "__main__"')
    # Parse arguments when running with uvicorn
    # parser = argparse.ArgumentParser(description="Start the application in a specific mode.")
    # parser.add_argument(
    #     "--mode",
    #     type=str,
    #     required=True,
    #     help="Mode to start the application in. Options: 'feedEnricher', 'campaignEvaluator', 'eventActuator'."
    # )
    # args, _ = parser.parse_known_args()
    # mode= args.mode
    # valid_modes = ["feedEnricher", "campaignEvaluator", "eventActuator"]

    # Read mode from environment variable
    mode = os.getenv("APP_MODE", None)
    if not mode:
        print("Error: APP_MODE environment variable is not set. Please set it to 'feedEnricher', 'campaignEvaluator', or 'eventActuator'.")
        exit(1)
    

    

    # Start the application in the specified mode
    start_application_mode(mode)
    # Define a function to handle SIGTERM
    def handle_sigterm(signum, frame):
        print("Received SIGTERM. Exiting...")
        exit(0)
    
    # Register the signal handler
    signal.signal(signal.SIGTERM, handle_sigterm)
    
    