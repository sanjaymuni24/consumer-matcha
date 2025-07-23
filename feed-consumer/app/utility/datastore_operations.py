import os
import redis
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
for key, value in os.environ.items():
    if key.startswith("DATASTORE_REDIS") or key.startswith("AGGREGATE_REDIS"):
        print(f"{key}: {value}")



def get_datastore_redis_connection():
    """
    Establish a connection to the Redis datastore using details from the .env file.
    
    Returns:
        redis.StrictRedis: Redis connection object.
    """
    try:
        redis_host = os.getenv("DATASTORE_REDIS_HOST", "localhost")
        redis_port = int(os.getenv("DATASTORE_REDIS_PORT", 6379))
        redis_db = int(os.getenv("DATASTORE_REDIS_DB", 0))
        redis_password = os.getenv("DATASTORE_REDIS_PASSWORD", None)

        # Create Redis connection
        connection = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password,
            decode_responses=True  # Ensures data is returned as strings
        )
        return connection
    except Exception as e:
        raise ConnectionError(f"Failed to connect to Redis: {e}")
def get_aggregate_redis_connection():
    """
    Establish a connection to the Redis aggregate datastore using details from the .env file.
    
    Returns:
        redis.StrictRedis: Redis connection object.
    """
    try:
        redis_host = os.getenv("AGGREGATE_REDIS_HOST", "localhost")
        redis_port = int(os.getenv("AGGREGATE_REDIS_PORT", 6380))
        redis_db = int(os.getenv("AGGREGATE_REDIS_DB", 0))
        redis_password = os.getenv("AGGREGATE_REDIS_PASSWORD", None)

        # Create Redis connection
        connection = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password,
            decode_responses=True  # Ensures data is returned as strings
        )
        return connection
    except Exception as e:
        raise ConnectionError(f"Failed to connect to Redis: {e}")
def get_redis_datastore_full_data(redis_conn, datastore_internal_name, key):
    return redis_conn.hgetall(f"{datastore_internal_name}:{key}")

def get_redis_data(redis_conn,key):
    """
    Retrieve data from Redis for the given key. If the key is not present, return None.
    
    Args:
        key (str): The key to retrieve data for.
    
    Returns:
        str or None: The value associated with the key, or None if the key does not exist.
    """
    try:
        # Get Redis connection
        # redis_conn = get_redis_connection()
        # Fetch the value for the given key
        value = redis_conn.hgetall(key)
        if value is None:
            return None
        return value
    except Exception as e:
        raise RuntimeError(f"Error retrieving data from Redis for key '{key}': {e}")

# Example usage
if __name__ == "__main__":
    # Example key to fetch
    key = "example_key"
    value = get_redis_data(key)
    if value is None:
        print(f"No value found for key: {key}")
    else:
        print(f"Value for key '{key}': {value}")