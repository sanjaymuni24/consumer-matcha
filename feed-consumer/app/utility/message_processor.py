import json
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MessageProcessor:
    def __init__(self, schema):
        """
        Initialize the message processor with a schema.
        
        Args:
            schema (dict): The schema to validate and enrich messages.
        """
        self.schema = schema

    def validate_message(self, message):
        """
        Validate the message against the schema.
        
        Args:
            message (dict): The message to validate.
        
        Returns:
            bool: True if the message is valid, False otherwise.
        """
        for field, field_type in self.schema.items():
            if field not in message:
                logger.warning(f"Missing field '{field}' in message")
                return False
            if not isinstance(message[field], field_type):
                logger.warning(f"Field '{field}' has invalid type. Expected {field_type}, got {type(message[field])}")
                return False
        return True

    def enrich_message(self, message):
        """
        Enrich the message with additional computed fields.
        
        Args:
            message (dict): The message to enrich.
        
        Returns:
            dict: The enriched message.
        """
        enriched_message = message.copy()
        
        # Example enrichment: Add a processing timestamp
        enriched_message['processing_timestamp'] = datetime.utcnow().isoformat()
        
        # Example enrichment: Compute a derived field
        if 'amount' in message and 'tax_rate' in message:
            enriched_message['tax'] = message['amount'] * message['tax_rate']
        
        return enriched_message

    def process_message(self, raw_message):
        """
        Process a raw message: parse, validate, enrich, and return the result.
        
        Args:
            raw_message (str): The raw message as a JSON string.
        
        Returns:
            dict: The processed message, or None if the message is invalid.
        """
        try:
            # Parse the raw message
            message = json.loads(raw_message)
            logger.info(f"Parsed message: {message}")
            
            # Validate the message
            if not self.validate_message(message):
                logger.error("Message validation failed")
                return None
            
            # Enrich the message
            enriched_message = self.enrich_message(message)
            logger.info(f"Enriched message: {enriched_message}")
            
            return enriched_message
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during message processing: {e}")
            return None

# Example usage
if __name__ == "__main__":
    # Define a sample schema
    schema = {
        "transaction_id": str,
        "amount": (int, float),
        "tax_rate": float,
        "transaction_time": str
    }
    
    # Initialize the message processor
    processor = MessageProcessor(schema)
    
    # Example raw message
    raw_message = json.dumps({
        "transaction_id": "12345",
        "amount": 100.0,
        "tax_rate": 0.05,
        "transaction_time": "2025-07-18T12:34:56"
    })
    
    # Process the message
    processed_message = processor.process_message(raw_message)
    if processed_message:
        print("Processed Message:", processed_message)