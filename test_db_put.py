import logging
import yaml
from datetime import datetime
from pymongo import MongoClient
import pandas as pd

def setup_logging():
    """Configure basic logging for the test script"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    return logging.getLogger(__name__)

def load_settings():
    """Load application settings from the YAML file"""
    with open("config/settings.yaml", "r") as file:
        return yaml.safe_load(file)

def store_to_mongodb(df, mongo_client, database, collection):
    """Store DataFrame to MongoDB - same as in main.py"""
    records = df.to_dict(orient='records')
    
    db = mongo_client[database]
    result = db[collection].insert_many(records)

    return len(result.inserted_ids)

def main():
    """Test MongoDB connection and insertion"""
    logger = setup_logging()
    logger.info("Starting MongoDB test script")

    # Load settings
    settings = load_settings()
    
    # Print connection details for debugging
    logger.info(f"Connecting to: {settings['mongodb']['connection_string']}")
    logger.info(f"Database name: {settings['mongodb']['database']}")
    
    # Connect to MongoDB
    try:
        mongo_client = MongoClient("mongodb://host.docker.internal:27017/")
        logger.info("Connected to MongoDB successfully")
        logger.info(f"MongoDB client info: {mongo_client}")
        logger.info(f"Mongodb address info: {mongo_client.address}")
        
        # List all databases to verify connection
        dbs = mongo_client.list_database_names()
        logger.info(f"Available databases: {dbs}")
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {str(e)}")
        return
    
    # Create a small test DataFrame
    test_data = pd.DataFrame([
        {"test_id": 1, "message": "Test message 1", "timestamp": datetime.now()},
        {"test_id": 2, "message": "Test message 2", "timestamp": datetime.now()},
        {"test_id": 3, "message": "Test message 3", "timestamp": datetime.now()}
    ])
    
    # Insert test data
    try:
        inserted_count = store_to_mongodb(
            test_data,
            mongo_client,
            settings["mongodb"]["database"],
            "test_collection"
        )
        logger.info(f"Inserted {inserted_count} test documents into MongoDB")
        
        # Verify the insertion by querying the collection
        db = mongo_client[settings["mongodb"]["database"]]
        count = db["test_collection"].count_documents({})
        logger.info(f"Total documents in test_collection: {count}")
        
        # Retrieve and print the first document
        first_doc = db["test_collection"].find_one()
        logger.info(f"First document: {first_doc}")
        
    except Exception as e:
        logger.error(f"Error inserting or retrieving test data: {str(e)}")

if __name__ == "__main__":
    main()