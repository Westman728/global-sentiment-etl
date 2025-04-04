from pymongo import MongoClient
import logging
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with open("../config/settings.yaml", "r") as file:
    settings = yaml.safe_load(file)

try:
    client = MongoClient(settings["mongodb"]["connection_string"])
    

    db = client[settings["mongodb"]["database"]]
    
    result = db.test_collection.insert_one({"test": "Connection with specific user successful!"})
    logger.info(f"Inserted document with ID: {result.inserted_id}")
    
    logger.info("MongoDB connection test successful!")
    
except Exception as e:
    logger.error(f"Error connecting to MongoDB: {str(e)}")