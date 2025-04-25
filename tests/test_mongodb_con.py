from pymongo import MongoClient
import yaml
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info(f"Testing MongoDB connection using hardcoded values.")
client = MongoClient("mongodb://localhost:27018")
logger.info(f"MongoDB database names: {client.list_database_names()}")
logger.info(f"MongoDB collection count: {client["global_sentiment"]["raw_reddit_posts"].count_documents({})}")
logger.info(f"First document in collection: {client["global_sentiment"]["raw_reddit_posts"].find_one({})}")

logger.info(f"Testing MongoDB connection using YAML config.")
with open("config/settings.yaml", "r") as file:
    config = yaml.safe_load(file)
    client = MongoClient(config["mongodb"]["connection_string"])
    logger.info(f"MongoDB connection string: {config['mongodb']['connection_string']}")
    logger.info(f"MongoDB database names: {client.list_database_names()}")
    logger.info(f"MongoDB collection count: {client['global_sentiment']['raw_reddit_posts'].count_documents({})}")

