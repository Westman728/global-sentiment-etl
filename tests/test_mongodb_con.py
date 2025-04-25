from pymongo import MongoClient
import yaml
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info(f"Testing MongoDB connection using YAML config.")
with open("config/settings.yaml", "r") as file:
    settings = yaml.safe_load(file)
with open("config/credentials.yaml", "r") as cred_file:
    credentials = yaml.safe_load(cred_file)
settings["mongodb"] = credentials["mongodb"]
client = MongoClient(settings["mongodb"]["connection_string"])
logger.info(f"MongoDB connection string: {settings['mongodb']['connection_string']}")
logger.info(f"MongoDB database names: {client.list_database_names()}")
logger.info(f"MongoDB collection count: {client['global_sentiment']['raw_reddit_posts'].count_documents({})}")

