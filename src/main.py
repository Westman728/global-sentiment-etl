import logging
import yaml
from pathlib import Path
from datetime import datetime
from pymongo import MongoClient

from extractors.reddit_extractor import RedditExtractor
# set other extractors here when implemented
# from extractors.twitter_extractor import TwitterExtractor
# from extractors.news_extractor import NewsExtractor

def setup_logging():
    """Configure logging settings for the app"""
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True) # parents=True creates parent directories if they don't exist

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"logs/extract_{datetime.now().strftime('%Y%m%d')}.log"),
            logging.StreamHandler() # logs to both file and console(stream)
        ]

    )

def load_settings():
    """Load application settings from a YAML file"""
    with open("config/settings.yaml", "r") as file:
        return yaml.safe_load(file)
    
def store_to_mongodb(df, mongo_client, database, collection):
    """Store DF to MongoDB"""
    records = df.to_dict(orient='records') # returns a list of dicts with each dict representing a record in the DF
    
    db = mongo_client[database]
    result = db[collection].insert_many(records)

    return len(result.inserted_ids)

def main():
    """Main execution function"""
    setup_logging()
    logger = logging.getLogger(__name__)

    settings = load_settings()
    
    try:
        mongo_client = MongoClient(settings["mongodb"]["con_string"])
        logger.info("Connected to MongoDB successfully.")
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {str(e)}")
        return
    
    try:
        reddit_extractor = RedditExtractor()
        reddit_settings = settings["reddit"]

        reddit_posts = reddit_extractor.extract_multiple_subreddits(
            subreddit_list=reddit_settings["subreddits"],
            limit=reddit_settings["posts_per_subreddit"],
            sort=reddit_settings["sort"]
        )

        inserted_count = store_to_mongodb(
            reddit_posts,
            mongo_client,
            settings["mongodb"]["database"],
            "raw_reddit_posts"  # collection name in MongoDB
        )

        logger.info(f"Inserted {inserted_count} Reddit posts into MongoDB.")
    except Exception as e:
        logger.error(f"Error extracting or storing Reddit posts: {str(e)}")


        # future extractors goes here <--------------------------

        logger.info("Data extraction and storage completed successfully.")

if __name__ == "__main__":
    main()