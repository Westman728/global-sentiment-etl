from pymongo import MongoClient
import yaml
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with open("../config/settings.yaml", "r") as file:
    settings = yaml.safe_load(file)

try:
    client = MongoClient(settings["mongodb"]["connection_string"])
    db = client[settings["mongodb"]["database"]]

    collections = db.list_collection_names()
    logger.info(f"Available collections: {collections}")

    reddit_count = db.raw_reddit_posts.count_documents({}) # count documents in collection
    twitter_count = db.raw_twitter_posts.count_documents({})
    news_count = db.raw_news_posts.count_documents({})

    logger.info(f"Raw data count: Reddit: {reddit_count}, Twitter: {twitter_count}, News: {news_count}")

    if "sentiment_analysis" in collections:
        sentiment_count = db.sentiment_analysis.count_documents({})
        logger.info(f"Sentiment analysis count: {sentiment_count}")

    logger.info("Average sentiment by source:")
    pipeline = [
        {
            "$group": {
                "_id": "$source",
                "average_sentiment": {"$avg": "$sentiment_compound"},
                "count": {"$sum": 1}
            }
        }
    ]
    results = list(db.sentiment_analysis.aggregate(pipeline))
    for result in results:
        logger.info(f"{result['_id']}: {result['average_sentiment']} (from {result['count']} records)")

    else:
        logger.warning("Sentiment analysis collection not found.")
except Exception as e:
    logger.error(f"Error connecting to MongoDB: {str(e)} or querying data")