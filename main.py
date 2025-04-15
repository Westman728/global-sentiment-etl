import logging
import yaml
from pathlib import Path
from datetime import datetime
from pymongo import MongoClient
import pandas as pd

from src.extractors.reddit_extractor import RedditExtractor
from src.extractors.twitter_extractor import TwitterExtractorMinimal
from src.extractors.news_extractor import NewsExtractor
from src.transformers.sentiment_transformer import SentimentTransformer

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
    logger.info("Start of script ---------------------")
    try:
        mongo_client = MongoClient(settings["mongodb"]["connection_string"])
        logger.info("Connected to MongoDB successfully.")
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {str(e)}")
        return
    
    # reddit extraction
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

    # twitter extraction
    try:
        twitter_extractor = TwitterExtractorMinimal()
        twitter_settings = settings["twitter"]
        tweets = pd.DataFrame(columns=[
            'id', 'text', 'created_at', 'user_id', 'user_name', 'user_followers',
            'retweet_count', 'reply_count', 'like_count', 'quote_count',
            'location', 'keyword', 'source'
        ])

        # skip if tweets_per_term = 0
        if twitter_settings["tweets_per_term"] == 0:
            logger.info("Skipping Twitter extraction (tweets_per_term = 0).")

        elif twitter_settings["search_terms"]:
            primary_term = twitter_settings["search_terms"][0]
            logger.info(f"Using primary term: {primary_term}")

            tweets = twitter_extractor.fetch_tweets_for_keyword(
                keyword = primary_term,
                max_results = min(twitter_settings["tweets_per_term"], 100),
                include_retweets = twitter_settings["include_retweets"]
            )

            inserted_count = store_to_mongodb(
                tweets,
                mongo_client,
                settings["mongodb"]["database"],
                "raw_twitter_posts"
            )

            logger.info(f"Inserted {inserted_count} for {primary_term} Twitter posts into MongoDB.")

            if len(twitter_settings["search_terms"]) > 1:
                skipped_terms = twitter_settings["search_terms"][1:]
                logger.info(f"Skipping terms: {skipped_terms}")
        else:
            logger.warning("No search terms provided. Skipping Twitter extraction.")

    except Exception as e:
        logger.error(f"Error extracting or storing Twitter posts: {str(e)}")
        tweets = pd.DataFrame([
            'id', 'text', 'created_at', 'user_id', 'user_name', 'user_followers',
            'retweet_count', 'reply_count', 'like_count', 'quote_count',
            'location', 'keyword', 'source'
        ])
    
    # news extraction
    try:
        news_extractor = NewsExtractor()
        news_settings = settings["news"]

        categories = news_settings.get("categories", None)
        if categories is None:
            raise Exception("Failed to fetch news categories from settings.")

        headlines = news_extractor.extract_from_all_sources(
            categories=categories,
            limit_per_source=news_settings["articles_per_source"]
        )
        # remove duplicates and assign back to headlines
        headlines_deduped = headlines.drop_duplicates(subset=["title", "url"], keep="first")
        logger.info(f"Removed {len(headlines) - len(headlines_deduped)} duplicate news articles.")
        headlines = headlines_deduped
        inserted_count = store_to_mongodb(
            headlines,
            mongo_client,
            settings["mongodb"]["database"],
            "raw_news_articles"
        )
        logger.info(f"Inserted {inserted_count} news articles into MongoDB.")
    except Exception as e:
        logger.error(f"Error extracting or storing news articles: {str(e)}")

        logger.info("Data extraction and storage completed successfully.")
    
    # Sentiment Analysis
    try:
        logger.info("Starting sentiment analysis...")
        sentiment_transformer = SentimentTransformer()

        logger.info(f"Reddit data type: {type(reddit_posts)}")
        logger.info(f"Reddit data type: {type(tweets)}")
        logger.info(f"Reddit data type: {type(headlines)}")

        if 'tweets' in locals():
            logger.info(f"Twitter data empty? {tweets.empty if hasattr(tweets, 'empty') else 'Not a DataFrame'}")

        try:
            if ("tweets" in locals() and not tweets.empty):
                logger.info("TRANSFORMING TWITTER DATA...")
                twitter_result = sentiment_transformer.transform_twitter_data(tweets)
                logger.info("TWITTER TRANSFORM SUCCESSFUL")
        except Exception as e:
            logger.error(f"ERROR IN TWITTER TRANSFORMATION: {str(e)}")

        reddit_posts = reddit_posts if "reddit_posts" in locals() else pd.DataFrame()
        if not ("tweets" in locals() and tweets is not None):
            tweets = pd.DataFrame(columns=[
                'id', 'text', 'created_at', 'user_id', 'user_name', 'user_followers',
                'retweet_count', 'reply_count', 'like_count', 'quote_count',
                'location', 'keyword', 'source'
            ])
        # if tweets is None:
        #     tweets = pd.DataFrame([{
        #         "id": "test123",
        #     "text": "Test tweet for dry run.",
        #     "created_at": datetime.now(),
        #     "user_id": "u1",
        #     "user_name": "testuser",
        #     "user_followers": 0,
        #     "retweet_count": 0,
        #     "reply_count": 0,
        #     "like_count": 0,
        #     "quote_count": 0,
        #     "location": "Internet",
        #     "keyword": "test",
        #     "source": "twitter"
        # }])
        headlines = headlines if "headlines" in locals() else pd.DataFrame()
        
        logger.info(f"Reddit post length: {len(reddit_posts)}!!! Tweets length: {len(tweets)}, Type: {type(tweets)}!!! Headlines: {len(headlines)}, Type: {type(headlines)}")
        # logger.info(f"Tweets value: {tweets.values}")
        

        if reddit_posts.empty:
            raise Exception("Reddit posts, tweets or headlines are empty. Skipping transformation.")
        # elif tweets.empty:
        #     raise Exception("Tweets are empty. Stopping.")
        elif headlines.empty:
            raise Exception("Headlines are empty. Stopping.")
        else:
            unified_sentiment_data = sentiment_transformer.transform_all_sources(
                reddit_posts,
                tweets,
                headlines
            )

        if not unified_sentiment_data.empty:
            sentiment_count = store_to_mongodb(
                unified_sentiment_data,
                mongo_client,
                settings["mongodb"]["database"],
                "sentiment_analysis" # collection name in MongoDB
            )

            logger.info(f"Inserted {sentiment_count} sentiment analysis results into MongoDB.")
        else:
            logger.warning("No sentiment analysis data to store.")

        logger.info(f"Data extraction, transformation, and storage completed successfully.")

    except Exception as e:
        logger.error(f"Error performing sentiment analysis: {str(e)}")
    
    logger.info("End of script ---------------------")

if __name__ == "__main__":
    main()