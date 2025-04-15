import logging
import yaml
import pandas as pd
from datetime import datetime
from pathlib import Path
import tweepy
import time

logger = logging.getLogger(__name__)

class TwitterExtractorMinimal:
    """A minimal Twitter data extractor designed for the strict limitations of the free tier.
    Only fetches the most recent tweets for a keyword in a single run (limited to 100 posts/month).
    """

    def __init__(self, config_path=None):
        """Initialize the TwitterExtractor with Tweepy credentials."""
        self.config_path = config_path or Path("config/credentials.yaml")
        self.client = None
        self.connect()

    def connect(self):
        """Establish a connection to the Twitter API v2 using Tweepy."""
        try:
            with open(self.config_path, 'r') as file:
                credentials = yaml.safe_load(file)["twitter"]

            self.client = tweepy.Client(
                bearer_token=credentials.get("bearer_token", None),
                consumer_key=credentials.get("api_key"),
                consumer_secret=credentials.get("api_secret"),
                access_token=credentials.get("access_token"),
                access_token_secret=credentials.get("access_token_secret"),
                wait_on_rate_limit=True
            )
            
            self.client.get_me()
            logger.info("Connected to Twitter API v2 successfully.")
        except Exception as e:
            logger.error(f"Error connecting to Twitter API v2: {str(e)}")
            raise

    def fetch_tweets_for_keyword(self, keyword, max_results=100, include_retweets=False):
        """Fetch the most recent tweets for a specific keyword.
        
        Args:
            keyword (str): The keyword to search for.
            max_results (int): Maximum number of tweets to retrieve (max 100).
            include_retweets (bool): Whether to include retweets.
            
        Returns:
            pd.DataFrame: DataFrame containing the collected tweets.
        """
        logger.info(f"Fetching up to {max_results} tweets for keyword: '{keyword}'")

        if max_results == 0:
            logger.info(f"Max_results == 0, returning empty dataframe for keyword: {keyword}")
            return pd.DataFrame({
                'id':[], 'text':[], 'created_at':[], 'user_id':[], 'user_name':[], 'user_followers':[],
                'retweet_count':[], 'reply_count':[], 'like_count':[], 'quote_count':[],
                'location':[], 'keyword':[], 'source':[]
            })
        elif max_results > 0 and max_results < 10:
            logger.warning("Max results must be at least 10. Defaulting to 10.")
            max_results = 10
        elif max_results > 100:
            logger.warning("Max results cannot exceed 100. Defaulting to 100.")
            max_results = 100
        
        query = keyword
        if not include_retweets:
            query = f"{keyword} -is:retweet"
        
        try:
            tweet_fields = ["created_at", "public_metrics", "author_id", "source"]
            user_fields = ["username", "name", "public_metrics", "location"]
            
            response = self.client.search_recent_tweets(
                query=query,
                max_results=min(max_results, 100),  # 100 max per request (limited to 100 posts/month)
                tweet_fields=tweet_fields,
                user_fields=user_fields,
                expansions=["author_id"]
            )
            
            if not response.data:
                logger.warning(f"No tweets found for keyword: {keyword}")
                return pd.DataFrame(columns=[
                    'id', 'text', 'created_at', 'user_id', 'user_name', 'user_followers',
                    'retweet_count', 'reply_count', 'like_count', 'quote_count',
                    'location', 'keyword', 'source'
                ])
            

            users = {user.id: user for user in response.includes["users"]} if "users" in response.includes else {}
            

            data = []
            for tweet in response.data:
                user = users.get(tweet.author_id)
                
                data.append({
                    "id": tweet.id,
                    "text": tweet.text,
                    "created_at": tweet.created_at,
                    "user_id": tweet.author_id,
                    "user_name": user.username if user else None,
                    "user_followers": user.public_metrics["followers_count"] if user else None,
                    "retweet_count": tweet.public_metrics["retweet_count"],
                    "reply_count": tweet.public_metrics["reply_count"],
                    "like_count": tweet.public_metrics["like_count"],
                    "quote_count": tweet.public_metrics["quote_count"],
                    "location": user.location if user and hasattr(user, "location") else None,
                    "keyword": keyword,
                    "source": "twitter"
                })
            
            logger.info(f"Successfully retrieved {len(data)} tweets for keyword: '{keyword}'")
            return pd.DataFrame(data)
        
        except Exception as e:
            logger.error(f"Error fetching tweets for keyword '{keyword}': {str(e)}")
            raise

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )

    try:
        extractor = TwitterExtractorMinimal()
        
        keyword = "politics"
        tweets = extractor.fetch_tweets_for_keyword(
            keyword=keyword,
            max_results=10 
        )
        
        print(f"Extracted {len(tweets)} tweets for keyword '{keyword}'.")
        
        if not tweets.empty:
            print("\nSample tweet information:")
            print(f"Tweet text: {tweets.iloc[0]['text'][:100]}...")
            print(f"Author: @{tweets.iloc[0]['user_name']}")
            print(f"Created at: {tweets.iloc[0]['created_at']}")
            print(f"Likes: {tweets.iloc[0]['like_count']}")
            
            print(f"\nAvailable data columns: {', '.join(tweets.columns.tolist())}")
    except Exception as e:
        print(f"Error demonstrating Twitter extractor: {str(e)}")
        print("Note: This module is primarily meant to be imported, not run directly.")
