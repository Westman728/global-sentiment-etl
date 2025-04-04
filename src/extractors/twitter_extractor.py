import logging
import tweepy
import yaml
import pandas as pd
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

class TwitterExtractor:
    """A class to extract tweets using Tweepy."""

    def __init__(self, config_path=None):
        """Initialize the TwitterExtractor with Tweepy credentials."""

        self.config_path = config_path or Path("config/credentials.yaml")
        self.client = None
        self.connect()

    def connect(self):
        """Establish a connection to the Twitter API using Tweepy."""
        try:
            with open(self.config_path, 'r') as file:
                credentials = yaml.safe_load(file)["twitter"]

                auth = tweepy.OAuth1UserHandler(
                    credentials["api_key"],
                    credentials["api_secret"],
                    credentials["access_token"],
                    credentials["access_token_secret"],
                )

                self.client = tweepy.API(auth, wait_on_rate_limit=True)
                self.client.verify_credentials()
                logger.info("Connected to Twitter API successfully.")
        except Exception as e:
            logger.error(f"Error connecting to Twitter API: {str(e)}")
            raise
    
    def search_tweets(self, query, count=100, lang="en", include_retweets=False):
        """Search for tweets based o a query.
        
        Args: 
            query (str): The search query.
            count (int): The maximum number of tweets to retrieve.
            lang (str): The language of the tweets to retrieve.
            include_retweets (bool): Whether to include retweets in the results.
            
            Returns:
                pd.DataFrame: A DataFrame containing the retrieved tweets.
        """
        logger.info(f"Searching for tweets with query: {query}, count: {count}, lang: {lang}, include_retweets: {include_retweets}")

        if not include_retweets:
            query = f"{query} -filter:retweets"

        try:
            tweets = tweepy.Cursor(
                self.client.search_tweets,
                q=query,
                lang=lang,
                tweet_mode="extended",
            ).items(count)

            data = []
            for tweet in tweets:
                location = None
                if tweet.user.location:
                    location = tweet.user.location

                if hasattr(tweet, "full_text"):
                    text = tweet.full_text
                else:
                    text = tweet.text

                data.append({
                    "id": tweet.id,
                    "text": text,
                    "created_at": tweet.created_at,
                    "user_name": tweet.user.screen_name,
                    "user_followers": tweet.user.followers_count,
                    "retweet_count": tweet.retweet_count,
                    "favorite_count": tweet.favorite_count,
                    "location": location,
                    "query": query,
                    "source": "twitter",
                })
            return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"Error searching for tweets: {str(e)}")
            raise

    def search_multiple_terms(self, search_terms, count_per_term=100, lang="en", include_retweets=False):
        """Search for tweets based on multiple search terms.
        
        Args:
            search_terms (list): A list of search terms.
            count_per_term (int): The maximum number of tweets to retrieve for each term.
            lang (str): The language of the tweets to retrieve.
            include_retweets (bool): Whether to include retweets in the results.
            
            Returns:
                pd.DataFrame: A DataFrame containing the retrieved tweets for all search terms.
        """
        all_tweets = pd.DataFrame()

        for term in search_terms:
            try:
                tweets = self.search_tweets(term, count_per_term, lang, include_retweets)
                all_tweets = pd.concat([all_tweets, tweets], ignore_index=True)
                logger.info(f"Retrieved {len(tweets)} tweets for term: {term}")
            except Exception as e:
                logger.error(f"Error retrieving tweets for term {term}: {str(e)}")

        return all_tweets
    
    def get_user_timeline(self, username, count=100, include_retwets=False):
        """Get the user timeline for a specific user.
        
        Args:
        username (str): The Twitter username.
        count (int): The maximum number of tweets to retrieve.
        include_retweets (bool): Whether to include retweets in the results.
        
        Returns:
        pd.DataFrame: A DataFrame containing the user's tweets.
        """
        logger.info(f"Getting {count} tweets from user timeline: {username}, include retweets: {include_retweets}")

        try:
            tweets = tweepy.Cursor(
                self.client.user_timeline,
                screen_name=username,
                tweet_mode="extended",
                include_rts=include_retweets,
            ).items(count)

            data = []
            for tweet in tweets:
                if hasattr(tweet, "full_text"):
                    text = tweet.full_text
                else:
                    text = tweet.text
                
                data.append({
                    "id": tweet.id,
                    "text": text,
                    "created_at": tweet.created_at,
                    "user_name": username,
                    "retweet_count": tweet.retweet_count,
                    "favorite_count": tweet.favorite_count,
                    "source": "twitter",
                    "type": "user_timeline",
                })

            return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"Error getting user timeline: {str(e)}")
            raise
    
    def get_trending_topics(self, woeid=1):
        """Get trending topics for a specific location.
        
        Args:
        woeid (int): The WOEID of the location (default is 1 for worldwide).
        
        Returns:
        pd.DataFrame: A DataFrame containing the trending topics.
        """
        logger.info(f"Getting trending topics for WOEID: {woeid}")

        try:
            trends = self.client.get_place_trends(woeid)[0]
            data = []
            for trend in trends["trends"]:
                data.append({
                    "name": trend["name"],
                    "url": trend["url"],
                    "tweet_volume": trend["tweet_volume"],
                    "timestamp": datetime.now(),
                    "woeid": woeid,
                    "source": "twitter",
                    "type": "trending_topic",
                })
            return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"Error getting trending topics: {str(e)}")
            raise

if __name__ == "main":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Tests
    extractor = TwitterExtractor()

    tweets = extractor.search_multiple_terms(
        ["climate change", "politics"],
        count_per_term=5,
    )
    print(f"Retrieved {len(tweets)} tweets")
    print(tweets.head())

    trends = extractor.get_trending_topics()
    print(f"Retrieved {len(trends)} trending topics")
    print(trends.head())