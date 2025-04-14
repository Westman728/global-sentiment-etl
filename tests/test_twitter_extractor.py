import logging
from pathlib import Path
import sys

from src.extractors.twitter_extractor import TwitterExtractorMinimal


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def test_twitter_extractor_minimal():
    try:

        twitter_extractor = TwitterExtractorMinimal()
        

        search_term = "trump"
        logger.info(f"Fetching a single tweet with term: '{search_term}'...")
        
        tweets = twitter_extractor.fetch_tweets_for_keyword(
            keyword=search_term,
            max_results=10,  
            include_retweets=False
        )
        
        if tweets is not None and not tweets.empty:
            logger.info(f"Successfully extracted {len(tweets)} tweet for '{search_term}'")
            logger.info(f"Columns: {tweets.columns.tolist()}")
            logger.info(f"Columns datatypes: {tweets.dtypes}")
            logger.info(f"Tweet text: {tweets.iloc[0]['text']}")
            logger.info(f"Tweet author: @{tweets.iloc[0]['user_name']}")
            logger.info(f"Tweet date: {tweets.iloc[0]['created_at']}")
            logger.info("API connectivity test: PASS")
            return True
        else:
            logger.error(f"No tweets returned for search term '{search_term}'")
            logger.info("API connectivity test: FAIL")
            return False
            
    except Exception as e:
        logger.error(f"Error testing Twitter extractor: {str(e)}")
        logger.error("API connectivity test: FAIL")
        return False

if __name__ == "__main__":
    logger.info("Starting Twitter minimal extractor test...")
    result = test_twitter_extractor_minimal()
    logger.info(f"Overall test result: {'PASS' if result else 'FAIL'}")