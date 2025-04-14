import logging
from src.extractors.reddit_extractor import RedditExtractor

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def test_reddit_extractor():
    try:
        reddit_extractor = RedditExtractor()
        
        subreddit = "news"
        logger.info(f"Extracting from r/{subreddit}...")
        
        posts = reddit_extractor.extract_subreddit_posts(
            subreddit_name=subreddit,
            limit=5,
            sort="hot"
        )
        
        if posts is not None and not posts.empty:
            logger.info(f"Successfully extracted {len(posts)} posts from r/{subreddit}")
            logger.info(f"Columns: {posts.columns.tolist()}")
            logger.info(f"Column datatypes: {posts.dtypes}")
            logger.info(f"Sample post title: {posts.iloc[0]['title']}")
            return True
        else:
            logger.error(f"No posts returned from r/{subreddit}")
            return False
            
    except Exception as e:
        logger.error(f"Error testing Reddit extractor: {str(e)}")
        if hasattr(e, 'response') and hasattr(e.response, 'text'):
            logger.error(f"Response text: {e.response.text}")
        return False

if __name__ == "__main__":
    logger.info("Starting Reddit extractor test...")
    result = test_reddit_extractor()
    logger.info(f"Test result: {'PASS' if result else 'FAIL'}")