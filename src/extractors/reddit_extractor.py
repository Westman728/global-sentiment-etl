import logging
import praw
import yaml
import pandas as pd
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

class RedditExtractor:
    """A class to extract Reddit posts and comments using PRAW."""

    def __init__(self, config_path=None):
        """Initialize the RedditExtractor with PRAW credentials."""

        self.config_path = config_path or Path("config/credentials.yaml")
        self.client = None
        self.connect()

    def connect(self):
        """Esatablish a connection to the Reddit API using PRAW."""
        try:
            with open(self.config_path, 'r') as file:   # r for read mode, with ensures file is closed after reading
                credential = yaml.safe_load(file)

                self.client = praw.Reddit(
                    client_id=credential["client_id"],
                    client_secret=credential["client_secret"],
                    user_agent=credential["user_agent"],
                    username=credential.get("username", ""),    # tuple for conditional access to username/password
                    password=credential.get("password", ""),
                )
                logger.info("Connected to Reddit API successfully.")
        except Exception as e:
            logger.error(f"Error connecting to Reddit API: {str(e)}")
            raise
    
    def extract_subreddit_posts(self, subreddit_name, limit=100, sort="hot"):
        """Extract posts from a specific subreddit
        
        Args:
            subreddit_name (str): The name of the subreddit to extract posts from.
            limit (int): The maximum number of posts to extract.
            sort (str): The sorting method for the posts (e.g., 'hot', 'new', 'top').
            
            Returns:
                pd.DataFrame: A DataFrame containing the extracted posts.
        """
        logger.info(f"Extracting {limit} posts from subreddit: {subreddit_name}, sorted by: {sort}")
        subreddit = self.client.subreddit(subreddit_name)

        if sort == "hot":
            posts = subreddit.hot(limit=limit)
        elif sort == "new":
            posts = subreddit.new(limit=limit)
        elif sort == "top":
            posts = subreddit.top(limit=limit)
        elif sort == "controversial":
            posts = subreddit.controversial(limit=limit)
        else:
            raise ValueError("Invalid sort method. Choose from 'hot', 'new', 'top', or 'controversial'.")
        
        data = []
        for post in posts:
            data.append({
                "id": post.id,
                "title": post.title,
                "text": post.selftext,
                "created_utc": datetime.fromtimestamp(post.created_utc),
                "score": post.score,
                "num_comments": post.num_comments,
                "url": post.url,
                "author": str(post.author),
                "subreddit": subreddit_name,
                "source": "reddit",
            })
        return pd.DataFrame(data)
    
    def extract_multiple_subreddits(self, subreddit_list, limit=100, sort="hot"):
        """Extract posts from multiple subreddits
        
        Args:
            subreddit_list (list): A list of subreddit names to extract posts from.
            limit (int): The maximum number of posts to extract from each subreddit.
            sort (str): The sorting method for the posts (e.g., 'hot', 'new', 'top').
            
            Returns:
                pd.DataFrame: A DataFrame containing the extracted posts from all subreddits.
        """
        all_posts = pd.DataFrame()

        for subreddit_name in subreddit_list:
            try:
                posts = self.extract_subreddit_posts(subreddit_name, limit, sort)
                all_posts = pd.concat([all_posts, posts], ignore_index=True)
            except Exception as e:
                logger.error(f"Error extracting posts from subreddit {subreddit_name}: {str(e)}")

        return all_posts
    
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )

    extractor = RedditExtractor()
    posts = extractor.extract_multiple_subreddits(
        ["worldnews", "news", "politics"],
        limit=10
    )
    print(f"Extracted {len(posts)} posts from multiple subreddits.")
    print(posts.head())