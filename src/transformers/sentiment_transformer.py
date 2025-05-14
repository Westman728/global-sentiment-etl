import logging
import pandas as pd
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from datetime import datetime

nltk.download('vader_lexicon', quiet=True)

logger = logging.getLogger(__name__)

class SentimentTransformer:
    """Transform raw text data into sentiment scores."""
    
    def __init__(self):
        """Initialize the sentiment transformer."""
        self.analyzer = SentimentIntensityAnalyzer()
        
    def analyze_text(self, text):
        """Analyze sentiment of a single text string.
        
        Args:
            text: Text string to analyze
            
        Returns:
            Dictionary with sentiment scores
        """
        if not text or not isinstance(text, str):
            return {
                'compound': 0,
                'pos': 0,
                'neu': 0,
                'neg': 0
            }
            
        return self.analyzer.polarity_scores(text)
    
    def transform_reddit_data(self, reddit_df):
        """Transform Reddit data with sentiment analysis.
        
        Args:
            reddit_df: DataFrame containing Reddit posts
            
        Returns:
            DataFrame with added sentiment columns
        """
        logger.info(f"Transforming {len(reddit_df)} Reddit posts with sentiment analysis")
        
        if reddit_df.empty:
            return reddit_df
            
        result_df = reddit_df.copy() # creates copy instead of modifying original
        
        result_df['title_sentiment'] = result_df['title'].apply(self.analyze_text) #analyzes titles
        
        # components of sentiment
        result_df['title_sentiment_compound'] = result_df['title_sentiment'].apply(lambda x: x['compound'])
        result_df['title_sentiment_positive'] = result_df['title_sentiment'].apply(lambda x: x['pos'])
        result_df['title_sentiment_neutral'] = result_df['title_sentiment'].apply(lambda x: x['neu'])
        result_df['title_sentiment_negative'] = result_df['title_sentiment'].apply(lambda x: x['neg'])
        
        # analyzes text if present
        if 'text' in result_df.columns:
            result_df['text_sentiment'] = result_df['text'].apply(self.analyze_text)
            result_df['text_sentiment_compound'] = result_df['text_sentiment'].apply(lambda x: x['compound'])
        
        # timestamps the alaysis
        result_df['processed_at'] = datetime.now()
        
        logger.info(f"Successfully transformed {len(result_df)} Reddit posts")
        return result_df
    
    def transform_twitter_data(self, twitter_df):
        """Transform Twitter data with sentiment analysis.
        
        Args:
            twitter_df: DataFrame containing tweets
            
        Returns:
            DataFrame with added sentiment columns
        """
        logger.info(f"Transforming {len(twitter_df)} tweets with sentiment analysis")
        
        if twitter_df.empty:
            return twitter_df
            
        result_df = twitter_df.copy()
        
        result_df['text_sentiment'] = result_df['text'].apply(self.analyze_text)
        
        result_df['sentiment_compound'] = result_df['text_sentiment'].apply(lambda x: x['compound'])
        result_df['sentiment_positive'] = result_df['text_sentiment'].apply(lambda x: x['pos'])
        result_df['sentiment_neutral'] = result_df['text_sentiment'].apply(lambda x: x['neu'])
        result_df['sentiment_negative'] = result_df['text_sentiment'].apply(lambda x: x['neg'])
        
        result_df['processed_at'] = datetime.now()
        
        logger.info(f"Successfully transformed {len(result_df)} tweets")
        return result_df
    
    def transform_news_data(self, news_df):
        """Transform news headline data with sentiment analysis.
        
        Args:
            news_df: DataFrame containing news headlines
            
        Returns:
            DataFrame with added sentiment columns
        """
        logger.info(f"Transforming {len(news_df)} news headlines with sentiment analysis")
        
        if news_df.empty:
            return news_df
            
        result_df = news_df.copy()
        
        result_df['title_sentiment'] = result_df['title'].apply(self.analyze_text)
        
        result_df['sentiment_compound'] = result_df['title_sentiment'].apply(lambda x: x['compound'])
        result_df['sentiment_positive'] = result_df['title_sentiment'].apply(lambda x: x['pos'])
        result_df['sentiment_neutral'] = result_df['title_sentiment'].apply(lambda x: x['neu'])
        result_df['sentiment_negative'] = result_df['title_sentiment'].apply(lambda x: x['neg'])
        
        result_df['processed_at'] = datetime.now()
        
        logger.info(f"Successfully transformed {len(result_df)} news headlines")
        return result_df
    
    def transform_all_sources(self, reddit_df, twitter_df, news_df):
        """Transform data from all sources and create a unified sentiment dataset.
        
        Args:
            reddit_df: DataFrame containing Reddit posts
            twitter_df: DataFrame containing tweets
            news_df: DataFrame containing news headlines
            
        Returns:
            DataFrame with unified sentiment data
        """
        
        logger.info("Creating unified sentiment dataset from all sources")

        
        if not reddit_df.empty:
            reddit_transformed = self.transform_reddit_data(reddit_df)
        else:
            reddit_transformed = pd.DataFrame()
        if not twitter_df.empty:
            twitter_transformed = self.transform_twitter_data(twitter_df)
        else:
            twitter_transformed = pd.DataFrame()
        if not news_df.empty:
            news_transformed = self.transform_news_data(news_df)
        else:
            news_transformed = pd.DataFrame()
        
        unified_data = []
        
        # adding the Reddit data
        if not reddit_transformed.empty:
            for _, row in reddit_transformed.iterrows():
                unified_data.append({
                    'source': 'reddit',
                    'source_id': row.get('url', '') or row.get('title', ''),
                    'text': row.get('title', ''),  # set title as primary text
                    'created_at': row.get('created_utc', datetime.now()),
                    'sentiment_compound': row.get('title_sentiment_compound', 0),
                    'sentiment_positive': row.get('title_sentiment_positive', 0),
                    'sentiment_neutral': row.get('title_sentiment_neutral', 0),
                    'sentiment_negative': row.get('title_sentiment_negative', 0),
                    'processed_at': row.get('processed_at', datetime.now())
                })
        
        # adding Twitter data
        if not twitter_transformed.empty:
            for _, row in twitter_transformed.iterrows():
                unified_data.append({
                    'source': 'twitter',
                    'source_id': str(row.get('id', '')),
                    'text': row.get('text', ''),
                    'created_at': row.get('created_at', datetime.now()),
                    'sentiment_compound': row.get('sentiment_compound', 0),
                    'sentiment_positive': row.get('sentiment_positive', 0),
                    'sentiment_neutral': row.get('sentiment_neutral', 0),
                    'sentiment_negative': row.get('sentiment_negative', 0),
                    'processed_at': row.get('processed_at', datetime.now())
                })
        
        news_counter = 1
        # adding News data
        if not news_transformed.empty:
            for _, row in news_transformed.iterrows():
                unified_data.append({
                    'source': 'news',
                    'source_id': f"news_{news_counter}",  # URL as source ID
                    'text': row.get('title', ''),
                    'created_at': row.get('extracted_at', datetime.now()),
                    'sentiment_compound': row.get('sentiment_compound', 0),
                    'sentiment_positive': row.get('sentiment_positive', 0),
                    'sentiment_neutral': row.get('sentiment_neutral', 0),
                    'sentiment_negative': row.get('sentiment_negative', 0),
                    'processed_at': row.get('processed_at', datetime.now())
                })
                news_counter += 1
        
        unified_df = pd.DataFrame(unified_data)
        logger.info(f"Created unified sentiment dataset with {len(unified_df)} records")
        return unified_df

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # MOCK TESTS V----------------------------------------------------------------
    transformer = SentimentTransformer()
    
    # SINGLE TEST
    sample_text = "This is a great day! I'm very happy with the results."
    sentiment = transformer.analyze_text(sample_text)
    print(f"Sample text sentiment: {sentiment}")
    
    # DF TESTS
    reddit_sample = pd.DataFrame({
        'id': ['123', '456'],
        'title': ['Great news about the economy', 'Terrible weather today'],
        'text': ['The economy is improving faster than expected', 'Rain and storms all week'],
        'created_utc': [datetime.now(), datetime.now()]
    })
    
    twitter_sample = pd.DataFrame({
        'id': [789, 101],
        'text': ['I love this new product!', 'Service was disappointing'],
        'created_at': [datetime.now(), datetime.now()]
    })
    
    news_sample = pd.DataFrame({
        'title': ['Stock market hits record high', 'Inflation concerns grow'],
        'url': ['http://example.com/1', 'http://example.com/2'],
        'extracted_at': [datetime.now(), datetime.now()]
    })
    
    reddit_result = transformer.transform_reddit_data(reddit_sample)
    twitter_result = transformer.transform_twitter_data(twitter_sample)
    news_result = transformer.transform_news_data(news_sample)
    
    unified = transformer.transform_all_sources(reddit_sample, twitter_sample, news_sample)
    
    print(f"Unified dataset shape: {unified.shape}")
    print(f"Positive: {unified['sentiment_positive'].mean()}")
    print(f"Neutral: {unified['sentiment_neutral'].mean()}")
    print(f"Negative: {unified['sentiment_negative'].mean()}")
    print(unified.head())