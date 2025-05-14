# twitter_transform_with_topics.py
import logging
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import numpy as np
from src.transformers.sentiment_transformer import SentimentTransformer
from src.transformers.topic_transformer import TopicModeler

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Connect to MongoDB
mongo_client = MongoClient('mongodb://localhost:27018/')
db = mongo_client['global_sentiment']  # Replace with your actual database name

def main():
    logger.info("Starting Twitter data transformation...")
    
    # 1. Get raw Twitter data
    raw_tweets = list(db.raw_twitter_posts.find())
    if not raw_tweets:
        logger.info("No Twitter data found in raw_twitter_posts collection.")
        return
    
    logger.info(f"Found {len(raw_tweets)} raw Twitter posts.")
    
    # 2. Convert to DataFrame
    tweets_df = pd.DataFrame(raw_tweets)
    
    # 3. Transform with sentiment
    transformer = SentimentTransformer()
    twitter_transformed = transformer.transform_twitter_data(tweets_df)
    
    # 4. Create unified format
    twitter_counter = 1
    unified_data = []
    
    for _, row in twitter_transformed.iterrows():
        unified_data.append({
            'source': 'twitter',
            'source_id': f"twitter_{twitter_counter}",
            'text': row.get('text', ''),
            'created_at': row.get('created_at', datetime.now()),
            'sentiment_compound': row.get('sentiment_compound', 0),
            'sentiment_positive': row.get('sentiment_positive', 0),
            'sentiment_neutral': row.get('sentiment_neutral', 0),
            'sentiment_negative': row.get('sentiment_negative', 0),
            'processed_at': row.get('processed_at', datetime.now()),
            # Default topic values to be updated later
            'topic_id': -1,
            'topic_confidence': 0.0,
            'topic_keywords': ""
        })
        twitter_counter += 1
    
    # 5. Get all existing texts for topic modeling
    existing_data = list(db.sentiment_analysis.find({}, {'text': 1}))
    existing_texts = [doc['text'] for doc in existing_data if 'text' in doc]
    logger.info(f"Collected {len(existing_texts)} existing texts for topic context")
    
    # 6. Get Twitter texts
    twitter_texts = [item['text'] for item in unified_data]
    
    # 7. Combine all texts for topic modeling
    all_texts = existing_texts + twitter_texts
    
    # 8. Get existing topic count (n_topics)
    n_topics = db.topics.count_documents({})
    if n_topics == 0:
        n_topics = 5  # Default if no topics exist
    
    # 9. Fit topic model on all texts
    logger.info(f"Fitting topic model on {len(all_texts)} texts with {n_topics} topics")
    topic_modeler = TopicModeler(n_topics=n_topics)
    topic_modeler.fit(all_texts)
    
    # 10. Transform only the Twitter texts to get their topics
    twitter_topic_results = topic_modeler.transform(twitter_texts)
    
    # 11. Update unified data with topic information
    for i, result in enumerate(twitter_topic_results):
        unified_data[i]['topic_id'] = result['topic_id']
        unified_data[i]['topic_confidence'] = result['topic_confidence']
        unified_data[i]['topic_keywords'] = ','.join(result['topic_keywords'][:5])
    
    # 12. Insert the transformed data
    if unified_data:
        result = db.sentiment_analysis.insert_many(unified_data)
        logger.info(f"Inserted {len(result.inserted_ids)} Twitter posts into sentiment_analysis collection.")
    else:
        logger.warning("No Twitter data to insert.")
    
    logger.info("Twitter transformation complete!")

if __name__ == "__main__":
    main()