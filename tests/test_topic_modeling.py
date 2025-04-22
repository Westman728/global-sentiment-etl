import logging
import pandas as pd
from src.extractors.reddit_extractor import RedditExtractor
from src.extractors.news_extractor import NewsExtractor
from src.transformers.topic_transformer import TopicModeler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def test_topic_modeling():
    """Test the topic modeling functionality of the TopicModeler class using our data sources."""
    logger.info("Starting test for topic modeling...")

    try:
        logger.info("Extracting Reddit data...")
        reddit_extractor = RedditExtractor()
        reddit_posts = reddit_extractor.extract_multiple_subreddits(
            subreddit_list=["news", "worldnews", "technology"],
            limit=15,
            sort="hot",
        )
        logger.info(f"Reddit data extracted: {len(reddit_posts)} posts.")
    except Exception as e:
        logger.error(f"Error extracting Reddit data: {str(e)}")
        reddit_posts = pd.DataFrame()  # fallback to empty DataFrame if extraction fails

    try:
        logger.info("Extracting news data...")
        news_extractor = NewsExtractor()
        categories = {
            "bbc" : ["news", "business"],
            "cnn" : ["world", "politics"],
        }
        headlines = news_extractor.extract_from_all_sources(
            categories=categories,
            limit_per_source=10,
        )
        logger.info(f"News data extracted: {len(headlines)} headlines.")
    except Exception as e:
        logger.error(f"Error extracting news data: {str(e)}")
        headlines = pd.DataFrame()
    
    all_texts = []

    if not reddit_posts.empty:
        all_texts.extend(reddit_posts["title"].tolist())
        logger.info(f"Reddit titles added: {len(reddit_posts['title'])} titles.")
    
    if not headlines.empty:
        all_texts.extend(headlines["title"].tolist())
        logger.info(f"News titles added: {len(headlines['title'])} titles.")
    
    if len(all_texts) < 10:
        logger.info(f"all_texts: {len(all_texts)}")
        logger.error("Not enough data for topic modeling. Need at least 10 titles.")
        return False
    
    try:
        n_topics = 5
        logger.info(f"Performing topic modeling with {n_topics} topics...")
        topic_modeler = TopicModeler(n_topics=n_topics, max_features=500)

        logger.info(f"Fitting model on {len(all_texts)} texts...")
        topic_modeler.fit(all_texts)

        logger.info("Topics discovered:")
        for i in range(topic_modeler.n_topics):
            keywords = topic_modeler.get_top_keywords(i)
            logger.info(f"Topic {i}: {', '.join(keywords[:5])}")

        logger.info("ASsigning topics to texts...")
        results = topic_modeler.transform(all_texts)

        results_df = pd.DataFrame(results)
        results_df["source"] = ["reddit"] * len(reddit_posts) + ["news"] * len(headlines)

        topic_distribution = pd.crosstab(
            results_df["topic"],
            results_df["source"],
        )

        logger.info("Topic distribution:")
        logger.info(f"\n{topic_distribution}")

        source_topics = {}
        for source in ["reddit", "news"]:
            if source in topic_distribution.columns:
                source_data = topic_distribution[source]
                dominant_topic = source_data.idxmax()
                source_topics[source] = dominant_topic
                logger.info(f"Dominant topic for {source}: {dominant_topic}")

        
        logger.info("Example topic assignments:")
        for i, (text, result) in enumerate(zip(all_texts, results)):
            topic_id = result["topic_id"]
            confidence = result["topic_confidence"]
            logger.info(f"Text: '{text[:50]}.. ' -> Topic {topic_id} (Confidence: {confidence:.2f})")

        logger.info("Topic modeling test completed successfully.")
        return True
    except Exception as e:
        print(f"Type: {type(topic_modeler)}")
        print(f"Dir: {dir(topic_modeler)}")
        print(f"Attributes: {vars(topic_modeler)}")
        logger.error(f"Error during topic modeling: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    
if __name__ == "__main__":
    test_result = test_topic_modeling()
    print(f"Test result: {'PASSED' if test_result else 'FAILED'}")