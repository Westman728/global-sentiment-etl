import logging
from pathlib import Path
from src.extractors.news_extractor import NewsExtractor
from src.transformers.sentiment_transformer import SentimentTransformer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def test_news_extractor():
    try:
        news_extractor = NewsExtractor()
        
        headlines = news_extractor.extract_bbc_headlines(
            category="news",
            limit=5
        )
        
        if headlines is None and headlines.empty:
            logger.error("No headlines returned from BBC extraction")
            return False
        
        logger.info("Extracted headlines for sentiment testing")
        for i, headline in enumerate(headlines["title"]):
            logger.info(f"Headline {i+1}: {headline}")

        transformer = SentimentTransformer()
        transformed = transformer.transform_news_data(headlines)

        logger.info("\nSentiment analysis results:")
        for i, row in transformed.iterrows():
            logger.info(f"Headline {i+1}: {row['title']}")
            logger.info(f"Sentiment compound: {row['sentiment_compound']}")
            logger.info(f"Sentiment positive: {row['sentiment_positive']}")
            logger.info(f"Sentiment neutral: {row['sentiment_neutral']}")
            logger.info(f"Sentiment negative: {row['sentiment_negative']}")
            logger.info("-----")
            logger.info("\n")

        has_sentiment = any(transformed["sentiment_compound"] != 0)
        if has_sentiment:
            logger.info("Sentiment test: PASS. Sentiment data available.")
            return True
        else:
            logger.warning("Sentiment test: FAIL. No sentiment data available.")
            logger.info("Could be due to natural language processing issues or issue with analyzer.")
            return False
    except Exception as e:
        logger.error(f"Error testing sentiment analysis: {str(e)}")
        return False
        

        # source = "reuters"
        # category = "world"
        # logger.info(f"Testing extraction from {source}/{category}...")
        

        # headlines = news_extractor.extract_reuters_headlines(
        #     category=category,
        #     limit=5
        # )
        

        # if headlines is not None and not headlines.empty:
        #     logger.info(f"Successfully extracted {len(headlines)} headlines from {source}/{category}")
        #     logger.info(f"Sample headline: {headlines.iloc[0]['title']}")
        #     logger.info(f"{source} extraction test: PASS")
        # else:
        #     logger.error(f"No headlines returned from {source}/{category}")
        #     logger.info(f"{source} extraction test: FAIL")

if __name__ == "__main__":
    logger.info("Starting News extractor test...")
    result = test_news_extractor()
    logger.info(f"Overall test result: {'PASS' if result else 'FAIL'}")