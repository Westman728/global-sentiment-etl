import logging
from pathlib import Path
from src.extractors.news_extractor import NewsExtractor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def test_news_extractor():
    try:
        news_extractor = NewsExtractor()
        
        # source = "bbc"
        # category = "news"
        # logger.info(f"Testing extraction from {source}/{category}...")
        

        # headlines = news_extractor.extract_bbc_headlines(
        #     category=category,
        #     limit=5
        # )
        
        # if headlines is not None and not headlines.empty:
        #     logger.info(f"Successfully extracted {len(headlines)} headlines from {source}/{category}")
        #     logger.info(f"Columns: {headlines.columns.tolist()}")
        #     logger.info(f"Column datatypes: {headlines.dtypes}")
        #     logger.info(f"Sample headline: {headlines.iloc[0]['title']}")
        #     logger.info(f"{source} extraction test: PASS")
        # else:
        #     logger.error(f"No headlines returned from {source}/{category}")
        #     logger.info(f"{source} extraction test: FAIL")
        #     return False
        

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

        

        logger.info("Testing extraction from all sources...")
        

        test_categories = {
            "bbc": ["news"],
            # "reuters": ["world"],
            "cnn": ["world"]
        }
        

        all_headlines = news_extractor.extract_from_all_sources(
            categories=test_categories,
            limit_per_source=10
        )
        

        if all_headlines is not None and not all_headlines.empty:
            logger.info(f"Successfully extracted {len(all_headlines)} headlines from all sources")
            sources = all_headlines['source'].unique().tolist()
            logger.info(f"Sources represented: {sources}")
            logger.info("Combined extraction test: PASS")
            return True
        else:
            logger.error("No headlines returned from combined extraction")
            logger.info("Combined extraction test: FAIL")
            return False
            
    except Exception as e:
        logger.error(f"Error testing News extractor: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("Starting News extractor test...")
    result = test_news_extractor()
    logger.info(f"Overall test result: {'PASS' if result else 'FAIL'}")