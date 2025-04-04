import logging
import requests
import yaml
import pandas as pd
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
import time
import random

logger = logging.getLogger(__name__)

class NewsExtractor:
    """Extract news headlines from various sources."""
    
    def __init__(self, config_path=None):
        """Initialize the news extractor."""
        self.config_path = config_path or Path("config/credentials.yaml")
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Brave/1.32.113 Chrome/91.0.4472.124 Safari/537.36"
        }
        
    def extract_bbc_headlines(self, category="news", limit=20):
        """Extract headlines from BBC.
        
        Args:
            category: News category (e.g., 'news', 'business', 'technology')
            limit: Maximum number of headlines to retrieve
            
        Returns:
            DataFrame containing headline data
        """
        logger.info(f"Extracting up to {limit} BBC headlines from category '{category}'")
        
        url = f"https://www.bbc.com/{category}"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, "html.parser")
            
            # Extract headlines
            headlines = []
            article_elements = soup.select("h3.gs-c-promo-heading__title")
            
            for element in article_elements[:limit]:
                headlines.append({
                    "title": element.text.strip(),
                    "url": element.parent.get("href") if element.parent.name == "a" else None,
                    "source": "bbc",
                    "category": category,
                    "extracted_at": datetime.now()
                })
            
            logger.info(f"Extracted {len(headlines)} BBC headlines")
            return pd.DataFrame(headlines)
        except Exception as e:
            logger.error(f"Error extracting BBC headlines: {str(e)}")
            return pd.DataFrame()
    
    def extract_reuters_headlines(self, category="world", limit=20):
        """Extract headlines from Reuters.
        
        Args:
            category: News category (e.g., 'world', 'business', 'technology')
            limit: Maximum number of headlines to retrieve
            
        Returns:
            DataFrame containing headline data
        """
        logger.info(f"Extracting up to {limit} Reuters headlines from category '{category}'")
        
        url = f"https://www.reuters.com/{category}"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, "html.parser")
            
            # Extract headlines (specific selectors may need adjustment)
            headlines = []
            article_elements = soup.select("h3.story-title")
            
            for element in article_elements[:limit]:
                headlines.append({
                    "title": element.text.strip(),
                    "url": element.parent.get("href") if element.parent.name == "a" else None,
                    "source": "reuters",
                    "category": category,
                    "extracted_at": datetime.now()
                })
            
            logger.info(f"Extracted {len(headlines)} Reuters headlines")
            return pd.DataFrame(headlines)
        except Exception as e:
            logger.error(f"Error extracting Reuters headlines: {str(e)}")
            return pd.DataFrame()
    
    def extract_cnn_headlines(self, category="world", limit=20):
        """Extract headlines from CNN.
        
        Args:
            category: News category (e.g., 'world', 'business', 'technology')
            limit: Maximum number of headlines to retrieve
            
        Returns:
            DataFrame containing headline data
        """
        logger.info(f"Extracting up to {limit} CNN headlines from category '{category}'")
        
        url = f"https://www.cnn.com/{category}"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, "html.parser")
            
            # Extract headlines (specific selectors may need adjustment)
            headlines = []
            article_elements = soup.select("span.container__headline-text")
            
            for element in article_elements[:limit]:
                headlines.append({
                    "title": element.text.strip(),
                    "url": element.parent.parent.get("href") if element.parent.parent else None,
                    "source": "cnn",
                    "category": category,
                    "extracted_at": datetime.now()
                })
            
            logger.info(f"Extracted {len(headlines)} CNN headlines")
            return pd.DataFrame(headlines)
        except Exception as e:
            logger.error(f"Error extracting CNN headlines: {str(e)}")
            return pd.DataFrame()
    
    def extract_from_all_sources(self, categories=None, limit_per_source=20):
        """Extract headlines from all configured sources.
        
        Args:
            categories: Dictionary mapping sources to categories
            limit_per_source: Maximum headlines per source/category
            
        Returns:
            DataFrame containing all headline data
        """
        if categories is None:
            categories = {
                "bbc": ["news", "business", "technology"],
                "reuters": ["world", "business", "technology"],
                "cnn": ["world", "business", "tech"]
            }
        
        all_headlines = pd.DataFrame()
        
        # BBC
        if "bbc" in categories:
            for category in categories["bbc"]:
                # Add random delay to be nice to servers
                time.sleep(random.uniform(1, 3))
                headlines = self.extract_bbc_headlines(category, limit_per_source)
                all_headlines = pd.concat([all_headlines, headlines], ignore_index=True)
        
        # Reuters
        if "reuters" in categories:
            for category in categories["reuters"]:
                time.sleep(random.uniform(1, 3))
                headlines = self.extract_reuters_headlines(category, limit_per_source)
                all_headlines = pd.concat([all_headlines, headlines], ignore_index=True)
        
        # CNN
        if "cnn" in categories:
            for category in categories["cnn"]:
                time.sleep(random.uniform(1, 3))
                headlines = self.extract_cnn_headlines(category, limit_per_source)
                all_headlines = pd.concat([all_headlines, headlines], ignore_index=True)
        
        logger.info(f"Extracted {len(all_headlines)} headlines from all sources")
        return all_headlines

if __name__ == "__main__":
    # Setup basic logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Test the extractor
    extractor = NewsExtractor()
    
    # Test with limited categories for testing
    test_categories = {
        "bbc": ["news"],
        "reuters": ["world"],
        "cnn": ["world"]
    }
    
    headlines = extractor.extract_from_all_sources(
        categories=test_categories,
        limit_per_source=5
    )
    
    print(f"Extracted {len(headlines)} headlines in total")
    print(headlines.head())