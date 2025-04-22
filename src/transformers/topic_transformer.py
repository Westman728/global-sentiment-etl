import pandas as pd
import logging
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import numpy as np

logger = logging.getLogger(__name__)

class TopicModeler:
    """ Extracts topics from text data using Latent Dirichlet Allocation (LDA) """

    def __init__(self, n_topics=5, max_features=1000):
        """
        Initializes the TopicModeler.
        
        Args:
            n_topics (int): Number of topics to extract.
            max_features (int): Maximum number of features to consider.
        """
        self.n_topics = n_topics
        self.max_features = max_features
        self.vectorizer = CountVectorizer(max_features=max_features, stop_words='english')
        self.model = LatentDirichletAllocation(
            n_components=n_topics,
            learning_method='online',
            random_state=30,
        )
        self.topic_keywords = None

        def fit(self, texts):
            """
            Fits the TopicModeler to the given text data.
            """
            logger.info(f"Fitting the topic model on {len(texts)} documents.")
            dtm = self.vectorizer.fit_transform(texts)
            self.model.fit(dtm)
            feature_names = self.vectorizer.get_feature_names_out()
            self.topic_keywords = []

            for topic_idx, topic in enumerate(self.model.components_):
                top_keywords_idx = topic.argsort()[:-11:-1] # gets the top 10 keywords
                top_keywords = [feature_names[i] for i in top_keywords_idx]
                self.topic_keywords.append(top_keywords)

                logger.info(f"Topic {topic_idx}: {', '.join(top_keywords)}")

            return self
        
        def transform(self, texts):
            """
            Assigns topics to the given text data.
            """
            if not isinstance(texts, list):
                texts = [texts]

            dtm = self.vectorizer.transform(texts)
            topic_distribution = self.model.transform(dtm)
            dominant_topics = topic_distribution.argmax(axis=1)
            confidences = topic_distribution.max(axis=1)

            results = []
            for i, (text, topic, confidence) in enumerate(zip(texts, dominant_topics, confidences)):
                keywords = self.topic_keywords[topic] if self.topic_keywords else []

                results.append({
                    "text": text,
                    "topic_id": int(topic),
                    "topic_confidence": float(confidence),
                    "topic_keywords": keywords,
                })

            return results
        
        def get_topic_keywords(self, topic_id):
            """
            Returns the keywords for a given topic ID.
            """
            if self.topic_keywords and 0 <= topic_id < len(self.topic_keywords):
                return self.topic_keywords[topic_id]
            return []
        
        def get_topic_name(self, topic_id):
            """
            Returns the name of a given topic ID based on its keywords.
            """
            keywords = self.get_topic_keywords(topic_id)
            if keywords:
                return f"Topic {topic_id}: {keywords[0]} - {keywords[1]}"
            return f"Topic {topic_id}"
