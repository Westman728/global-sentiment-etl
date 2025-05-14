# Global Sentiment Analysis ETL Pipeline

A modular ETL (Extract, Transform, Load) pipeline for sentiment analysis across multiple data sources including Reddit, Twitter/X, and news headlines. The system collects text data, analyzes sentiment using VADER, identifies topics using LDA, and presents results through a Streamlit dashboard.

## Features

- **Multi-source data extraction**: Reddit posts, Twitter/X tweets, and news headlines
- **Sentiment analysis** via VADER (Valence Aware Dictionary and Sentiment Reasoner)
- **Topic modeling** using Latent Dirichlet Allocation (LDA)
- **Containerized database** with MongoDB in Docker
- **Interactive dashboard** for data exploration and visualization
- **Modular design** allowing easy extension and customization

## Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Reddit API credentials (via Reddit App)
- Twitter/X API credentials (v2)
- Access to internet for news scraping

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/westman728/global-sentiment-etl.git
cd global-sentiment-etl
```
### 2. Install dependencies
```bash
pip install -r requirements.txt
```
### 3. Configure credentials
Create the following YAML file in the `config` directory: <br>
**credentials.yaml**:
```yaml
# Reddit API credentials
reddit:
  client_id: "YOUR_REDDIT_CLIENT_ID"
  client_secret: "YOUR_REDDIT_CLIENT_SECRET"
  user_agent: "YOUR_APP_NAME by /u/YOUR_REDDIT_USERNAME"
  username: "username"  # Optional
  password: "password"  # Optional

# Twitter API credentials
twitter:
  bearer_token: YOUR_BEARER_TOKEN
  api_key: YOUR_TWITTER_API_KEY
  api_secret: YOUR_TWITTER_API_KEY_SECRET
  access_token: YOUR_ACCESS_TOKEN
  access_token_secret: YOUR_ACCESS_TOKEN_SECRET

mongodb:
  connection_string: "mongodb://localhost:CHOSEN_PORT"
  database: "global_sentiment"
```
### 4. Start MongoDB with Docker
```bash
docker-compose up -d
```
Starts MongoDB on port specified in docker-compose.yaml <br>
### 5. Run pipeline
```bash
python main.py
```
This will: <br>
* Extract data from Reddit, X and news sites.
* Transform data with sentiment analysis/topic modeling
* Load results into MongoDB

### 6. Launch dashboard
```bash
python -m streamlit run dashboard.py
```

**The settings.yaml** file contains system settings such as which subreddits, X search terms and news categories to collect.


