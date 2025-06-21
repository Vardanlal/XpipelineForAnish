import os
import logging
import asyncio
from typing import Dict, List
from dotenv import load_dotenv
import yaml
from tools.analyzer import TweetAnalyzer
from tools.storer import TweetStorer

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/main.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def main():
    """Main function to run the tweet analysis pipeline."""
    try:
        # Load configuration
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
        usernames = config.get('usernames', [])
        logger.info(f"Loaded {len(usernames)} usernames from configuration")

        # Initialize analyzer and storer
        analyzer = TweetAnalyzer()
        storer = TweetStorer()

        # Process each username
        for username in usernames:
            logger.info(f"Processing user: {username}")

            # Fetch and analyze tweets
            analysis_result = analyzer.analyze_user(username)
            if analysis_result["status"] == "success":
                logger.info(f"Analysis for {username}: {analysis_result['message']}")
            else:
                logger.warning(f"Analysis failed for {username}: {analysis_result['message']}")

            # Fetch and store tweets
            storage_result = storer.store_user_tweets(username)
            if storage_result["status"] == "success":
                logger.info(f"Storage for {username}: {storage_result['message']}")
            else:
                logger.warning(f"Storage failed for {username}: {storage_result['message']}")

    except Exception as e:
        logger.error(f"Error in main pipeline: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main()) 