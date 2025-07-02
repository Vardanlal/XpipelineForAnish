import os
import sys
import logging
import yaml
from datetime import datetime
from dotenv import load_dotenv

# Add parent directory to path to import tools
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.data_fetcher import DataFetcher
from tools.analysis_processor import AnalysisProcessor
from tools.data_manager import DataManager

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_usernames():
    """Load usernames from config.yaml."""
    try:
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.yaml')
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            return config.get('usernames', [])
    except Exception as e:
        logger.error(f"Error loading usernames from config: {str(e)}")
        return []

def main():
    """Main function to run the entire pipeline."""
    try:
        # Initialize components
        fetcher = DataFetcher()
        analyzer = AnalysisProcessor()
        manager = DataManager()
        
        # Load usernames from config
        usernames = load_usernames()
        if not usernames:
            logger.error("No usernames found in config.yaml")
            sys.exit(1)
        
        # Step 1: Fetch tweets
        logger.info("Step 1: Fetching tweets...")
        fetch_results = fetcher.fetch_all_users(usernames)
        
        # Step 2: Store raw tweets and download media
        logger.info("Step 2: Storing raw tweets and downloading media...")
        for username, result in fetch_results.items():
            if result.get("status") == "success":
                # Store raw tweets
                store_result = manager.store_tweet_data(result["tweets"], username, data_type='raw')
                if store_result["status"] == "success":
                    logger.info(f"Successfully stored raw tweets for {username}")
                else:
                    logger.warning(f"Failed to store raw tweets for {username}: {store_result['message']}")
                # Download media
                date_str = datetime.now().strftime('%Y-%m-%d')
                media_info = fetcher.extract_media_from_tweets(result["tweets"])
                all_media = media_info.get('images', []) + media_info.get('videos', []) + media_info.get('gifs', [])
                if all_media:
                    saved_files = fetcher.download_and_store_media(all_media, username, date_str)
                    logger.info(f"Downloaded {len(saved_files)} media files for {username}")
            else:
                logger.warning(f"Failed to fetch tweets for {username}: {result.get('message')}")
        
        # Step 3: Analyze tweets
        logger.info("Step 3: Analyzing tweets...")
        analysis_results = {}
        for username in usernames:
            analysis_result = analyzer.analyze_user_tweets(username)
            analysis_results[username] = analysis_result
            if analysis_result["status"] == "success":
                store_analysis = manager.store_analysis_results(analysis_result["analysis"], username)
                if store_analysis["status"] == "success":
                    logger.info(f"Successfully stored analysis for {username}")
                else:
                    logger.warning(f"Failed to store analysis for {username}: {store_analysis['message']}")
            else:
                logger.warning(f"Analysis failed for {username}: {analysis_result['message']}")
        
        # Log final results
        logger.info("Pipeline completed!")
        for username in usernames:
            fetch_count = fetch_results.get(username, {}).get("tweet_count", 0)
            analysis_status = analysis_results.get(username, {}).get("status", "error")
            logger.info(f"Results for {username}:")
            logger.info(f"  - Fetched {fetch_count} tweets")
            logger.info(f"  - Analysis status: {analysis_status}")
            
    except Exception as e:
        logger.error(f"Error in pipeline: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 