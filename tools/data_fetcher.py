import os
import logging
import json
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from apify_client import ApifyClient
from dotenv import load_dotenv
import yaml
import requests
from pathlib import Path

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/fetcher.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataFetcher:
    """
    Comprehensive data fetcher tool that handles all data retrieval operations.
    Includes tweet fetching, user information, media extraction, and real-time monitoring.
    """
    
    def __init__(self):
        """Initialize the DataFetcher with Apify client and configuration."""
        self.apify_token = os.getenv('APIFY_TOKEN')
        if not self.apify_token:
            raise ValueError("APIFY_TOKEN not found in environment variables")
        
        self.client = ApifyClient(self.apify_token)
        self.config = self._load_config()
        self.max_tweets_per_user = self.config.get('max_tweets_per_user', 100) if self.config else 100
        self.base_dir = Path('data/raw')
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
    def _load_config(self) -> Optional[Dict]:
        """Load configuration from config.yaml."""
        try:
            with open('config.yaml', 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Error loading config: {str(e)}")
            return None
    
    def fetch_user_tweets(self, username: str, max_tweets: int = None) -> Dict:
        """
        Fetch tweets for a single user using Apify.
        
        Args:
            username (str): Twitter username to fetch tweets for
            max_tweets (int): Maximum number of tweets to fetch
            
        Returns:
            Dict: Fetch results with status and data
        """
        try:
            if max_tweets is None:
                max_tweets = self.max_tweets_per_user
                
            logger.info(f"Fetching tweets for user: {username}")
            
            # Run the Twitter scraper actor
            run_input = {
                "handles": [username],
                "maxTweets": max_tweets,
                "addUserInfo": True,
                "includeReplies": True,
                "includeRetweets": True
            }
            
            run = self.client.actor("quacker/twitter-scraper").call(run_input=run_input)
            
            # Get the results
            tweets = []
            for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                tweets.append(item)
            
            # Save raw data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{username}_raw_{timestamp}.json"
            filepath = self.base_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(tweets, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Successfully fetched {len(tweets)} tweets for {username}")
            
            return {
                "status": "success",
                "username": username,
                "tweet_count": len(tweets),
                "filepath": str(filepath),
                "tweets": tweets
            }
            
        except Exception as e:
            logger.error(f"Error fetching tweets for {username}: {str(e)}")
            return {
                "status": "error",
                "username": username,
                "message": str(e)
            }
    
    def fetch_all_users(self, usernames: List[str]) -> Dict[str, Dict]:
        """
        Fetch tweets for multiple users concurrently.
        
        Args:
            usernames (List[str]): List of Twitter usernames
            
        Returns:
            Dict[str, Dict]: Results for each user
        """
        try:
            logger.info(f"Fetching tweets for {len(usernames)} users")
            
            results = {}
            for username in usernames:
                results[username] = self.fetch_user_tweets(username)
            
            return results
            
        except Exception as e:
            logger.error(f"Error fetching all users: {str(e)}")
            return {}
    
    def fetch_recent_tweets(self, username: str, days: int = 7) -> Dict:
        """
        Fetch recent tweets for a user within the specified number of days.
        
        Args:
            username (str): Twitter username
            days (int): Number of days to look back
            
        Returns:
            Dict: Fetch results with status and data
        """
        try:
            since_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            
            logger.info(f"Fetching recent tweets for {username} since {since_date}")
            
            # Run the Twitter scraper actor with date filter
            run_input = {
                "handles": [username],
                "maxTweets": self.max_tweets_per_user,
                "addUserInfo": True,
                "since": since_date
            }
            
            run = self.client.actor("quacker/twitter-scraper").call(run_input=run_input)
            
            # Get the results
            tweets = []
            for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                tweets.append(item)
            
            # Save raw data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{username}_recent_{timestamp}.json"
            filepath = self.base_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(tweets, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Successfully fetched {len(tweets)} recent tweets for {username}")
            
            return {
                "status": "success",
                "username": username,
                "tweet_count": len(tweets),
                "since_date": since_date,
                "filepath": str(filepath),
                "tweets": tweets
            }
            
        except Exception as e:
            logger.error(f"Error fetching recent tweets for {username}: {str(e)}")
            return {
                "status": "error",
                "username": username,
                "message": str(e)
            }
    
    def fetch_tweets_by_keyword(self, keyword: str, max_tweets: int = 100) -> Dict:
        """
        Fetch tweets containing a specific keyword.
        
        Args:
            keyword (str): Keyword to search for
            max_tweets (int): Maximum number of tweets to fetch
            
        Returns:
            Dict: Fetch results with status and data
        """
        try:
            logger.info(f"Fetching tweets for keyword: {keyword}")
            
            # Run the Twitter scraper actor with keyword search
            run_input = {
                "searchTerms": [keyword],
                "maxTweets": max_tweets,
                "addUserInfo": True
            }
            
            run = self.client.actor("quacker/twitter-scraper").call(run_input=run_input)
            
            # Get the results
            tweets = []
            for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                tweets.append(item)
            
            # Save raw data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"keyword_{keyword.replace(' ', '_')}_{timestamp}.json"
            filepath = self.base_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(tweets, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Successfully fetched {len(tweets)} tweets for keyword '{keyword}'")
            
            return {
                "status": "success",
                "keyword": keyword,
                "tweet_count": len(tweets),
                "filepath": str(filepath),
                "tweets": tweets
            }
            
        except Exception as e:
            logger.error(f"Error fetching tweets for keyword '{keyword}': {str(e)}")
            return {
                "status": "error",
                "keyword": keyword,
                "message": str(e)
            }
    
    def fetch_user_profile(self, username: str) -> Dict:
        """
        Fetch detailed user profile information.
        
        Args:
            username (str): Twitter username
            
        Returns:
            Dict: User profile information
        """
        try:
            logger.info(f"Fetching profile for user: {username}")
            
            # Run the Twitter scraper actor for user info
            run_input = {
                "handles": [username],
                "maxTweets": 1,  # We only need user info
                "addUserInfo": True
            }
            
            run = self.client.actor("quacker/twitter-scraper").call(run_input=run_input)
            
            # Get the results
            user_info = None
            for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                if 'user' in item:
                    user_info = item['user']
                    break
            
            if user_info:
                # Save user profile
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{username}_profile_{timestamp}.json"
                filepath = self.base_dir / filename
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(user_info, f, ensure_ascii=False, indent=2)
                
                logger.info(f"Successfully fetched profile for {username}")
                
                return {
                    "status": "success",
                    "username": username,
                    "filepath": str(filepath),
                    "profile": user_info
                }
            else:
                return {
                    "status": "error",
                    "username": username,
                    "message": "No user profile found"
                }
                
        except Exception as e:
            logger.error(f"Error fetching profile for {username}: {str(e)}")
            return {
                "status": "error",
                "username": username,
                "message": str(e)
            }
    
    def extract_media_from_tweets(self, tweets: List[Dict]) -> Dict:
        """
        Extract and organize media from tweets.
        
        Args:
            tweets (List[Dict]): List of tweets
            
        Returns:
            Dict: Organized media information
        """
        try:
            media_info = {
                'images': [],
                'videos': [],
                'gifs': [],
                'total_media': 0
            }
            
            for tweet in tweets:
                if 'media' in tweet:
                    for item in tweet['media']:
                        media_item = {
                            'tweet_id': tweet.get('id_str', ''),
                            'tweet_text': tweet.get('full_text', tweet.get('text', '')),
                            'media_url': item.get('url', ''),
                            'media_type': item.get('type', ''),
                            'created_at': tweet.get('created_at', '')
                        }
                        
                        if item['type'] == 'photo':
                            media_info['images'].append(media_item)
                        elif item['type'] == 'video':
                            media_info['videos'].append(media_item)
                        elif item['type'] == 'animated_gif':
                            media_info['gifs'].append(media_item)
                        
                        media_info['total_media'] += 1
            
            return media_info
            
        except Exception as e:
            logger.error(f"Error extracting media: {str(e)}")
            return {'images': [], 'videos': [], 'gifs': [], 'total_media': 0}
    
    def fetch_trending_topics(self, location: str = "1") -> Dict:
        """
        Fetch trending topics for a location.
        
        Args:
            location (str): Location ID (1 for worldwide)
            
        Returns:
            Dict: Trending topics information
        """
        try:
            logger.info(f"Fetching trending topics for location: {location}")
            
            # Run the Twitter scraper actor for trending topics
            run_input = {
                "trendingTopics": True,
                "location": location
            }
            
            run = self.client.actor("quacker/twitter-scraper").call(run_input=run_input)
            
            # Get the results
            trending_topics = []
            for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                trending_topics.append(item)
            
            # Save trending topics
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"trending_topics_{location}_{timestamp}.json"
            filepath = self.base_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(trending_topics, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Successfully fetched {len(trending_topics)} trending topics")
            
            return {
                "status": "success",
                "location": location,
                "topic_count": len(trending_topics),
                "filepath": str(filepath),
                "trending_topics": trending_topics
            }
            
        except Exception as e:
            logger.error(f"Error fetching trending topics: {str(e)}")
            return {
                "status": "error",
                "location": location,
                "message": str(e)
            }
    
    def download_and_store_media(self, media_items: list, username: str, date_str: str) -> list:
        """
        Download media files and store them in output/{date}/media/{username}/
        Args:
            media_items (list): List of dicts with 'media_url' and 'media_type'
            username (str): Twitter username
            date_str (str): Date string in YYYY-MM-DD format
        Returns:
            list: List of saved file paths
        """
        saved_files = []
        base_dir = Path(f'output/{date_str}/media/{username}')
        base_dir.mkdir(parents=True, exist_ok=True)
        for item in media_items:
            url = item.get('media_url')
            mtype = item.get('media_type', 'media')
            if not url:
                continue
            ext = '.jpg' if mtype == 'photo' else '.mp4' if mtype == 'video' else '.gif'
            fname = url.split('/')[-1].split('?')[0]
            if not fname.endswith((ext, '.jpeg', '.png', '.webp', '.webm')):
                fname += ext
            file_path = base_dir / fname
            try:
                resp = requests.get(url, stream=True, timeout=10)
                if resp.status_code == 200:
                    with open(file_path, 'wb') as f:
                        for chunk in resp.iter_content(1024):
                            f.write(chunk)
                    saved_files.append(str(file_path))
            except Exception as e:
                logger.error(f"Failed to download media {url}: {e}")
        return saved_files 