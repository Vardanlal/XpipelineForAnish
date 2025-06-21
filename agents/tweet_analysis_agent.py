import os
import logging
import asyncio
from typing import Dict, List, Optional
from datetime import datetime
from tools.analyzer import TweetAnalyzer
from tools.storer import TweetStorer

logger = logging.getLogger(__name__)

class TweetAnalysisAgent:
    """
    Agent responsible for orchestrating tweet analysis workflows.
    Coordinates between data fetching, analysis, and storage operations.
    """
    
    def __init__(self):
        """Initialize the TweetAnalysisAgent."""
        self.analyzer = TweetAnalyzer()
        self.storer = TweetStorer()
        self.base_dir = 'data'
        
    async def process_user(self, username: str) -> Dict:
        """
        Process a single user's tweets through the complete pipeline.
        
        Args:
            username (str): Twitter username to process
            
        Returns:
            Dict: Processing results with status and details
        """
        try:
            logger.info(f"Starting processing for user: {username}")
            
            # Step 1: Fetch and store raw tweets
            storage_result = await self._fetch_and_store_tweets(username)
            if storage_result["status"] != "success":
                return {
                    "status": "error",
                    "message": f"Failed to fetch tweets for {username}: {storage_result['message']}",
                    "step": "fetch_and_store"
                }
            
            # Step 2: Analyze the stored tweets
            analysis_result = await self._analyze_user_tweets(username)
            if analysis_result["status"] != "success":
                return {
                    "status": "error", 
                    "message": f"Failed to analyze tweets for {username}: {analysis_result['message']}",
                    "step": "analysis"
                }
            
            return {
                "status": "success",
                "message": f"Successfully processed {username}",
                "storage": storage_result,
                "analysis": analysis_result,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error processing user {username}: {str(e)}")
            return {
                "status": "error",
                "message": f"Unexpected error processing {username}: {str(e)}"
            }
    
    async def process_multiple_users(self, usernames: List[str]) -> Dict[str, Dict]:
        """
        Process multiple users concurrently.
        
        Args:
            usernames (List[str]): List of Twitter usernames to process
            
        Returns:
            Dict[str, Dict]: Results for each user
        """
        try:
            logger.info(f"Starting batch processing for {len(usernames)} users")
            
            # Process users concurrently
            tasks = [self.process_user(username) for username in usernames]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Combine results
            combined_results = {}
            for i, username in enumerate(usernames):
                if isinstance(results[i], Exception):
                    combined_results[username] = {
                        "status": "error",
                        "message": f"Exception occurred: {str(results[i])}"
                    }
                else:
                    combined_results[username] = results[i]
            
            return combined_results
            
        except Exception as e:
            logger.error(f"Error in batch processing: {str(e)}")
            return {}
    
    async def _fetch_and_store_tweets(self, username: str) -> Dict:
        """Fetch and store tweets for a user."""
        try:
            # Use the storer to fetch and store tweets
            result = self.storer.store_user_tweets(username)
            return result
        except Exception as e:
            logger.error(f"Error fetching and storing tweets for {username}: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    async def _analyze_user_tweets(self, username: str) -> Dict:
        """Analyze tweets for a user."""
        try:
            # Use the analyzer to analyze tweets
            result = self.analyzer.analyze_user(username)
            return result
        except Exception as e:
            logger.error(f"Error analyzing tweets for {username}: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    def get_user_insights(self, username: str) -> Dict:
        """
        Get comprehensive insights for a user.
        
        Args:
            username (str): Twitter username
            
        Returns:
            Dict: User insights including sentiment analysis, engagement metrics, etc.
        """
        try:
            # Get the latest analysis file
            analyzed_dir = os.path.join(self.base_dir, 'analyzed')
            user_files = [f for f in os.listdir(analyzed_dir) if f.startswith(username)]
            
            if not user_files:
                return {
                    "status": "error",
                    "message": f"No analysis data found for {username}"
                }
            
            # Get the most recent analysis
            latest_file = sorted(user_files)[-1]
            filepath = os.path.join(analyzed_dir, latest_file)
            
            with open(filepath, 'r', encoding='utf-8') as f:
                import json
                analysis_data = json.load(f)
            
            # Calculate additional insights
            insights = self._calculate_insights(analysis_data)
            
            return {
                "status": "success",
                "username": username,
                "insights": insights,
                "analysis_file": latest_file
            }
            
        except Exception as e:
            logger.error(f"Error getting insights for {username}: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    def _calculate_insights(self, analysis_data: Dict) -> Dict:
        """Calculate additional insights from analysis data."""
        try:
            tweets = analysis_data.get('tweets', [])
            
            if not tweets:
                return {}
            
            # Calculate engagement metrics
            total_engagement = sum(tweet.get('total_engagement', 0) for tweet in tweets)
            avg_engagement = total_engagement / len(tweets) if tweets else 0
            
            # Calculate sentiment distribution
            sentiment_dist = analysis_data.get('sentiment_distribution', {})
            
            # Find most engaging tweets
            most_engaging = sorted(tweets, key=lambda x: x.get('total_engagement', 0), reverse=True)[:5]
            
            # Calculate posting frequency (if timestamps are available)
            posting_frequency = self._calculate_posting_frequency(tweets)
            
            return {
                "total_tweets": len(tweets),
                "total_engagement": total_engagement,
                "average_engagement": avg_engagement,
                "sentiment_distribution": sentiment_dist,
                "most_engaging_tweets": most_engaging,
                "posting_frequency": posting_frequency
            }
            
        except Exception as e:
            logger.error(f"Error calculating insights: {str(e)}")
            return {}
    
    def _calculate_posting_frequency(self, tweets: List[Dict]) -> Dict:
        """Calculate posting frequency from tweets."""
        try:
            if not tweets:
                return {}
            
            # Group tweets by date
            from collections import defaultdict
            date_counts = defaultdict(int)
            
            for tweet in tweets:
                created_at = tweet.get('created_at', '')
                if created_at:
                    # Extract date from timestamp
                    date = created_at.split(' ')[0] if ' ' in created_at else created_at[:10]
                    date_counts[date] += 1
            
            return dict(date_counts)
            
        except Exception as e:
            logger.error(f"Error calculating posting frequency: {str(e)}")
            return {} 