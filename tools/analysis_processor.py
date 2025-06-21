import os
import logging
import json
from typing import Dict, List, Optional, Any
from datetime import datetime
from textblob import TextBlob
from transformers import pipeline
import yaml
from pathlib import Path
import re
from collections import defaultdict, Counter
import numpy as np
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/analyzer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AnalysisProcessor:
    """
    Comprehensive analysis processor tool that handles all analysis operations.
    Includes sentiment analysis, text processing, insights generation, and data transformation.
    """
    
    def __init__(self):
        """Initialize the AnalysisProcessor with NLP models and configuration."""
        self.base_dir = Path('data')
        self.ensure_directories()
        self.config = self._load_config()
        
        # Initialize NLP models
        try:
            self.summarizer = pipeline("summarization", model="facebook/bart-large-cnn")
            self.sentiment_analyzer = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment-latest")
        except Exception as e:
            logger.warning(f"Could not initialize some NLP models: {str(e)}")
            self.summarizer = None
            self.sentiment_analyzer = None
    
    def ensure_directories(self):
        """Ensure all necessary directories exist."""
        directories = [
            self.base_dir / 'analyzed',
            self.base_dir / 'processed',
            self.base_dir / 'insights',
            self.base_dir / 'reports'
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def _load_config(self) -> Optional[Dict]:
        """Load configuration from config.yaml."""
        try:
            with open('config.yaml', 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Error loading config: {str(e)}")
            return None
    
    def analyze_user_tweets(self, username: str) -> Dict:
        """
        Analyze tweets for a specific user.
        
        Args:
            username (str): Twitter username to analyze
            
        Returns:
            Dict: Analysis results with status and data
        """
        try:
            # Find the most recent raw data file for this user
            raw_dir = self.base_dir / 'raw'
            user_files = [f for f in raw_dir.glob(f"{username}_raw_*.json")]
            
            if not user_files:
                return {
                    "status": "error",
                    "message": f"No data files found for {username}"
                }
            
            # Get the most recent file
            latest_file = max(user_files, key=lambda x: x.stat().st_mtime)
            
            # Load and analyze tweets
            with open(latest_file, 'r', encoding='utf-8') as f:
                tweets = json.load(f)
            
            analyzed_tweets = []
            for tweet in tweets:
                analysis = self.analyze_single_tweet(tweet)
                analyzed_tweets.append(analysis)
            
            # Generate comprehensive analysis
            analysis_results = self._generate_comprehensive_analysis(analyzed_tweets, username)
            
            # Save analyzed results
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = self.base_dir / 'analyzed' / f"{username}_analyzed_{timestamp}.json"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(analysis_results, f, ensure_ascii=False, indent=2)
            
            return {
                "status": "success",
                "message": f"Successfully analyzed {len(analyzed_tweets)} tweets for {username}",
                "filepath": str(output_file),
                "analysis": analysis_results
            }
            
        except Exception as e:
            logger.error(f"Error analyzing tweets for {username}: {str(e)}")
            return {
                "status": "error",
                "message": f"Error analyzing tweets for {username}: {str(e)}"
            }
    
    def analyze_single_tweet(self, tweet: Dict) -> Dict:
        """
        Analyze a single tweet comprehensively.
        
        Args:
            tweet (Dict): Raw tweet data
            
        Returns:
            Dict: Comprehensive tweet analysis
        """
        try:
            # Extract text from Apify's tweet format
            text = tweet.get('full_text', tweet.get('text', ''))
            
            # Perform sentiment analysis
            sentiment_analysis = self._analyze_sentiment(text)
            
            # Generate summary
            summary = self._generate_summary(text)
            
            # Extract engagement metrics
            engagement = self._extract_engagement_metrics(tweet)
            
            # Extract media information
            media_info = self._extract_media_info(tweet)
            
            # Perform text analysis
            text_analysis = self._analyze_text_content(text)
            
            # Extract user information
            user_info = self._extract_user_info(tweet)
            
            return {
                'id': tweet.get('id_str', ''),
                'text': text,
                'summary': summary,
                'created_at': tweet.get('created_at', ''),
                'sentiment': sentiment_analysis,
                'engagement': engagement,
                'text_analysis': text_analysis,
                'media': media_info,
                'user': user_info,
                'url': tweet.get('url', ''),
                'lang': tweet.get('lang', ''),
                'is_retweet': tweet.get('retweeted_status') is not None,
                'is_reply': tweet.get('in_reply_to_status_id_str') is not None
            }
            
        except Exception as e:
            logger.error(f"Error analyzing tweet: {str(e)}")
            return {
                'error': str(e),
                'raw_tweet': tweet
            }
    
    def _analyze_sentiment(self, text: str) -> Dict:
        """Perform comprehensive sentiment analysis."""
        try:
            # TextBlob sentiment analysis
            blob = TextBlob(text)
            textblob_sentiment = {
                'polarity': blob.sentiment.polarity,
                'subjectivity': blob.sentiment.subjectivity,
                'classification': self._classify_sentiment(blob.sentiment.polarity)
            }
            
            # Transformer-based sentiment analysis
            transformer_sentiment = None
            if self.sentiment_analyzer:
                try:
                    result = self.sentiment_analyzer(text[:512])  # Limit length for transformer
                    transformer_sentiment = {
                        'label': result[0]['label'],
                        'score': result[0]['score']
                    }
                except Exception as e:
                    logger.warning(f"Transformer sentiment analysis failed: {str(e)}")
            
            return {
                'textblob': textblob_sentiment,
                'transformer': transformer_sentiment,
                'overall_sentiment': textblob_sentiment['classification']
            }
            
        except Exception as e:
            logger.error(f"Error in sentiment analysis: {str(e)}")
            return {
                'textblob': {'polarity': 0, 'subjectivity': 0, 'classification': 'neutral'},
                'transformer': None,
                'overall_sentiment': 'neutral'
            }
    
    def _classify_sentiment(self, polarity: float) -> str:
        """Classify sentiment as bullish, bearish, or neutral."""
        if polarity > 0.1:
            return "bullish"
        elif polarity < -0.1:
            return "bearish"
        else:
            return "neutral"
    
    def _generate_summary(self, text: str) -> str:
        """Generate a summary of the tweet text."""
        try:
            if not self.summarizer or len(text.split()) < 10:
                return text[:100] + "..." if len(text) > 100 else text
            
            # Generate summary
            summary = self.summarizer(text, max_length=50, min_length=10, do_sample=False)
            return summary[0]['summary_text']
            
        except Exception as e:
            logger.warning(f"Error generating summary: {str(e)}")
            return text[:100] + "..." if len(text) > 100 else text
    
    def _extract_engagement_metrics(self, tweet: Dict) -> Dict:
        """Extract engagement metrics from a tweet."""
        try:
            engagement = {
                'retweets': tweet.get('retweet_count', 0),
                'likes': tweet.get('favorite_count', 0),
                'replies': tweet.get('reply_count', 0),
                'quotes': tweet.get('quote_count', 0)
            }
            
            # Calculate total engagement
            total_engagement = sum(engagement.values())
            
            # Calculate engagement rate (if follower count is available)
            follower_count = tweet.get('user', {}).get('followers_count', 0)
            engagement_rate = (total_engagement / follower_count * 100) if follower_count > 0 else 0
            
            return {
                **engagement,
                'total_engagement': total_engagement,
                'engagement_rate': engagement_rate
            }
            
        except Exception as e:
            logger.error(f"Error extracting engagement metrics: {str(e)}")
            return {
                'retweets': 0, 'likes': 0, 'replies': 0, 'quotes': 0,
                'total_engagement': 0, 'engagement_rate': 0
            }
    
    def _extract_media_info(self, tweet: Dict) -> Dict:
        """Extract media information from a tweet."""
        try:
            media_info = {
                'images': [],
                'videos': [],
                'gifs': [],
                'has_media': False
            }
            
            if 'media' in tweet:
                media_info['has_media'] = True
                for item in tweet['media']:
                    media_item = {
                        'url': item.get('url', ''),
                        'type': item.get('type', ''),
                        'media_url': item.get('media_url_https', '')
                    }
                    
                    if item['type'] == 'photo':
                        media_info['images'].append(media_item)
                    elif item['type'] == 'video':
                        media_info['videos'].append(media_item)
                    elif item['type'] == 'animated_gif':
                        media_info['gifs'].append(media_item)
            
            return media_info
            
        except Exception as e:
            logger.error(f"Error extracting media info: {str(e)}")
            return {'images': [], 'videos': [], 'gifs': [], 'has_media': False}
    
    def _analyze_text_content(self, text: str) -> Dict:
        """Analyze text content for various metrics."""
        try:
            # Basic text metrics
            word_count = len(text.split())
            char_count = len(text)
            avg_word_length = char_count / word_count if word_count > 0 else 0
            
            # Hashtag analysis
            hashtags = re.findall(r'#\w+', text)
            
            # Mention analysis
            mentions = re.findall(r'@\w+', text)
            
            # URL analysis
            urls = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)
            
            # Emoji analysis
            emoji_pattern = re.compile("["
                u"\U0001F600-\U0001F64F"  # emoticons
                u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                u"\U0001F680-\U0001F6FF"  # transport & map symbols
                u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                u"\U00002702-\U000027B0"
                u"\U000024C2-\U0001F251"
                "]+", flags=re.UNICODE)
            emojis = emoji_pattern.findall(text)
            
            return {
                'word_count': word_count,
                'char_count': char_count,
                'avg_word_length': avg_word_length,
                'hashtags': hashtags,
                'hashtag_count': len(hashtags),
                'mentions': mentions,
                'mention_count': len(mentions),
                'urls': urls,
                'url_count': len(urls),
                'emojis': emojis,
                'emoji_count': len(emojis)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing text content: {str(e)}")
            return {}
    
    def _extract_user_info(self, tweet: Dict) -> Dict:
        """Extract user information from a tweet."""
        try:
            user = tweet.get('user', {})
            return {
                'username': user.get('screen_name', ''),
                'name': user.get('name', ''),
                'followers_count': user.get('followers_count', 0),
                'following_count': user.get('friends_count', 0),
                'tweets_count': user.get('statuses_count', 0),
                'verified': user.get('verified', False),
                'created_at': user.get('created_at', ''),
                'description': user.get('description', ''),
                'location': user.get('location', '')
            }
            
        except Exception as e:
            logger.error(f"Error extracting user info: {str(e)}")
            return {}
    
    def _generate_comprehensive_analysis(self, analyzed_tweets: List[Dict], username: str) -> Dict:
        """Generate comprehensive analysis from analyzed tweets."""
        try:
            if not analyzed_tweets:
                return {}
            
            # Sentiment distribution
            sentiment_distribution = Counter()
            for tweet in analyzed_tweets:
                sentiment = tweet.get('sentiment', {}).get('overall_sentiment', 'neutral')
                sentiment_distribution[sentiment] += 1
            
            # Engagement analysis
            total_engagement = sum(tweet.get('engagement', {}).get('total_engagement', 0) for tweet in analyzed_tweets)
            avg_engagement = total_engagement / len(analyzed_tweets) if analyzed_tweets else 0
            
            # Most engaging tweets
            most_engaging = sorted(analyzed_tweets, 
                                 key=lambda x: x.get('engagement', {}).get('total_engagement', 0), 
                                 reverse=True)[:5]
            
            # Text analysis summary
            total_words = sum(tweet.get('text_analysis', {}).get('word_count', 0) for tweet in analyzed_tweets)
            total_hashtags = sum(tweet.get('text_analysis', {}).get('hashtag_count', 0) for tweet in analyzed_tweets)
            total_mentions = sum(tweet.get('text_analysis', {}).get('mention_count', 0) for tweet in analyzed_tweets)
            
            # Media analysis
            tweets_with_media = sum(1 for tweet in analyzed_tweets if tweet.get('media', {}).get('has_media', False))
            media_percentage = (tweets_with_media / len(analyzed_tweets) * 100) if analyzed_tweets else 0
            
            # Posting frequency analysis
            posting_frequency = self._analyze_posting_frequency(analyzed_tweets)
            
            return {
                "username": username,
                "tweets": analyzed_tweets,
                "summary": {
                    "total_tweets": len(analyzed_tweets),
                    "sentiment_distribution": dict(sentiment_distribution),
                    "total_engagement": total_engagement,
                    "average_engagement": avg_engagement,
                    "total_words": total_words,
                    "total_hashtags": total_hashtags,
                    "total_mentions": total_mentions,
                    "tweets_with_media": tweets_with_media,
                    "media_percentage": media_percentage
                },
                "insights": {
                    "most_engaging_tweets": most_engaging,
                    "posting_frequency": posting_frequency,
                    "sentiment_trends": self._analyze_sentiment_trends(analyzed_tweets),
                    "engagement_patterns": self._analyze_engagement_patterns(analyzed_tweets)
                },
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error generating comprehensive analysis: {str(e)}")
            return {}
    
    def _analyze_posting_frequency(self, tweets: List[Dict]) -> Dict:
        """Analyze posting frequency patterns."""
        try:
            date_counts = defaultdict(int)
            hour_counts = defaultdict(int)
            
            for tweet in tweets:
                created_at = tweet.get('created_at', '')
                if created_at:
                    # Parse date and hour
                    try:
                        dt = datetime.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y')
                        date_counts[dt.date().isoformat()] += 1
                        hour_counts[dt.hour] += 1
                    except ValueError:
                        continue
            
            return {
                "daily_posts": dict(date_counts),
                "hourly_distribution": dict(hour_counts),
                "most_active_hour": max(hour_counts.items(), key=lambda x: x[1])[0] if hour_counts else None
            }
            
        except Exception as e:
            logger.error(f"Error analyzing posting frequency: {str(e)}")
            return {}
    
    def _analyze_sentiment_trends(self, tweets: List[Dict]) -> Dict:
        """Analyze sentiment trends over time."""
        try:
            sentiment_by_date = defaultdict(lambda: {'bullish': 0, 'bearish': 0, 'neutral': 0})
            
            for tweet in tweets:
                created_at = tweet.get('created_at', '')
                sentiment = tweet.get('sentiment', {}).get('overall_sentiment', 'neutral')
                
                if created_at:
                    try:
                        dt = datetime.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y')
                        date_key = dt.date().isoformat()
                        sentiment_by_date[date_key][sentiment] += 1
                    except ValueError:
                        continue
            
            return dict(sentiment_by_date)
            
        except Exception as e:
            logger.error(f"Error analyzing sentiment trends: {str(e)}")
            return {}
    
    def _analyze_engagement_patterns(self, tweets: List[Dict]) -> Dict:
        """Analyze engagement patterns."""
        try:
            engagement_by_sentiment = defaultdict(list)
            engagement_by_media = {'with_media': [], 'without_media': []}
            
            for tweet in tweets:
                engagement = tweet.get('engagement', {}).get('total_engagement', 0)
                sentiment = tweet.get('sentiment', {}).get('overall_sentiment', 'neutral')
                has_media = tweet.get('media', {}).get('has_media', False)
                
                engagement_by_sentiment[sentiment].append(engagement)
                
                if has_media:
                    engagement_by_media['with_media'].append(engagement)
                else:
                    engagement_by_media['without_media'].append(engagement)
            
            # Calculate averages
            avg_engagement_by_sentiment = {
                sentiment: np.mean(engagements) if engagements else 0
                for sentiment, engagements in engagement_by_sentiment.items()
            }
            
            avg_engagement_by_media = {
                'with_media': np.mean(engagement_by_media['with_media']) if engagement_by_media['with_media'] else 0,
                'without_media': np.mean(engagement_by_media['without_media']) if engagement_by_media['without_media'] else 0
            }
            
            return {
                "by_sentiment": avg_engagement_by_sentiment,
                "by_media": avg_engagement_by_media
            }
            
        except Exception as e:
            logger.error(f"Error analyzing engagement patterns: {str(e)}")
            return {}
    
    def generate_insights_report(self, username: str) -> Dict:
        """
        Generate a comprehensive insights report for a user.
        
        Args:
            username (str): Twitter username
            
        Returns:
            Dict: Insights report
        """
        try:
            # Get the latest analysis
            analysis_result = self.analyze_user_tweets(username)
            if analysis_result["status"] != "success":
                return analysis_result
            
            analysis_data = analysis_result["analysis"]
            
            # Generate insights
            insights = {
                "username": username,
                "generated_at": datetime.now().isoformat(),
                "key_metrics": self._generate_key_metrics(analysis_data),
                "sentiment_insights": self._generate_sentiment_insights(analysis_data),
                "engagement_insights": self._generate_engagement_insights(analysis_data),
                "content_insights": self._generate_content_insights(analysis_data),
                "recommendations": self._generate_recommendations(analysis_data)
            }
            
            # Save insights report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = self.base_dir / 'insights' / f"{username}_insights_{timestamp}.json"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(insights, f, ensure_ascii=False, indent=2)
            
            return {
                "status": "success",
                "message": f"Generated insights report for {username}",
                "filepath": str(output_file),
                "insights": insights
            }
            
        except Exception as e:
            logger.error(f"Error generating insights report for {username}: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    def _generate_key_metrics(self, analysis_data: Dict) -> Dict:
        """Generate key performance metrics."""
        summary = analysis_data.get('summary', {})
        return {
            "total_tweets": summary.get('total_tweets', 0),
            "total_engagement": summary.get('total_engagement', 0),
            "average_engagement": summary.get('average_engagement', 0),
            "engagement_rate": summary.get('average_engagement', 0),
            "media_usage_percentage": summary.get('media_percentage', 0)
        }
    
    def _generate_sentiment_insights(self, analysis_data: Dict) -> Dict:
        """Generate sentiment-based insights."""
        sentiment_dist = analysis_data.get('summary', {}).get('sentiment_distribution', {})
        total_tweets = analysis_data.get('summary', {}).get('total_tweets', 0)
        
        if total_tweets == 0:
            return {}
        
        dominant_sentiment = max(sentiment_dist.items(), key=lambda x: x[1])[0] if sentiment_dist else 'neutral'
        sentiment_percentages = {
            sentiment: (count / total_tweets * 100) 
            for sentiment, count in sentiment_dist.items()
        }
        
        return {
            "dominant_sentiment": dominant_sentiment,
            "sentiment_distribution": sentiment_percentages,
            "sentiment_balance": abs(sentiment_dist.get('bullish', 0) - sentiment_dist.get('bearish', 0))
        }
    
    def _generate_engagement_insights(self, analysis_data: Dict) -> Dict:
        """Generate engagement-based insights."""
        insights = analysis_data.get('insights', {})
        engagement_patterns = insights.get('engagement_patterns', {})
        
        return {
            "best_performing_sentiment": max(
                engagement_patterns.get('by_sentiment', {}).items(), 
                key=lambda x: x[1]
            )[0] if engagement_patterns.get('by_sentiment') else None,
            "media_impact": engagement_patterns.get('by_media', {}),
            "engagement_trends": "increasing" if analysis_data.get('summary', {}).get('average_engagement', 0) > 0 else "stable"
        }
    
    def _generate_content_insights(self, analysis_data: Dict) -> Dict:
        """Generate content-based insights."""
        summary = analysis_data.get('summary', {})
        
        return {
            "content_length": {
                "average_words": summary.get('total_words', 0) / summary.get('total_tweets', 1),
                "hashtag_usage": summary.get('total_hashtags', 0),
                "mention_usage": summary.get('total_mentions', 0)
            },
            "content_strategy": {
                "media_usage": summary.get('media_percentage', 0),
                "interaction_rate": summary.get('total_mentions', 0) / summary.get('total_tweets', 1)
            }
        }
    
    def _generate_recommendations(self, analysis_data: Dict) -> List[str]:
        """Generate actionable recommendations."""
        recommendations = []
        summary = analysis_data.get('summary', {})
        insights = analysis_data.get('insights', {})
        
        # Engagement recommendations
        avg_engagement = summary.get('average_engagement', 0)
        if avg_engagement < 10:
            recommendations.append("Consider posting more engaging content to increase interaction rates")
        
        # Media recommendations
        media_percentage = summary.get('media_percentage', 0)
        if media_percentage < 30:
            recommendations.append("Increase media usage in posts to boost engagement")
        
        # Sentiment recommendations
        sentiment_dist = summary.get('sentiment_distribution', {})
        if sentiment_dist.get('neutral', 0) > 70:
            recommendations.append("Consider adding more emotional content to increase engagement")
        
        # Posting frequency recommendations
        posting_freq = insights.get('posting_frequency', {})
        if posting_freq:
            recommendations.append("Analyze optimal posting times based on engagement patterns")
        
        return recommendations 