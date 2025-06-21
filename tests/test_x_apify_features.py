"""
Detailed feature-specific tests for X_apify_tool.

Tests specific features and edge cases:
1. Logging Setup: Tests log file creation and writing
2. Stats Per User: Tests statistics generation for multiple users
3. Tweet Metrics: Tests accurate capture of likes/retweets/replies
4. 24-hour Filter: Tests proper filtering of tweets by time
5. Media Download: Tests correct media file structure and organization

Features:
- Uses mocking for API calls to test specific scenarios
- Tests edge cases and specific features
- More granular assertions than basic tests
- Focuses on detailed functionality
- Tests with mock data to ensure consistent results

This test suite complements test_x_apify_tool.py by providing more
detailed testing of specific features and edge cases, using mocked
data to ensure consistent and reliable test results.
"""

import unittest
import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock
from tools.X_apify_tool import XApifyTool

class TestXApifyFeatures(unittest.TestCase):
    def setUp(self):
        if not os.getenv('APIFY_API_TOKEN'):
            os.environ['APIFY_API_TOKEN'] = 'test_token'
        self.tool = XApifyTool()
        
        # Create test directories
        self.log_dir = Path('logs')
        self.media_dir = Path('media')
        self.tweets_dir = Path('tweets')
        
        for dir_path in [self.log_dir, self.media_dir, self.tweets_dir]:
            dir_path.mkdir(exist_ok=True)

    def test_logging_setup(self):
        """Test that logging is properly set up"""
        log_files = list(self.log_dir.glob(f"x_apify_{datetime.now().strftime('%Y%m%d')}.log"))
        self.assertTrue(len(log_files) > 0, "Log file should exist")
        self.assertTrue(log_files[0].stat().st_size > 0, "Log file should not be empty")

    def test_stats_per_user(self):
        """Test that stats are generated per user"""
        test_tweets = [
            {
                'username': 'user1',
                'text': 'Test tweet 1',
                'created_at': datetime.now().isoformat(),
                'likes': 10,
                'retweets': 5,
                'replies': 2,
                'media': []
            },
            {
                'username': 'user2',
                'text': 'Test tweet 2',
                'created_at': datetime.now().isoformat(),
                'likes': 20,
                'retweets': 8,
                'replies': 3,
                'media': []
            }
        ]
        
        stats = self.tool.create_stats_summary(test_tweets)
        self.assertIn('user1', stats)
        self.assertIn('user2', stats)
        self.assertEqual(stats['user1']['total_tweets'], 1)
        self.assertEqual(stats['user2']['total_tweets'], 1)

    def test_tweet_metrics(self):
        """Test that tweet metrics are correctly captured"""
        with patch('tools.X_apify_tool.ApifyClient') as mock_client:
            mock_dataset = MagicMock()
            mock_dataset.list_items.return_value.items = [{
                'full_text': 'Test tweet',
                'created_at': datetime.now().isoformat(),
                'favorite_count': 100,
                'retweet_count': 50,
                'reply_count': 25
            }]
            
            mock_client.return_value.actor.return_value.call.return_value = {
                'id': 'test_run',
                'defaultDatasetId': 'test_dataset'
            }
            mock_client.return_value.dataset.return_value = mock_dataset
            
            result = self.tool.run(['test_user'])
            tweet = result['test_user'][0]
            
            self.assertEqual(tweet['likes'], 100)
            self.assertEqual(tweet['retweets'], 50)
            self.assertEqual(tweet['replies'], 25)

    def test_24hour_filter(self):
        """Test that only tweets from last 24 hours are included"""
        old_tweet = {
            'full_text': 'Old tweet',
            'created_at': (datetime.now() - timedelta(days=2)).isoformat()
        }
        new_tweet = {
            'full_text': 'New tweet',
            'created_at': datetime.now().isoformat()
        }
        
        with patch('tools.X_apify_tool.ApifyClient') as mock_client:
            mock_dataset = MagicMock()
            mock_dataset.list_items.return_value.items = [old_tweet, new_tweet]
            
            mock_client.return_value.actor.return_value.call.return_value = {
                'id': 'test_run',
                'defaultDatasetId': 'test_dataset'
            }
            mock_client.return_value.dataset.return_value = mock_dataset
            
            result = self.tool.run(['test_user'])
            self.assertEqual(len(result['test_user']), 1)
            self.assertEqual(result['test_user'][0]['text'], 'New tweet')

    @patch('tools.X_apify_tool.requests.get')
    def test_media_download(self, mock_get):
        """Test that media is downloaded to correct directory structure"""
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b"fake_media_content"]
        mock_get.return_value = mock_response
        
        test_date = datetime.now()
        result = self.tool.download_media(
            "https://test.com/image.jpg",
            "test_user",
            test_date
        )
        
        expected_dir = self.media_dir / test_date.strftime('%m-%d-%Y') / "test_user"
        self.assertTrue(expected_dir.exists())
        self.assertTrue(result.startswith(str(expected_dir)))
        self.assertTrue(Path(result).exists())

if __name__ == '__main__':
    unittest.main(verbosity=2) 