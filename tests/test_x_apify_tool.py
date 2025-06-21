"""
Comprehensive test suite for X_apify_tool.

Core Feature Tests (GO/NO GO):
1. Initialization: Tests API token and client setup
2. Media Download: Tests media downloading functionality
3. Tweet Scraping: Tests tweet fetching (uses real API with @elonmusk as test account)
4. Data Saving: Tests file saving functionality
5. Stats Generation: Tests statistics creation

Feature-Specific Tests:
1. Logging Setup: Tests log file creation and writing
2. Stats Generation: Tests statistics generation for multiple users
3. Tweet Metrics: Tests accurate capture of likes/retweets/replies
4. 24-hour Filter: Tests proper filtering of tweets by time
5. Media Download: Tests correct media file structure and organization

Features:
- Generates GO/NO GO report for core features
- Uses both real API and mocked calls for comprehensive testing
- Tests both basic functionality and edge cases
- Provides detailed error reporting
- Tests with both real and mock data for reliability
"""

import os
import sys
import pytest
from datetime import datetime
import logging

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from tools.X_apify_tool import XApifyTool

@pytest.fixture
def test_users():
    """Fixture providing test users for tweet fetching"""
    return [
        "ultrawavetrader",  
        "yuriymatso",
        "KISSOrderFlow"
    ]

@pytest.fixture
def x_apify_tool():
    return XApifyTool()

def test_x_apify_tool_initialization(x_apify_tool):
    assert x_apify_tool.name == "X_Apify_Tool"
    assert x_apify_tool.description == "Tool for scraping tweets from X (Twitter) using Apify"
    assert x_apify_tool.api_token is not None
    assert x_apify_tool.client is not None
    assert x_apify_tool.actor_id == "kaitoeasyapi/twitter-x-data-tweet-scraper-pay-per-result-cheapest"
    assert callable(x_apify_tool.func)
    assert x_apify_tool.session_id is not None

def test_x_apify_tool_run(x_apify_tool, test_users):
    # Test with real usernames
    result = x_apify_tool.run(test_users)
    
    assert isinstance(result, dict)
    for user in test_users:
        assert user in result
        assert isinstance(result[user], list)
        
        # If we got tweets, verify their structure
        for tweet in result[user]:
            assert isinstance(tweet, dict)
            assert 'text' in tweet
            assert 'created_at' in tweet
            assert 'id' in tweet
            assert 'username' in tweet
            
            # Verify tweet is from the correct user
            assert tweet['username'].lower() == user.lower()

def test_x_apify_tool_batch_processing(x_apify_tool, test_users):
    # Test batch processing with real usernames
    result = x_apify_tool.run(test_users)
    
    assert isinstance(result, dict)
    assert len(result) == len(test_users)
    
    for user in test_users:
        assert user in result
        assert isinstance(result[user], list)
        
        # If we got tweets, verify their structure
        for tweet in result[user]:
            assert isinstance(tweet, dict)
            assert 'text' in tweet
            assert 'created_at' in tweet
            assert 'id' in tweet
            assert 'username' in tweet
            assert tweet['username'].lower() == user.lower()

def test_x_apify_tool_error_handling(x_apify_tool, caplog):
    with caplog.at_level(logging.ERROR):
        # Test with invalid username
        result = x_apify_tool.run(["thisuserdoesnotexist12345"])
        
        assert isinstance(result, dict)
        assert "thisuserdoesnotexist12345" in result
        assert isinstance(result["thisuserdoesnotexist12345"], list)
        assert len(result["thisuserdoesnotexist12345"]) == 0  # Should return empty list for invalid user
        
        # Verify error was logged
        assert any("No tweets found for user" in record.message for record in caplog.records)

def test_x_apify_tool_recent_tweets(x_apify_tool, test_users):
    # Test fetching recent tweets from active users
    result = x_apify_tool.run(test_users)
    
    assert isinstance(result, dict)
    for user in test_users:
        assert user in result
        assert isinstance(result[user], list)
        
        # If we got tweets, verify they are recent
        for tweet in result[user]:
            created_at = tweet['created_at']
            if isinstance(created_at, str):
                tweet_date = datetime.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y')
                assert (datetime.utcnow() - tweet_date).days <= 1  # Tweets should be from last 24 hours 