import os
import tweepy
import pandas as pd

def get_twitter_client():
    """Initializes Tweepy Client using Bearer Token"""
    # Replace these with your actual keys from the dashboard!
    bearer_token = os.getenv("TWITTER_BEARER_TOKEN", "YOUR_BEARER_TOKEN_HERE")
    
    if bearer_token == "YOUR_BEARER_TOKEN_HERE":
        print("WARNING: Please set the TWITTER_BEARER_TOKEN environment variable or replace it in the script.")
        
    client = tweepy.Client(bearer_token=bearer_token)
    return client

def get_top_selling_line(base_dir):
    """Reads the top selling line from the ETL output file."""
    try:
        with open(os.path.join(base_dir, "data", "top_line.txt"), "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        print("Could not find top_line.txt. Did the ETL script run successfully?")
        return "PROTETOR SOLAR" # Fallback

def scrape_tweets(client, top_line):
    """Scrapes up to 50 recent tweets in Portuguese mentioning Boticário and the top line."""
    
    # Challenge parameter: “Boticário” and the name of the line with the most sales in month 12 of 2019
    query = f'"boticário" "{top_line.lower()}" lang:pt -is:retweet'
    print(f"Executing Twitter Query: {query}")
    
    try:
        response = client.search_recent_tweets(
            query=query,
            max_results=50,
            tweet_fields=['created_at', 'author_id']
        )
        
        tweets_data = []
        if response.data:
            for tweet in response.data:
                tweets_data.append({
                    "author_id": tweet.author_id,
                    "created_at": tweet.created_at,
                    "text": tweet.text
                })
            print(f"Retrieved {len(tweets_data)} tweets successfully.")
        else:
            print("No tweets found matching the query in the last 7 days.")
            
        return pd.DataFrame(tweets_data)
        
    except tweepy.errors.TweepyException as e:
        print(f"An error occurred during Twitter API extraction: {e}")
        print("NOTE: If you have a 'Basic' access tier, search_recent_tweets might be limited.")
        return pd.DataFrame()

def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    target_dir = os.path.join(base_dir, "data", "silver")
    
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    client = get_twitter_client()
    top_line = get_top_selling_line(base_dir)
    
    df_tweets = scrape_tweets(client, top_line)
    
    # Save the Data to a CSV (or parquet)
    if not df_tweets.empty:
        output_file = os.path.join(target_dir, "tweets_boticario.csv")
        df_tweets.to_csv(output_file, index=False)
        print(f"Saved tweets to {output_file}")
    else:
        print("No data extracted to save.")
        
if __name__ == "__main__":
    main()
