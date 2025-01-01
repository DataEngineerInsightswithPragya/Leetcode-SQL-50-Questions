import pandas as pd

# Create the Tweets table
tweets_data = {
    "tweet_id": [1, 2],
    "content": ["Let us Code", "More than fifteen chars are here!"]
}

tweets = pd.DataFrame(tweets_data)

# Display the DataFrame
print("Tweets Table:")
print(tweets)

filter_df = tweets[tweets['content'].str.len() > 15][['tweet_id']]
print(filter_df)