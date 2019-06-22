import json
import tweepy

from kafka import KafkaProducer

import twitter_config


class TwitterStreamListener(tweepy.StreamListener):
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.tweets = []

    def on_data(self, data):
        text = json.loads(data)[u'text']
        self.producer.send('tweets', text)
        self.producer.flush()
        print(text)

    def on_error(self, status_code):
        if status_code == 420:
            return False


def initialize():
    consumer_key = twitter_config.consumer_key
    consumer_secret = twitter_config.consumer_secret
    access_token = twitter_config.access_token
    access_secret = twitter_config.access_secret

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)

    stream = TwitterStreamListener()
    twitter_stream = tweepy.Stream(auth=api.auth, listener=stream)
    twitter_stream.filter(track=['python, pyspark, spark'], languages=['en'])


if __name__ == "__main__":
    initialize()