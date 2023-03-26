import snscrape.modules.twitter as sntwitter
from kafka import KafkaProducer
import json
from time import sleep

colums2 = ['Date', 'User', 'Tweet']
data = []

query = '#covid19'
limit = 100

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))

for tweet in sntwitter.TwitterSearchScraper(query).get_items():
    if tweet.lang == 'en':
        location = {}
        if tweet.coordinates and tweet.coordinates.longitude and tweet.coordinates.latitude:
            location = {'longitude': tweet.coordinates.longitude, 'latitude': tweet.coordinates.latitude}
        data = {'date': tweet.date.strftime("%Y-%m-%d %H:%M:%S"), 'user': tweet.user.username, 'tweet': tweet.content, 'tweet_id': tweet.id, 'location': location, 'source': tweet.sourceLabel}
        print('Sending', data)
        producer.send('tweet_stream', data)    
        if len(data) == limit:
            break
        sleep(2)