from kafka import KafkaConsumer
import json
from textblob import TextBlob

consumer = KafkaConsumer(
    'tweet_stream',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    message = message.value
    sentiment = TextBlob(message['tweet']).sentiment.polarity
    if sentiment > 0:
        message['sentiment'] = 1
    elif sentiment < 0:
        message['sentiment'] = -1
    else:
        message['sentiment'] = 0
    print(message)