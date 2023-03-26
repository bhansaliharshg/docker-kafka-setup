from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'tweet_stream',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    message = message.value
    print(message)