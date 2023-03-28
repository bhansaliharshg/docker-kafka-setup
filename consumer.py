from kafka import KafkaConsumer
import json
from textblob import TextBlob
from geopy.geocoders import Nominatim
import folium

map = folium.Map(location=[0,0], zoom_start=3)

geolocator = Nominatim(user_agent="tweet_steam_consumer")

consumer = KafkaConsumer(
    'tweet_stream',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    message = message.value
    if len(message) == 1 and message['search_trigger']:
        print('================= New Search Occured =================')
    else:
        sentiment = TextBlob(message['tweet']).sentiment.polarity
        if sentiment > 0:
            message['sentiment'] = 1
        elif sentiment < 0:
            message['sentiment'] = -1
        else:
            message['sentiment'] = 0
        if message['location']:
            coordinates = str(message['location']['latitude']) + ', ' + str(message['location']['longitude'])
            location = geolocator.reverse(coordinates)
            tooltip = 'Location'
            if location and location.address:
                message['address'] = location.address
                tooltip = message['address']
            if sentiment < 0:
                folium.Marker(location=[message['location']['latitude'], message['location']['longitude']], tooltip=tooltip, icon=folium.Icon(color='red')).add_to(map)
            elif sentiment > 0:
                folium.Marker(location=[message['location']['latitude'], message['location']['longitude']], tooltip=tooltip, icon=folium.Icon(color='green')).add_to(map)
            else:
                folium.Marker(location=[message['location']['latitude'], message['location']['longitude']], tooltip=tooltip).add_to(map)
        map.save('index.html')
    print(message)