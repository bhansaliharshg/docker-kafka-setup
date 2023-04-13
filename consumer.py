from kafka import KafkaConsumer
import json
from textblob import TextBlob
from geopy.geocoders import Nominatim
import folium

map = folium.Map(location=[0,0], zoom_start=3)

geolocator = Nominatim(user_agent="tweet_steam_consumer")

values = []
searchedTerm = ''

consumer = KafkaConsumer(
    'tweet_stream',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    message = message.value
    if len(message) == 1 and 'search_trigger' in message:
        print('================= New Search Occured =================')
        searchedTerm = str(message['search_trigger']).replace('[^a-zA-Z0-9 ]', '')
    elif len(message) == 1 and 'search_end' in message:
        print('================= Search End =================')
        map.save(searchedTerm+'-location.html')
        with open(searchedTerm+'.html', 'w') as file:
            file.write('<!DOCTYPE html>')
            file.write('\n<html>')
            file.write('\n<head>'+
                       '\n\t<title>'+searchedTerm+' - Results</title>'+
                       '\n\t<meta charset="utf-8">'+
                       '\n\t<meta name="viewport" content="width=device-width, initial-scale=1">'+
                       '\n\t<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css">'+
                       '\n\t<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.6.3/jquery.min.js"></script>'+
                       '\n\t<script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script>'+
                       '\n</head>')
            file.write('\n<body class=\'container\'>')
            file.write('\n<div class=\'jumbotron text-center\'> <h2>'+searchedTerm+' - Results</h2> </div>')
            file.write('\n\t<table class=\'table table-striped table-bordered\'>')
            file.write('\n\t\t<thead>')
            file.write('\n\t\t<tr>')
            file.write('\n\t\t\t<th>ID</th>')
            file.write('\n\t\t\t<th>User</th>')
            file.write('\n\t\t\t<th>Tweet</th>')
            file.write('\n\t\t\t<th>Length</th>')
            file.write('\n\t\t\t<th>Date</th>')
            file.write('\n\t\t\t<th>Likes</th>')
            file.write('\n\t\t\t<th>Retweet</th>')
            file.write('\n\t\t\t<th>Tweet Source</th>')
            file.write('\n\t\t\t<th>Sentiment</th>')
            file.write('\n\t\t\t<th>Location</th>')
            file.write('\n\t\t</tr>')
            file.write('\n\t\t</thead>')
            file.write('\n\t\t<tbody>')
            for value in values:
                file.write('\n\t\t<tr>')
                file.write('\n\t\t\t<td><a target=\'_blank\' href=\'https://twitter.com/'+value['user']['username']+'/status/'+str(value['tweet_id'])+'\'>'+str(value['tweet_id'])+'</a></td>')
                file.write('\n\t\t\t<td>'+value['user']['display_name']+'</td>')
                file.write('\n\t\t\t<td>'+value['tweet']+'</td>')
                file.write('\n\t\t\t<td>'+str(len(value['tweet']))+'</td>')
                file.write('\n\t\t\t<td>'+value['date']+'</td>')
                file.write('\n\t\t\t<td>'+str(value['favourite_count'])+'</td>')
                file.write('\n\t\t\t<td>'+str(value['retweet_count'])+'</td>')
                file.write('\n\t\t\t<td>'+value['source']+'</td>')
                file.write('\n\t\t\t<td>'+str(value['sentiment'])+'</td>')
                file.write('\n\t\t\t<td>'+value['address'] if 'address' in value else ''+'</td>')
                file.write('\n\t\t</tr>')
            file.write('\n\t\t</tbody>')
            file.write('\n\t</table>')
            file.write('\n</body>')
            file.write('\n</html>')
            values = []
    else:
        values.append(message)
        sentiment = TextBlob(message['clean_tweet']).sentiment.polarity
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
    print(message)