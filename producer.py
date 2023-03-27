import snscrape.modules.twitter as sntwitter
from kafka import KafkaProducer
import json, re, string
from time import sleep
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

wn = WordNetLemmatizer()
listOfStopWords = stopwords.words('english')

def cleanText(text):
    if text:   
        lines = text.lower().split('\n')
        lines = [' '.join(re.split('\W+', line.strip())) for line in lines if not line.startswith('http')]
        lines = [''.join([char for char in line if char not in string.punctuation]) for line in lines]
        return ''.join(' '.join(wn.lemmatize(word) for word in line.split(' ') if word not in listOfStopWords and not word.isnumeric()) for line in lines)
    else:
        return ''

colums2 = ['Date', 'User', 'Tweet']
data = []

query = '#covid19'
limit = 100
tweets = []

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))

for tweet in sntwitter.TwitterSearchScraper(query).get_items():
    if tweet.lang == 'en':
        location = {}
        user = {'username': tweet.user.username}
        if tweet.coordinates and tweet.coordinates.longitude and tweet.coordinates.latitude:
            location = {'longitude': tweet.coordinates.longitude, 'latitude': tweet.coordinates.latitude}
        if tweet.user:
            user = {'username': tweet.user.username, 'display_name': tweet.user.displayname, 'location': tweet.user.location, 'description': tweet.user.description, 'followers': tweet.user.followersCount, 'friends': tweet.user.friendsCount, 'id': tweet.user.id, 'verified': tweet.user.verified}
        data = {'date': tweet.date.strftime("%Y-%m-%d %H:%M:%S"),
                'user': user,
                'tweet': cleanText(tweet.content),
                'tweet_id': tweet.id,
                'location': location,
                'source': tweet.sourceLabel,
                'like_count': tweet.likeCount,
                'view_count': tweet.viewCount}
        tweets.append(data)
        producer.send('tweet_stream', data)
        print('Length of Data',len(tweets))
        if len(tweets) == limit:
            break
        sleep(1)