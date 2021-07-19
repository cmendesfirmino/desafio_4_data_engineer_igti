import json
from tweepy import OAuthHandler, Stream, StreamListener
from datetime import datetime as dt

#read access data
json_file = open('keys.json')
keys = json.load(json_file)

CONSUMER_KEY = keys['API_KEY']
CONSUMER_SECRET = keys['API_SECRET_KEY']  
ACCESS_TOKEN = keys['ACCESS_TOKEN']
ACCESS_TOKEN_SECRET = keys['ACCESS_TOKEN_SECRET']

#writing data to txt
today_date = dt.now().strftime('%Y%m%d%H%M%S')
out = open(f"collected_tweets{today_date}.txt", "w")

#implements Mylistener class
class MyListener(StreamListener):
    def on_data(self, data):
        item_string = json.dumps(data)
        out.write(item_string + "\n")
        return True
    
    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    l = MyListener()
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    stream = Stream(auth, l)
    stream.filter(track=["Bolsonaro"])