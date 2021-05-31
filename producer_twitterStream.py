import sys
import time
from json import dumps

from kafka import KafkaProducer
from tweepy import API, OAuthHandler, Stream, StreamListener

# Enter twitter developer account keys
consumer_key = 'dtfR91bG6l364jYHzNQmCtpKy'
consumer_secret = '6V7JjG9oTmuk5Bx9pDklPAGOm80c2fjmeIN4QgEzSkNmRapLOY'
access_token = '1252194626179604480-9pN09NR2AcleDevPNZCCFlG3MaaLrK'
access_token_secret = 'Xj70fFDOHuW9ttuvPGNX3PEAo5DjvXtJSRg7hCmnJyXOM'

# Consumer key authentication(consumer_key,consumer_secret can be collected from our twitter developer profile)
auth = OAuthHandler(consumer_key, consumer_secret)

# Access key authentication(access_token,access_token_secret can be collected from our twitter developer profile)
auth.set_access_token(access_token, access_token_secret)

# Set up the API with the authentication handler
api = API(auth, wait_on_rate_limit=True)

# Create the producer, specify port and ip address of kafka server (broker address), and type of encoding: utf-8
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

class StdOutlistener(StreamListener):
    def __init__(self, time_limit):
        self.start_time = time.time() # start time of stream
        self.limit = time_limit # time limit        
        super(StdOutlistener, self).__init__() 
    
    def on_data(self, data):
        # publish the data to the kafka cluster
        if (time.time() - self.start_time) < self.limit:
            producer.send("twitterStream_EN", value=data) # topic 'ukStream' 
            return True
        else:
            return False       

    def on_error(self, status):
        print(status)

class StreamerProducer:
    def run(self, run_time):
        try:
            # Instantiate the stream object
            l = StdOutlistener(time_limit = 60 * run_time) # stream for run_time minutes
            # Begin collecting data
            stream = Stream(auth, l)
            # Filter streams in English language
            # .. Also, check if tweets are processed quickly enough (using the stall_warnings parameter).
            stream.sample(languages=["en"], stall_warnings=True)             
        except:
            e =  sys.exc_info()[0]
            print(e)
            pass




