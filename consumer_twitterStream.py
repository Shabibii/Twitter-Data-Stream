# List of all import libraries
import _thread
import re
import sys
import time
import warnings
from collections import Counter
from itertools import chain
from json import loads
from os import path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from kafka import KafkaConsumer
from nltk import stem
from nltk.corpus import stopwords
from pandas.core.frame import DataFrame
from PIL import Image
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer
from wordcloud import WordCloud

sys.path.append(".")
from producer_twitterStream import StreamerProducer

################################################################################################################################
# CONSUMER CLASS: Uses Kafka Consumer to consume from the topic 'ukStream' in the Kafka broker. 
# This class analyses the messages in the topic and performs several operations. 
# The main operations involve:
# - Regular expression to find hashtags (#), remove urls, remove users (@), etc.
# - NLTK package to remove stopwords and stem the words (ML technique)
# - LDA to present the top 10, 30 or 50 bag of words, each bag forms a topic (human interpretation) (ML technique)
################################################################################################################################

class Consumer:
    ############################################################################################################################
    # KAFKA CONSUMER: Connects to KafkaConsumer API and creates instance of KafkaConsumer object
    ############################################################################################################################ 
    
    consumer = KafkaConsumer(
        'twitterStream_EN',
        bootstrap_servers=['localhost:9092'], 
        consumer_timeout_ms=3000, # wait a maximum of 3 seconds for data
        auto_offset_reset='latest', # start from latest messages
        enable_auto_commit=True,
        group_id='my-tweets', # define consumer group name
        value_deserializer=lambda x: loads(x.decode('utf-8'))) # decode utf-8 data: value for deserialization
    
    ############################################################################################################################
    # RELEVANT FUNCTIONS AND VARIABLESObtai
    ############################################################################################################################
    
    popular_hashtags_set = () # define variable for popular hastag
    tf_feature_names = () # define variable for popular hastag
    trending_tweets = [] # define variable for trending tweets   
    trending_tweets_clean = [] # define variable for trending tweets for Word Cloud
    my_stopwords = stopwords.words('english') # get stopwords from NLTK package    
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@' # some punctuations
    ignore_words = ['rt', 'im', 'like', 'people', 'dont', 'us' , 'really', 'cant', 'yes', 
        'please', 'amp', '#', 'one', 'u', 'get', 'want', 'thats', 'said', 'time' 'thats', 'go',
        'good', 'thank', 'time', 'let', 'see', 'follow', 'watch'] # words to ignore in tweets  
    #word_rooter = stem.snowball.PorterStemmer(ignore_stopwords=False).stem # only keep stem of words  
       
    # Function: finds hashtags in a text
    def find_hashtags(self, tweet):
        return re.findall('(#[A-Za-z]+[A-Za-z0-9-_]+)', tweet)
    
    # Function: Removes url in a text 
    def remove_url(self, text):
        return " ".join(re.sub("([^0-9A-Za-z-@# \t])|(\w+:\/\/\S+)", "", text).split())

    # Function: Removes user (@..) from text 
    def remove_users(self, tweet):
        '''Takes a string and removes retweet and @user information'''
        tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', tweet) # remove retweet
        tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', tweet) # remove tweeted at
        return tweet 

    # Function: Main cleaning function 
    def clean_tweet(self, tweet, bigrams=False):
        tweet = self.remove_url(tweet) # remove url
        tweet = self.remove_users(tweet) # remove users
        tweet = tweet.lower() # lower case
        tweet = re.sub('['+self.my_punctuation + ']+', ' ', tweet) # strip punctuation
        tweet = re.sub('\s+', ' ', tweet) # remove double spacing
        tweet = re.sub('([0-9]+)', '', tweet) # remove numbers
        # Remove stopwords and words to ignore
        tweet_token_list = [word for word in tweet.split(' ') if word not in self.my_stopwords and word not in self.ignore_words]
        #tweet_token_list = [self.word_rooter(word) if '#' not in word else word for word in tweet_token_list] # apply word rooter
        if bigrams: # keep words together if bigram
            tweet_token_list = tweet_token_list+[tweet_token_list[i]+'_'+tweet_token_list[i+1]
                for i in range(len(tweet_token_list)-1)]
        tweet = ' '.join(tweet_token_list) # 'tweet' after changes
        return tweet

    # Function: Returns data obtained from LDA model as DataFrame
    def get_topic_dataframe(self, model, feature_names, length):
        topic_dict = {}
        for topic_index, topic in enumerate(model.components_):
            topic_dict["Words Topic #%d" % (topic_index+1)]= ['{}'.format(feature_names[i])
                for i in topic.argsort()[:-length - 1:-1]]
            topic_dict["Words Weight #%d" % (topic_index+1)]= ['{:.1f}'.format(topic[i])
                for i in topic.argsort()[:-length - 1:-1]]
        return pd.DataFrame(topic_dict)
        
    ############################################################################################################################
    # ANALYSE TEXT: Get relevant information to specify trending topics, hasthags and tweets
    # It gets the trending topics and trending hastags via the tweet content, and the trending tweets
    # via the sum of several numerical tweet attributes (retweets, replies, quotes, favourites)
    ############################################################################################################################ 

    def analyse_text(self, topics_or_tweets, len):
        # Define variables to hold specific tweet data
        tweets = []
        retweets = []
        replies = []
        quotes = []
        favourites = []
        # Iterate
        for message in self.consumer: # for each message in consumer            
            tweet_object = loads(message.value) # extract the value  

            # Get text field, reply count, quote count, retweet count and favorites count from tweet 
            f_text=tweet_object.get('text') 
            reply_count = tweet_object['reply_count'] 
            quote_count = tweet_object['quote_count'] 
            retweet_count = tweet_object['retweet_count'] 
            favorite_count = tweet_object['favorite_count'] 

             # Check if there is a fuller version of the tweet text field
            if 'extended_tweet' in tweet_object:        
                # Store the extended tweet as the text field 'f_text'
                f_text = tweet_object['extended_tweet']['full_text']  

            # If 'retweeted_status' exists, take the numerical values from there
            if 'retweeted_status' in tweet_object:        
                # Replace the values with the new ones
                reply_count = tweet_object['retweeted_status']['reply_count']   
                quote_count = tweet_object['retweeted_status']['quote_count'] 
                retweet_count = tweet_object['retweeted_status']['retweet_count']   
                favorite_count = tweet_object['retweeted_status']['favorite_count'] 

            # If 'quoted_status' exists, take the numerical values from there
            if 'quoted_status' in tweet_object:        
                # Replace the values with the new ones
                reply_count = tweet_object['quoted_status']['reply_count']   
                quote_count = tweet_object['quoted_status']['quote_count'] 
                retweet_count = tweet_object['quoted_status']['retweet_count']   
                favorite_count = tweet_object['quoted_status']['favorite_count']     

            tweets.append(f_text) # append text fields to 'tweets' 
            replies.append(reply_count) # append reply counts to 'replies'
            quotes.append(quote_count) # append quote counts to 'quotes'
            retweets.append(retweet_count) # append retweet counts to 'retweets'            
            favourites.append(favorite_count) # append favorite counts to 'favorites'
            
            # Print each tweet's text indicating that consumer is active
            print('FULL TEXT: {}'.format(f_text)) 
            print("\n----------------------------------------\n")

        # Fill the 'df' dataframe with relevant program data. 
        df = pd.DataFrame(tweets) # fill with 'tweets'
        df.columns = ['tweet'] # name first column
        df['clean_tweet'] = df.tweet.apply(self.clean_tweet) # append column 'clean_tweet'; load clean tweet after function (clean_tweet)
        df['tweet_hashtag'] = df.tweet.apply(self.find_hashtags) # append column 'tweet_hashtag': to dataframe 'df'; load found hashtags after function find_hashtags (clean_tweet)
        df['reply_c'] = replies # add column 'reply_c' to dataframe; load replies
        df['quote_c'] = quotes # add column 'quote_c' to dataframe; load quotes
        df['retweet_c'] = retweets # add column 'retweet_c' to dataframe; load retweets
        df['favorites_c'] = favourites # add column 'quote_c' to dataframe; load quotes
        column_list = list(df) # list of columns     
        df['total_c'] = df[column_list].sum(axis=1) # sum up numerical values  

        pd.set_option("display.max_rows", None, "display.max_columns", None) # prints full dataframe in terminal   
        
        ########################################################################################################################
        # POPULAR HASHTAGS: Gets popular hashtags based on their appearance count
        ########################################################################################################################
        
        # Remove empty rows and put hashtags together in list
        hashtags_list_df = df.loc[df.tweet_hashtag.apply(lambda hashtags_list: hashtags_list !=[]),['tweet_hashtag']]
        # Create new dataframe splitting hashtags from same tweet
        flattened_hashtags_df = pd.DataFrame([hashtag for hashtags_list in hashtags_list_df.tweet_hashtag
            for hashtag in hashtags_list],columns=['all_hashtags'])
        # Number of unique hashtags
        flattened_hashtags_df['all_hashtags'].unique().size
        # Occurence count of each hashtag
        popular_hashtags = flattened_hashtags_df.groupby('all_hashtags').size().reset_index(name='counts')\
        .sort_values('counts', ascending=False).reset_index(drop=True)
        # Isolate hashtags that appear at least 'min_appear' times
        min_appear = 10
        # Obtain popular hashtags
        popular_hashtags_set = set(popular_hashtags[popular_hashtags.counts>=min_appear]['all_hashtags'])

        ########################################################################################################################
        # TRENDING TOPICS: Use of ML-technique: LDA. Produces bags of words, each bag indicating a topic (human interpretation)
        # Additionally, removes words that appear in >90% of the tweets or in less than 10 tweets 
        ########################################################################################################################

        if topics_or_tweets:
            # Transforms the text to vector form (removes words that appear >90% or less than 5 times)
            vectorizer = CountVectorizer(max_df=0.9, min_df=5, token_pattern='\w+|\$[\d\.]+|\S+')
            term_frequency = vectorizer.fit_transform(df.clean_tweet).toarray() # transform
            # Get words that represent the colums
            self.tf_feature_names = vectorizer.get_feature_names()

            number_of_topics = 10 
            model = LatentDirichletAllocation(n_components=number_of_topics, random_state=0)
            model.fit(term_frequency)           
            self.top_topic_table = self.get_topic_dataframe(model, self.tf_feature_names, len) # display top 10/30/50
            print(popular_hashtags.head(10))
            return self.top_topic_table 
        else:
            sorted_df = df.sort_values(by=['total_c'], ascending=False).head(n=len)                       
            dropped_df = sorted_df.drop_duplicates('tweet')            
            self.trending_tweets = dropped_df.tweet # obtain trending tweets
            self.trending_tweets_clean = dropped_df.clean_tweet # obtain trending tweets clean for Word Cloud
            return self.trending_tweets

    ######################################################################################################################
    # WORD CLOUD VISUALISATION
    ######################################################################################################################
    
    def generate_wordcloud(self, topics_or_tweets):    
        warnings.filterwarnings("ignore")
        
        twitter_mask = np.array(Image.open("Visual/tlogo.png")) # add mask (form of word cloud)
        wordcloud = WordCloud(background_color="white", max_words=100, mask=twitter_mask, contour_width=2, contour_color='powderblue')
        
        # Check if word cloud of topics or tweets
        if topics_or_tweets:            
            wordcloud_words = self.tf_feature_names                                 
        else:            
            wordcloud_words = self.trending_tweets_clean         
        
        text_of_all = " ".join(tweet for tweet in wordcloud_words) # join all relevant text       
        wordcloud.generate(text_of_all) # generate word       
        wordcloud.to_file("Visual/my_word_cloud.png")  # save the image in the Visual folder:

        # Show
        plt.figure() #(figsize=[20,10])
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis("off")
        plt.show()

    def __init__(self):
        pass
       
# Define a function for the thread (producer)
def start_producer(run_time):    
    x = StreamerProducer() # Run for two minutes
    x.run(run_time)
    #time.sleep(60 * run_time) # wait 5 minutes before producing again

# Define a function for the thread (consumer)
def start_streaming():    
    run_time = int(input('Would you like to stream data each 10, 30 or 60 minutes? (Answer: 10 / 30 / 60) \u2192  '))
    length = int(input('How long would you like the top list to be? (Answer: 10, 20, 50) \u2192  '))
    topics_or_tweets = input('Would you like to see the trending topics (1) or tweets (2)? (Answer: 1 / 2) \u2192  ') == '1' 

    _thread.start_new_thread( start_producer, (run_time, ) )
    print("Producer started. Consumer will start too")

    twitterConsumer = Consumer()
    twitterConsumer.length = length
       
    print("Consumer Started")   
    print("Close program using Ctrl + C\n\n")
    
    if topics_or_tweets:
        # Display popular tweets
        print(twitterConsumer.analyse_text(True, length))            
    else:
        # View Popular trends
        print(twitterConsumer.analyse_text(False, length))

    # Pause the consumer
    twitterConsumer.consumer.poll()

    # Generate word cloud    
    twitterConsumer.generate_wordcloud(topics_or_tweets)    
 
    while 1:
        answer = input("Enter yes or no: ") 
        if answer == "yes": 
            start_streaming()       
        elif answer == "no": 
            print('Closing Game')
            break
        else:
            print("Please enter yes or no.")            

    print("Good bye")

def main(): 
    # Run producer thread
    try:
        start_streaming()         
    except Exception as e: 
        print(e)
    # while 1: # kreep thread running
    #     pass
   
if __name__ == "__main__":
    main()



