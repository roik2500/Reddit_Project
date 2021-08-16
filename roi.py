import datetime
import pymongo
import re
import pandas as pd
import seaborn as sns
from numpy import sort
from textblob import TextBlob
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


class RedditClient(object):

    def get_post_sentiment(self, tweet):
        # create TextBlob object of passed tweet text
        analysis = TextBlob(self.clean_tweet(tweet))
        print("Sentiment:")
        print("The polarity is: {}".format(analysis.sentiment.polarity))
        print("The subjectivity is: {}".format(analysis.sentiment.subjectivity))

        # set sentiment
        if analysis.sentiment.polarity > 0:
            return 'positive'
        elif analysis.sentiment.polarity == 0:
            return 'neutral'
        else:
            return 'negative'

    def clean_tweet(self, tweet):
        '''
            Utility function to clean tweet text by removing links, special characters
            using simple regex statements.
            '''
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

    def get_posts_from_mongodb(self):
        '''
        This function is return the posts from mongoDB
        :return:
        '''
        myclient = pymongo.MongoClient("mongodb+srv://roi:1234@redditdata.aav2q.mongodb.net/")
        mydb = myclient["reddit"]
        posts = mydb["deletedData"]
        return posts

    def draw_sentiment_time(self, posts):
        data = []
        for x in posts.find({}):
            temp = self.get_post_sentiment(x['selftext'])
            year = int(datetime.datetime.strptime(x['created_utc'][0], "%Y-%m-%d").date().year)
            data.append([year, temp])
        # x_values = [str(datetime.datetime.strptime(d, "%Y-%m-%d").date().month) for d in dates]

        hist = pd.DataFrame(data, columns=['Year', 'Sentiment'])
        with sns.axes_style('white'):
            g = sns.factorplot("Year", data=hist, aspect=1.7, kind='count', hue='Sentiment', order=range(2017, 2023))

        return plt.show()


# if __name__ == '__main__':
#     api = RedditClient()
#     posts = api.get_posts_from_mongodb()
#     api.draw_sentiment_time(posts)
