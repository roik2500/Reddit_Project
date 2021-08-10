import datetime
import pymongo
import re
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

    def draw_sentiment_time(self,posts):
        dates = []
        posts_id = []
        for x in posts.find({}):
            #ext_post = x['selftext']
            #print(x['selftext'])
            dates.append(x['created_utc'][0])
            posts_id.append(str(x['id']))
            # sentiment = api.get_post_sentiment(post)
            # print(sentiment)
            #api.sentiment_time(dates)

        # convert the dates to datetime
        x_values = [datetime.datetime.strptime(d, "%Y-%m-%d").date() for d in dates]
        plt.plot(x_values, posts_id)
        return plt.show()

if __name__ == '__main__':
    api = RedditClient()
    posts = api.get_posts_from_mongodb()
    api.draw_sentiment_time(posts)





