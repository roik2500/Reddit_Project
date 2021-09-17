import datetime
import re
from textblob import TextBlob
import matplotlib.pyplot as plt
from afinn import Afinn
from tqdm import tqdm


class Sentiment:
    def __init__(self):
        self.text_per_month = {}
        self.sentiment_per_month = {}  ##{'1': {"positive":5,'netural:4,'negative:8}, '2': {"positive":7,'netural:4,'negative:8}}
        self.positive = 0
        self.negative = 0
        self.neutral = 0
        self.total_posts = 0
        self.percent_positive = 0
        self.percent_negative = 0
        self.percent_neutral = 0
        self.subreddit = subreddit
        self.type_of_post = type

    def clean_tweet(self, tweet):
        '''
            Utility function to clean tweet text by removing links, special characters
            using simple regex statements.
            '''
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

    def get_polarity(self, text):
        analysis = TextBlob(self.clean_tweet(text))
        return analysis.sentiment.polarity

    def get_subjectivity(self, text):
        analysis = TextBlob(self.clean_tweet(text))
        return analysis.sentiment.subjectivity

    '''
    This function returning the sentiment of specific text
    return if this text is positive/neutral/negative
    :argument text: text
    '''

    def get_post_sentiment(self, text):
        # create TextBlob object of passed tweet text
        analysis = TextBlob(self.clean_tweet(text))
        if analysis.sentiment.polarity > 0:
            return 'positive'
            # return 1
        elif analysis.sentiment.polarity == 0:
            return 'neutral'
            # return 0
        else:
            return 'negative'
            # return -1

    def check_removed_or_notremoved(self, post):
        if self.type_of_post == "Removed":
            if post['reddit_api']['post']['selftext'] == "[removed]":
                return post['pushift_api'].keys()
            else:
                return None

        elif self.type_of_post == "NotRemoved":
            if post['reddit_api']['post']['selftext'] != "[removed]":
                return post['pushift_api'].keys()
            else:
                return None

        elif self.type_of_post == "All":
            return post['pushift_api'].keys()

        else:
            return None

    '''
    This function is called from the main loop in Main_test file
    and the function updating the self variables.
    :argument post: specific post from db 
    :argument remove_all_NotRemove - indicate the type of the posts. (Removed/All/NotRemoved)
    :argument Month_Or_Year - iteration posts per month or per year. 
                              month - x_values = days of the posts
                              year - x_values = month of the posts
    '''

    def update_sentiment_values(self, post, Month_Or_Year):
        keys = {}
        # checking if this specific post is removed or not, depends in what we choose as a type

        keys = self.check_removed_or_notremoved(post, self.type_of_post)
        if keys == None:
            return
        self.total_posts += 1

        text = ""
        if 'selftext' in keys:
            text = post['pushift_api']['selftext']
            text += post['pushift_api']['title']
        else:
            # if 'title' in keys:
            text += post['pushift_api']['title']

        temp = self.get_post_sentiment(text)

        # update the dict in order to know how many posts are positive,neutral or negative
        if temp == 'positive':
            self.positive += 1
        elif temp == 'neutral':
            self.neutral += 1
        else:
            self.negative += 1

        if Month_Or_Year == "month":
            x_value = int(datetime.datetime.strptime(post['pushift_api']['created_utc'][0], "%Y-%m-%d").date().month)
        else:
            x_value = int(datetime.datetime.strptime(post['pushift_api']['created_utc'][0], "%Y-%m-%d").date().day)

        # creating a list of all the text(title) per month
        sentiment_month = {"positive": 0, 'neutral': 0, "negative": 0}
        if x_value not in self.text_per_month.keys():
            self.text_per_month[x_value] = [text]
            self.sentiment_per_month[x_value] = sentiment_month
            self.sentiment_per_month[x_value][temp] = 1
        else:
            self.text_per_month[x_value].append(text)
            self.sentiment_per_month[x_value][temp] += 1

    '''
    This function drawing a graph of the sentiment by the self variables.
    using with plt.plot
    :argument kind_of_sentiment - could be polarity or subjectivity
    :argument subreddit - Name of the sub-reddit
    :argument type - indicate if this a removed post or not. (Removed/All/Not Removed)
    :argument topic (optionally) - the defult value is "". otherwise if we creating graph for topic this value will be
                      the number of the topic (0,1,2....)
    :argument fullpath (optionally) - if we want to save an img of the graph this is the full path to drive.
    '''

    def draw_sentiment_time(self, kind_of_sentiment, subreddit, type, topic="", fullpath=""):
        # calculate the % of positive, negative and neutral
        self.percent_positive = (self.positive / self.total_posts) * 100
        self.percent_negative = (self.negative / self.total_posts) * 100
        self.percent_neutral = (self.neutral / self.total_posts) * 100
        y_value = []

        # afn = Afinn()
        for text in tqdm(self.text_per_month.values()):
            if kind_of_sentiment == "polarity":
                # temp = self.get_polarity(text[0])
                scores = sum([self.get_polarity(article) for article in text]) / self.total_posts

            if kind_of_sentiment == "subjectivity":
                scores = sum([self.get_subjectivity(article) for article in text]) / self.total_posts
            y_value.append(scores)

        self.text_per_month = dict(sorted(self.text_per_month.items(), key=lambda item: item[0]))

        # creating graph serially
        self.draw_sentiment_option1(y_value, list(self.text_per_month.keys()), kind_of_sentiment)

    def draw_sentiment_option1(self, Yaxis, Xaxis, kind_of_sentiment, topic="", fullpath=""):
        # position of text plot
        x = max(Xaxis) + 0.5
        y = max(Yaxis) / 2

        fig = plt.figure(figsize=(15, 5))
        ax = fig.add_subplot(111)

        # position of the max point
        ymax = max(Yaxis)
        xpos = Yaxis.index(ymax)
        xmax = Xaxis[xpos]

        # position of the min point
        ymin = min(Yaxis)
        xpos2 = Yaxis.index(ymin)
        xmin = Xaxis[xpos2]

        line, = ax.plot(list(self.text_per_month.keys()), Yaxis, label="Year: 2020")

        ax.annotate('Max', xy=(xmax, ymax), xytext=(xmax, ymax), arrowprops=dict(facecolor='black'))
        ax.annotate('Min', xy=(xmin, ymin), xytext=(xmin, ymin), arrowprops=dict(facecolor='black'))

        # check if this drawing is for topic anaylasis
        if topic != "":
            plt.text(x, y,
                     "Sub-Reddit: {}\n Type of posts: {}\n Total Post: {}\n Year: 2020\n Positive: {}%\n Negative: {}%\n Neutral: {}%\n Topic: {}".format(
                         self.subreddit, self.type_of_post, self.total_posts, round(self.percent_positive, 2),
                         round(self.percent_negative, 2), round(self.percent_neutral, 2),
                         topic))
        else:
            plt.text(x, y,
                     "Sub-Reddit: {}\n Type of posts: {}\n Total Post: {}\n Year: 2020\n Positive: {}%\n Negative: {}%\n Neutral: {}%".format(
                         self.subreddit, self.type_of_post, self.total_posts, round(self.percent_positive, 2),
                         round(self.percent_negative, 2), round(self.percent_neutral, 2)))

        # plt.xlabel("Month")
        # plt.ylabel("Sentiment ({})".format(kind_of_sentiment))

        # case: saving a plot img if there is a fuul path to locaion of saving
        fileName = "topic"  # change to your file name
        if fullpath != "":
            if topic != "":
                plt.savefig('{}/{} topic{}({}).png'.format(fullpath, fileName, topic, kind_of_sentiment))
            else:
                plt.savefig('{}/{} ({}).png'.format(fullpath, fileName, kind_of_sentiment))

        return plt.show()
