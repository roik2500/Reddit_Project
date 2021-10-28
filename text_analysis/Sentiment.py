import datetime
import logging
import re
from nltk.sentiment import SentimentIntensityAnalyzer
import matplotlib.pyplot as plt
from tqdm import tqdm
from pysentimiento import SentimentAnalyzer
from pysentimiento.preprocessing import preprocess_tweet
logging.getLogger().setLevel(logging.CRITICAL)


class Sentiment:
    def __init__(self, subreddit, type):
        self.text_per_month = {}
        self.sentiment_per_month = {}  ##{'1': {"positive":5,'netural:4,'negative:8}, '2': {"positive":7,'netural:4,'negative:8}}
        self.score_per_month = {}
        self.positive = 0
        self.negative = 0
        self.neutral = 0
        self.total_posts = 0
        self.percent_positive = 0
        self.percent_negative = 0
        self.percent_neutral = 0
        self.subreddit = subreddit
        self.type_of_post = type
        self.analyzer = SentimentAnalyzer(lang="es")
    def print_values(self,text=""):
        if text:
            print(text)
        print("percent_positive: {} \n percent_negative: {} \n percent_neutral: {} \n Num of positive: {} \n Num of "
              "negative: {} \n  Num of neutral: {}".format(self.percent_positive,self.percent_negative,
                                                           self.percent_neutral,self.positive,self.negative,
                                                           self.neutral))
        print(self.sentiment_per_month)

    def clean_tweet(self, tweet):
        '''
            Utility function to clean tweet text by removing links, special characters
            using simple regex statements.
            '''
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

    def get_score_pysentimiento(self, text):
        score = self.analyzer.predict(text).probas
        return score[self.analyzer.predict(text).output]

    def get_post_sentiment_pysentimiento(self, text):
        output = self.analyzer.predict(text).output
        if output == 'NEG':
            return 'negative'
        elif output == 'POS':
            return 'positive'
        else:
            return 'neutral'

    '''
    This function checking if the specific post is removed/All/Not removed
    :argument post
    :return list of the pushift_api's dict keys
    '''

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

    def update_sentiment(self, post_or_comment, Month_Or_Year, post_comment):
        keys = {}
        text = ""

        if post_comment == "post":
            if type(post_or_comment['pushift_api']['created_utc']) != list:
                post_or_comment["pushift_api"]["created_utc"] = datetime.datetime.fromtimestamp(int(post_or_comment["pushift_api"]["created_utc"])).isoformat().split("T")
            created = post_or_comment['pushift_api']['created_utc'][0]
            # checking if this specific post is removed or not, depends in what we choose as a type
            keys = self.check_removed_or_notremoved(post_or_comment)
            if keys == None:
                return

            if 'selftext' in keys:
                text = post_or_comment['pushift_api']['selftext']
                text += post_or_comment['pushift_api']['title']
            else:
                # if 'title' in keys:
                text += post_or_comment['pushift_api']['title']
        else:
            text = post_or_comment['text']
            created = post_or_comment['created']

        self.total_posts += 1

        # preprocess
        # 1. Replaces user handles and URLs by special tokens
        # 2. Shortens repeated characters
        # 3. Normalizes laughters
        # 4. Handles hashtags
        # 5. Handles emojis
        text = preprocess_tweet(text)

        temp = self.get_post_sentiment_pysentimiento(text)

        # update the dict in order to know how many posts are positive,neutral or negative
        if temp == 'positive':
            self.positive += 1
        elif temp == 'neutral':
            self.neutral += 1
        else:
            self.negative += 1

        if Month_Or_Year == "month":
            x_value = int(datetime.datetime.strptime(created, "%Y-%m-%d").date().month)
        elif Month_Or_Year == "date":
            x_value = str(datetime.datetime.strptime(created, "%Y-%m-%d").date())
        else:
            x_value = int(datetime.datetime.strptime(created, "%Y-%m-%d").date().day)

        # creating a list of all the text(title) per month
        sentiment_month = {"positive": 0, 'neutral': 0, "negative": 0}
        if x_value not in self.text_per_month.keys():
            self.score_per_month[x_value] = self.get_score_pysentimiento(text)
            self.text_per_month[x_value] = [text]
            self.sentiment_per_month[x_value] = sentiment_month
            self.sentiment_per_month[x_value][temp] = 1
        else:
            self.score_per_month[x_value] += self.get_score_pysentimiento(text)
            self.text_per_month[x_value].append(text)
            self.sentiment_per_month[x_value][temp] += 1

    '''
    This function drawing a graph of the sentiment by the self variables.
    using with plt.plot
    :argument name - could be polarity or subjectivity
    :argument subreddit - Name of the sub-reddit
    :argument type - indicate if this a removed post or not. (Removed/All/Not Removed)
    :argument topic (optionally) - the defult value is "". otherwise if we creating graph for topic this value will be
                      the number of the topic (0,1,2....)
    :argument fullpath (optionally) - if we want to save an img of the graph this is the full path to drive.
    '''

    def draw_sentiment_time(self, LayberyName, name="", topic="", fullpath=""):
        # calculate the % of positive, negative and neutral
        self.percent_positive = (self.positive / self.total_posts) * 100
        self.percent_negative = (self.negative / self.total_posts) * 100
        self.percent_neutral = (self.neutral / self.total_posts) * 100
        y_value = []

        # for text in tqdm(self.text_per_month.values()):
        #     # if LayberyName == "afin":
        #     #     scores = sum([self.get_polarity_afin(article) for article in text]) / self.total_posts
        #     #
        #     # elif name == "polarity" or LayberyName == "textblob":
        #     #     # temp = self.get_polarity(text[0])
        #     #     scores = sum([self.get_polarity_textblob(article) for article in text]) / self.total_posts
        #     #
        #     # elif name == "subjectivity":
        #     #     scores = sum([self.get_subjectivity_textblob(article) for article in text]) / self.total_posts
        #     #
        #     # elif LayberyName == "pysentimiento":
        #     scores = sum([self.get_score_pysentimiento(article) for article in text]) / self.total_posts
        #     y_value.append(scores)


        [y_value.append(scores / self.total_posts) for scores in list(self.score_per_month.values())]

        self.text_per_month = dict(sorted(self.text_per_month.items(), key=lambda item: item[0]))

        # creating graph serially
        self.draw_sentiment_plot(y_value, list(self.text_per_month.keys()), name, topic=topic,
                                 fullpath=fullpath, LayberyName=LayberyName)

    def draw_sentiment_plot(self, Yaxis, Xaxis, name, topic="", fullpath="", LayberyName=""):
        # position of text plot
        # x = max(Xaxis) + 0.5
        # y = max(Yaxis) / 2

        x = max(Xaxis)
        y = max(Yaxis)

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

        ax.annotate('Max = {}'.format(round(ymax, 3)), xy=(xmax, ymax), xytext=(xmax, ymax),
                    arrowprops=dict(facecolor='black'))
        ax.annotate('Min = {}'.format(round(ymin)), xy=(xmin, ymin), xytext=(xmin, ymin),
                    arrowprops=dict(facecolor='black'))

        # check if this drawing is for topic anaylasis
        if topic != "":
            ax.text(x, y,
                    "Sub-Reddit: {}\n Type of posts: {}\n Total Post: {}\n Year: 2020\n Positive: {}%\n Negative: {}%\n Neutral: {}%\n Topic: {}\n LayberyName: {}\n name: {}".format(
                        self.subreddit, self.type_of_post, self.total_posts, round(self.percent_positive, 2),
                        round(self.percent_negative, 2), round(self.percent_neutral, 2),
                        topic, LayberyName, name))
        else:
            ax.text(x, y,
                    "Sub-Reddit: {}\n Type of posts: {}\n Total Post: {}\n Year: 2020\n Positive: {}%\n Negative: {}%\n Neutral: {}%".format(
                        self.subreddit, self.type_of_post, self.total_posts, round(self.percent_positive, 2),
                        round(self.percent_negative, 2), round(self.percent_neutral, 2)))

        # plt.xlabel("Month")
        # plt.ylabel("Sentiment ({})".format(name))

        # case: saving a plot img if there is a fuul path to locaion of saving
        fileName = ""
        fileName += self.subreddit
        fileName += self.type_of_post  # change to your file name

        if fullpath != "":
            if topic != "" and name != "":
                plt.savefig('{}/{} {}({}).png'.format(fullpath, fileName, topic, name))

            elif topic != "" and name == "":
                plt.savefig('{}/{} {}.png'.format(fullpath, fileName, topic))

            elif name == "" and topic == "":
                p = fullpath + '/' + self.subreddit + self.type_of_post+'.png'
                plt.savefig('{}/{} {}.png'.format(fullpath,self.subreddit,self.type_of_post))
            else:
                ax.savefig('{}/{} ({}).png'.format(fullpath,self.subreddit, name))

        return plt.show()
