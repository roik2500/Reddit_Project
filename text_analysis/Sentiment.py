import datetime
import re
from textblob import TextBlob
import matplotlib.pyplot as plt
from afinn import Afinn
from tqdm import tqdm

class Sentiment:
    def __init__(self):
        self.text_per_month = {}
        self.positive = 0
        self.negative = 0
        self.neutral = 0
        self.total_posts = 0

    def clean_tweet(self,tweet):
        '''
            Utility function to clean tweet text by removing links, special characters
            using simple regex statements.
            '''
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

    def get_polarity(self,text):
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
        # analysis.detect_language(to="en")
        # print("Sentiment:")
        # print("The polarity is: {}".format(analysis.sentiment.polarity))
        # print("The subjectivity is: {}".format(analysis.sentiment.subjectivity))

        # set sentiment
        if analysis.sentiment.polarity > 0:
            return 'positive'
            # return 1
        elif analysis.sentiment.polarity == 0:
            return 'neutral'
            # return 0
        else:
            return 'negative'
            # return -1



    '''
    This function is called from the main loop in Main_test file
    and the function updating the self variables.
    :argument post: specific post from db 
    '''
    def update_sentiment_values(self, post):
        # data = []
        #text_per_month = {}
        #positive, negative, neutral = 0, 0, 0
        # The total amount of the post by one year
        #total_posts = 0
       # for x in tqdm(posts.find({})):

        self.total_posts += 1
        keys = post['pushift_api'].keys()
        text = ""

        if 'selftext' in keys:
            text = post['pushift_api']['selftext']
            text += post['pushift_api']['title']
        else:
        #if 'title' in keys:
            text += post['pushift_api']['title']

        temp = self.get_post_sentiment(text)

        # update the dict in order to know how many posts are positive,neutral or negative
        if temp == 'positive':
            self.positive += 1
        elif temp == 'neutral':
            self.neutral += 1
        else:
            self.negative += 1

        month = int(datetime.datetime.strptime(post['pushift_api']['created_utc'][0], "%Y-%m-%d").date().month)

        # creating a list of all the text(title) per month
        if month not in self.text_per_month.keys():
            self.text_per_month[month] = [text]
        else:
            self.text_per_month[month].append(text)
        # data.append([month, temp])



    '''
    This function drawing a graph of the sentiment by the self variables.
    using with plt.plot
    '''
    def draw_sentiment_time(self):
        # calculate the % of positive, negative and neutral
        positive = (self.positive / self.total_posts) * 100
        negative = (self.negative / self.total_posts) * 100
        neutral = (self.neutral / self.total_posts) * 100
        y_value = []

        afn = Afinn()
        for text in tqdm(self.text_per_month.values()):
            #scores = sum([afn.score(article) for article in text]) / self.total_posts
            scores = sum([self.get_polarity(article) for article in text]) / self.total_posts
            y_value.append(scores)

        self.text_per_month = dict(sorted(self.text_per_month.items(), key=lambda item: item[0]))

        print("text_per_month: {}".format(self.text_per_month))
        print("Sentiment\polarity: {}".format(y_value))

        plt.plot(list(self.text_per_month.keys()), y_value, label="Year: 2020")
        plt.text(6, 0.00,
                 "Sub-Reddit: Politics\n Total Post: {}\n Year: 2020\n Positive: {}%\n Negative: {}%\n Neutral: {}%".format(
                     self.total_posts,round(positive,2), round(negative,2), round(neutral,2)))
        plt.xlabel("Month")
        plt.ylabel("Sentiment (polarity)")
        return plt.show()
















        # # for option count
        # hist = pd.DataFrame(data, columns=['month', 'Sentiment'])
        # with sns.axes_style('white'):
        #     # g = sns.factorplot("Year", data=hist, aspect=1.7, kind='count', hue='Sentiment', order=range(2017, 2023))
        #     g = sns.factorplot("month", data=hist, aspect=1.7, kind='count', hue='Sentiment', order=range(2017, 2023))
        # return plt.show()


# if __name__ == '__main__':
#     api = Sentiment()
#     con = Con_DB()
#     # post_csv = api.get_post_from_csv()
#     # api.draw_sentiment_time(post_csv,'c')
#     posts = con.get_cursor_from_mongodb(collection_name="politics")
#     #text = con.get_posts_text(posts,"title")
#     api.draw_sentiment_time(posts)
#
