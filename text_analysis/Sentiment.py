import datetime
import re
from textblob import TextBlob
import matplotlib.pyplot as plt
from afinn import Afinn
from tqdm import tqdm

from db_utils.Con_DB import Con_DB


class Sentiment:
    def clean_tweet(self,tweet):
        '''
            Utility function to clean tweet text by removing links, special characters
            using simple regex statements.
            '''
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

    def get_post_sentiment(self, tweet):
        # create TextBlob object of passed tweet text
        analysis = TextBlob(self.clean_tweet(tweet))
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

    # return all the posts text that include the parametar "title"
    def draw_sentiment_time(self, posts_text):
        # data = []
        text_per_month = {}
        positive, negative, neutral = 0, 0, 0
        afn = Afinn()

        # The total amount of the post by one year
        total = 0

        for x in tqdm(posts_text):
            total += 1
            temp = self.get_post_sentiment(x['title'])
            # update the dict in order to know how many posts are positive,neutral or negative
            if temp == 'positive':
                positive += 1
            elif temp == 'neutral':
                neutral += 1
            else:
                negative += 1

            month = int(datetime.datetime.strptime(x['created_utc'][2:12], "%Y-%m-%d").date().month)

            # creating a list of all the text(title) per month
            if month not in text_per_month.keys():
                text_per_month[month] = [x['title']]
            else:
                text_per_month[month].append(x['title'])
            # data.append([month, temp])

        # calculate the % of positive, negative and neutral
        positive = (positive / total) * 100
        negative = (negative / total) * 100
        neutral = (neutral / total) * 100
        y_value = []
        for text in tqdm(text_per_month.values()):
            scores = sum([afn.score(article) for article in text]) / total
            y_value.append(scores)

        text_per_month = dict(sorted(text_per_month.items(), key=lambda item: item[0]))
        plt.plot(list(text_per_month.keys()), y_value, label="Year: 2020")
        plt.text(13, -0.02,
                 "Sub-Reddit: Politic\n Total Post: 20,000\n Year: 2020\n Positive: {}%\n Negative: {}%\n Neutral: {}%".format(
                     positive, negative, neutral))
        plt.xlabel("Month")
        plt.ylabel("Sentiment (polarity)")
        return plt.show()

        # # for option count
        # hist = pd.DataFrame(data, columns=['month', 'Sentiment'])
        # with sns.axes_style('white'):
        #     # g = sns.factorplot("Year", data=hist, aspect=1.7, kind='count', hue='Sentiment', order=range(2017, 2023))
        #     g = sns.factorplot("month", data=hist, aspect=1.7, kind='count', hue='Sentiment', order=range(2017, 2023))
        # return plt.show()


if __name__ == '__main__':
    api = Sentiment()
    con = Con_DB()
    # post_csv = api.get_post_from_csv()
    # api.draw_sentiment_time(post_csv,'c')
    posts = con.get_cursor_from_mongodb(collection_name="politics_sample")
    text = con.get_posts_text(posts,"title")
    api.draw_sentiment_time(text)
