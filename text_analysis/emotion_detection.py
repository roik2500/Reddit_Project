import os
from statistics import mean
import text2emotion as te
from db_utils.Con_DB import Con_DB
from dotenv import load_dotenv
from datetime import datetime
import json
import matplotlib.pyplot as plt
from api_utils.reddit_api import reddit_api

load_dotenv()
con_DB = Con_DB()
reddit_api = reddit_api()


class EmotionDetection:
    def __init__(self):
        self.emotion_dict = {}
        self.emotion_posts_avg_of_subreddit = {"Angry" : {}, "Fear": {},
                                            "Happy" : {}, "Sad": {}, "Surprise" : {}}

    def extract_posts_emotion_rate(self, posts_cursor): # posts format: [title, selftext, date, id]
        # for post in posts_cursor.find({}):
        for post in posts_cursor:
            post = con_DB.get_text_from_post_OR_comment(object=post, post_or_comment='post')
            # if post['reddit_api'][0]['data']['children'][0]['data']['selftext'] == '[removed]':

            # The the created_utc time should be converted to "%Y-%m-%d" format
            # year = int(datetime.strptime(post['reddit_api']['post']['created_utc'][0]
            #                               , "%Y-%m-%d").date().year)
            # month = int(datetime.strptime(post['reddit_api']['post']['created_utc'][0]
            #                               , "%Y-%m-%d").date().month)
            year = int(datetime.strptime(post[2]
                                          , "%Y-%m-%d").date().year)
            month = int(datetime.strptime(post[2]
                                          , "%Y-%m-%d").date().month)
            date_key = str(year) + "/" + str(month)

            # pushshift_post = post["pushift_api"]
            #
            # # id_list.append(pushshift_post["pushift_api"]["id"])
            # # tokens = pushshift_post["title"]
            # # if "selftext" in pushshift_post and not pushshift_post["selftext"].__contains__("[removed]") and pushshift_post[
            # #     "selftext"] != "[deleted]":
            # #     tokens+=pushshift_post["selftext"]
            # # text_data.append(tokens)
            #
            # reddit_post = post["reddit_api"]['post']
            # tokens = reddit_post['title']
            # if "selftext" in pushshift_post and reddit_post['selftext'] == '[removed]':
            #     tokens+=pushshift_post["selftext"]

            if post[1] == '':
                tokens = post[0]
            else:
                tokens = post[0] + post[1]

            self.insert_rates_to_emotion_dict(date_key, post[3], tokens)

    def insert_rates_to_emotion_dict(self, date_key, reddit_id, tokens):
        if date_key in self.emotion_dict.keys():
            self.emotion_dict[date_key].append([reddit_id, self.get_post_emotion_rate(tokens)])
        else:
            self.emotion_dict[date_key] = [[reddit_id, self.get_post_emotion_rate(tokens)]]

    def get_post_emotion_rate(self, text):
        return te.get_emotion(text)

    # date_format  = '%Y/%m' or  "%Y-%m-%d"
    def emotion_plot_for_posts_in_subreddit(self, date_format, subreddit_name, NER, path_to_save_plt, category):

        x1 = sorted([*self.emotion_posts_avg_of_subreddit["Angry"]], key=lambda t: datetime.strptime(t, date_format))
        x2 = sorted([*self.emotion_posts_avg_of_subreddit["Fear"]], key=lambda t: datetime.strptime(t, date_format))
        x3 = sorted([*self.emotion_posts_avg_of_subreddit["Happy"]], key=lambda t: datetime.strptime(t, date_format))
        x4 = sorted([*self.emotion_posts_avg_of_subreddit["Sad"]], key=lambda t: datetime.strptime(t, date_format))
        x5 = sorted([*self.emotion_posts_avg_of_subreddit["Surprise"]], key=lambda t: datetime.strptime(t, date_format))

        y1 = [*self.emotion_posts_avg_of_subreddit["Angry"].values()]
        y2 = [*self.emotion_posts_avg_of_subreddit["Fear"].values()]
        y3 = [*self.emotion_posts_avg_of_subreddit["Happy"].values()]
        y4 = [*self.emotion_posts_avg_of_subreddit["Sad"].values()]
        y5 = [*self.emotion_posts_avg_of_subreddit["Surprise"].values()]

        # plot lines
        plt.xticks(rotation=90)
        plt.plot(x1, y1, label="Angry", linestyle="-")
        plt.plot(x2, y2, label="Fear", linestyle="--")
        plt.plot(x3, y3, label="Happy", linestyle="-.")
        plt.plot(x4, y4, label="Sad", linestyle=":")
        plt.plot(x5, y5, label="Surprise", linestyle="-")

        plt.title('Emotion Detection to ' + subreddit_name + ' subreddit ' + NER + "-" + category)
        plt.ylabel("Emotion rate")
        plt.xlabel("Time (month)")
        plt.legend()
        plt.savefig(path_to_save_plt + '_' + NER + '_' + datetime.now().strftime("%H-%M-%S") + ".jpg", transparent=True)
        plt.show()



    def calculate_post_emotion_rate_mean(self):
        for emotion in self.emotion_posts_avg_of_subreddit.keys():
            for key, month_posts_emotions in self.emotion_dict.items():
                self.emotion_posts_avg_of_subreddit[emotion][key] = \
                    mean([emotion_rate[1][emotion] for emotion_rate in month_posts_emotions])


if __name__ == '__main__':
    emotion_detection = EmotionDetection()
    Con_DB = Con_DB()
    # emotion_avg_in_month = ["Angry", "Fear", "Happy", "Sad", "Surprise"]
    Con_DB.get_data_categories(category="Removed", collection_name="wallstreetbets")
    # relevant_posts = Con_DB.get_specific_items_by_post_ids(ids_list=['hjaejw'])
    # posts_cursor = con_DB.get_specific_items_by_post_ids(db_name="reddit", collection_name="wallstreetbets")

    print("extracte emotion rate")
    emotion_detection.extract_posts_emotion_rate(Con_DB.posts_cursor.find({}))

    print("MEAN")
    emotion_detection.calculate_post_emotion_rate_mean()

    print("plot")
    emotion_detection.emotion_plot_for_posts_in_subreddit(date_format='%Y/%m', subreddit_name="wallstreetbets",
                          NER="",
                          path_to_save_plt='C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\files\\xxx.jpg',
                          category="Removed")

    # print("write to disk")
    # with open('C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\emotion_wallstreetbets.json', 'w') as fp:
    #     json.dump(emotion_detection.emotion_posts_avg_of_subreddit, fp)



