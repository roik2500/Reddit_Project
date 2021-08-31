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


def extract_posts_emotion_rate(posts, emotion_dict, text_filed):
    for post in posts.find({}):
        if post['reddit_api'][0]['data']['children'][0]['data']['selftext'] == '[removed]':
            reddit_api.convert_time_format(post['reddit_api'][0]['data']['children'][0]['data'])
            year = int(datetime.strptime(post['reddit_api'][0]['data']['children'][0]['data']['created_utc'][0]
                                          , "%Y-%m-%d").date().year)
            month = int(datetime.strptime(post['reddit_api'][0]['data']['children'][0]['data']['created_utc'][0]
                                          , "%Y-%m-%d").date().month)
            date_key = str(year) + "/" + str(month)
            if date_key in emotion_dict.keys():
                emotion_dict[date_key].append([post['reddit_api'][0]['data']['children'][0]['data']['id'],
                                    te.get_emotion(post['reddit_api'][0]['data']['children'][0]['data'][text_filed])])
            else:
                emotion_dict[date_key] = [[post['reddit_api'][0]['data']['children'][0]['data']['id'],
                                   te.get_emotion(post['reddit_api'][0]['data']['children'][0]['data'][text_filed])]]



# date_format  = '%Y/%m' or  "%Y-%m-%d"
def emotion_plot_for_posts_in_subreddit(emotion_posts_avg_of_subreddit, date_format):

    x1 = sorted([*emotion_posts_avg_of_subreddit["Angry"]], key=lambda t: datetime.strptime(t, date_format))
    x2 = sorted([*emotion_posts_avg_of_subreddit["Fear"]], key=lambda t: datetime.strptime(t, date_format))
    x3 = sorted([*emotion_posts_avg_of_subreddit["Happy"]], key=lambda t: datetime.strptime(t, date_format))
    x4 = sorted([*emotion_posts_avg_of_subreddit["Sad"]], key=lambda t: datetime.strptime(t, date_format))
    x5 = sorted([*emotion_posts_avg_of_subreddit["Surprise"]], key=lambda t: datetime.strptime(t, date_format))

    y1 = [*emotion_posts_avg_of_subreddit["Angry"].values()]
    y2 = [*emotion_posts_avg_of_subreddit["Fear"].values()]
    y3 = [*emotion_posts_avg_of_subreddit["Happy"].values()]
    y4 = [*emotion_posts_avg_of_subreddit["Sad"].values()]
    y5 = [*emotion_posts_avg_of_subreddit["Surprise"].values()]

    # plot lines
    plt.xticks(rotation=90)
    plt.plot(x1, y1, label="Angry", linestyle="-")
    plt.plot(x2, y2, label="Fear", linestyle="--")
    plt.plot(x3, y3, label="Happy", linestyle="-.")
    plt.plot(x4, y4, label="Sad", linestyle=":")
    plt.plot(x5, y5, label="Surprise", linestyle="-")

    plt.title('Emotion detection to titles posts in subreddit politics')
    plt.ylabel("Emotion rate")
    plt.xlabel("Time (month)")
    plt.legend()
    plt.show()




emotion_dict = {}
emotion_avg_in_month = ["Angry", "Fear", "Happy", "Sad", "Surprise"]
emotion_posts_avg_of_subreddit = {"Angry" : {}, "Fear": {},
                                    "Happy" : {}, "Sad": {}, "Surprise" : {}}

posts = con_DB.get_cursor_from_mongodb(db_name="reddit", collection_name="politics")

print("extracte emotion rate")
extract_posts_emotion_rate(posts, emotion_dict, 'title')

print("MEAN")
for emotion in emotion_posts_avg_of_subreddit.keys():
    for key, month_posts_emotions in emotion_dict.items():
        emotion_posts_avg_of_subreddit[emotion][key] = \
            mean([emotion_rate[1][emotion] for emotion_rate in month_posts_emotions])

print("plot")
emotion_plot_for_posts_in_subreddit(emotion_posts_avg_of_subreddit,  '%Y/%m')

print("write to disk")
with open('C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\emotion_politics_removed.json', 'w') as fp:
    json.dump(emotion_posts_avg_of_subreddit, fp)



