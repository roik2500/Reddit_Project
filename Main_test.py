from tqdm import tqdm

from db_utils.FileReader import FileReader
from statistics_analysis.Statistics import Statistic
from db_utils.Con_DB import Con_DB
from text_analysis.Sentiment import Sentiment
import csv
import os
import matplotlib.pyplot as plt
from matplotlib.dates import date2num
import datetime

# "mongodb+srv://roi:1234@redditdata.aav2q.mongodb.net/"
if __name__ == '__main__':
    con = Con_DB()
    #posts = con.get_cursor_from_mongodb(collection_name="wallstreetbets")
    # path = 'C:/Users/roik2/PycharmProjects/Reddit_Project/data/document_topic_table_general.csv'  # Change to your path in your computer
    path_csv = 'C:/Users/roik2/PycharmProjects/Reddit_Project/data/document_topic_table_general_best .csv'  # Change to your path in your computer
    path_drive = 'G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/'

    # outputs
    afin_sentiment = 'G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/afin/afin_sentiment'
    afin_statistic = 'G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/afin/afin_statistic'
    textblob_sentiment = 'G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/textblob/textblob_sentiment'
    textblob_statistic = 'G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/textblob/textblob_statistic'

    # define a objects for tests
    subreddit = "wallstreetbets"
    TypeOfPost = "All"  # All/removed/NotRemoved
    api_all = Sentiment(subreddit, "All")
    api_Removed = Sentiment(subreddit, "Removed")
    api_Not_Removed = Sentiment(subreddit, "NotRemoved")

    stat_all = Statistic(subreddit, "All")
    stat_api_Removed = Statistic(subreddit, "Removed")
    stat_Not_Removed = Statistic(subreddit, "NotRemoved")

    # All
    afin_sent_all = Sentiment("wallstreetbets", "All")
    textblob_sent_all = Sentiment("wallstreetbets", "All")
    afin_stat_all = Statistic("wallstreetbets", "All")
    textblob_stat_all = Statistic("wallstreetbets", "All")

    # Removed
    afin_sent_removed = Sentiment("wallstreetbets", "Removed")
    textblob_sent_removed = Sentiment("wallstreetbets", "Removed")
    afin_stat_removed = Statistic("wallstreetbets", "Removed")
    textblob_stat_removed = Statistic("wallstreetbets", "Removed")

    # NotRemoved
    afin_sent_notremoved = Sentiment("wallstreetbets", "NotRemoved")
    textblob_sent_notremoved = Sentiment("wallstreetbets", "NotRemoved")
    afin_stat_notremoved = Statistic("wallstreetbets", "NotRemoved")
    textblob_stat_notremoved = Statistic("wallstreetbets", "NotRemoved")

    '''
    This function creating graphs for all the topics (graph per topic)
    :argument path - path to csv file that contains the data of all topics
    :return graphs -> graph per topic
    '''


    def creat_Sentiment_Graph_For_Topics(path_drive=""):
        posts = con.get_cursor_from_mongodb(collection_name="wallstreetbets")
        ##full_path = path_drive + "sentiment for topics/"
        topic_set = Sentiment("wallstreetbets", "All")
        topic_csv = con.read_fromCSV(path_csv)
        for topic_id in tqdm(topic_csv["Dominant_Topic"].unique()):
            dff = topic_csv[topic_csv["Dominant_Topic"] == topic_id]
            for post_id in tqdm(dff['post_id']):
                post = posts.find({"post_id": post_id})
                if post.count() == 0: continue
                post = posts.find({"post_id": post_id})[0]
                topic_set.update_sentiment(post, "All", "month")  # iteration per month or per year

            path_textblob_polarity = creat_new_folder_drive("textblob_polarity", path_drive + '/')
            path_afin_polarity = creat_new_folder_drive("afin_polarity", path_drive + '/')
            path_textblob_subjectivity = creat_new_folder_drive("textblob_subjectivity", path_drive + '/')

            topic_set.draw_sentiment_time("textblob", "polarity",topic_id,path_textblob_polarity)
            topic_set.draw_sentiment_time("textblob", "subjectivity", topic_id, path_textblob_subjectivity)
            topic_set.draw_sentiment_time("afin", "polarity",topic_id,path_afin_polarity)


            # topic_set.draw_sentiment_time("polarity", "wallstreetbets", "All")
            # topic_set.draw_sentiment_time("subjectivity", "wallstreetbets", "All")



    '''
    Creating a new folder in Google drive. 
    :argument folderName - name of the new folder
    :return full path to the new folder
    '''
    def creat_new_folder_drive(folderName, oldpath):
        # newpath = r'{}'.format(path_drive)+'/{}'.format(folderName)
        path = os.path.join(oldpath, folderName)
        if not os.path.exists(path):
            os.mkdir(path)
        return path


    '''
    Test function
    This function creating graphs for posts are Removed/NotRemove/All
    '''


    def creatSentiment_and_statistic_Graph():
        for post in tqdm(posts.find({})):
            # text = con.get_posts_text(posts,"title")
            api_all.update_sentiment(post, "month", "textblob")
            stat_all.precentage_media(con, post)

            api_Removed.update_sentiment(post, "month", "textblob")
            stat_api_Removed.precentage_media(con, post)

            api_Not_Removed.update_sentiment(post, "month", "textblob")
            stat_Not_Removed.precentage_media(con, post)

        stat_all.get_percent()
        stat_api_Removed.get_percent()
        stat_Not_Removed.get_percent()

        api_all.draw_sentiment_time("textblob", "polarity")
        api_Removed.draw_sentiment_time("textblob", "polarity")
        api_Not_Removed.draw_sentiment_time("textblob", "polarity")

        api_all.draw_sentiment_time("textblob", "subjectivity")
        api_Removed.draw_sentiment_time("textblob", "subjectivity")


    # api_Not_Removed.draw_sentiment_time("subjectivity", "wallstreetbets", "NotRemoved")

    def update_sentiment_afin_textblob(posts,
                                       afin_sent_all, textblob_sent_all, afin_stat_all, textblob_stat_all,
                                       afin_sent_removed, textblob_sent_removed, afin_stat_removed,
                                       textblob_stat_removed,
                                       afin_sent_notremoved, textblob_sent_notremoved, afin_stat_notremoved,
                                       textblob_stat_notremoved
                                       ):

        # afin = creat_new_folder_drive("afin", path_drive)
        # textblob = creat_new_folder_drive("textblob", path_drive)

        # # statistic path
        # statistic_afin_path = creat_new_folder_drive("AllData", afin_statistic + '/')
        # statistic_textblob_path = creat_new_folder_drive("AllData", textblob_statistic + '/')
        #
        # # sentiment  path
        # sentiment_afin_path = creat_new_folder_drive("AllData", afin_sentiment + '/')
        # sentiment_textblob_path = creat_new_folder_drive("AllData", textblob_sentiment + '/')
        for post in tqdm(posts.find({})):
            # All
            afin_sent_all.update_sentiment(post, "month", "afin")
            textblob_sent_all.update_sentiment(post, "month", "textblob")
            afin_stat_all.precentage_media(con, post)
            textblob_stat_all.precentage_media(con, post)

            # Removed
            afin_sent_removed.update_sentiment(post, "month", "afin")
            textblob_sent_removed.update_sentiment(post, "month", "textblob")
            afin_stat_removed.precentage_media(con, post)
            textblob_stat_removed.precentage_media(con, post)

            # NotRemoved
            afin_sent_notremoved.update_sentiment(post, "month", "afin")
            textblob_sent_notremoved.update_sentiment(post, "month", "textblob")
            afin_stat_notremoved.precentage_media(con, post)
            textblob_stat_notremoved.precentage_media(con, post)
        # draw_sentiment_afin_textblob()


    def draw_sentiment_afin_textblob(sentiment_afin_path,sentiment_textblob_path,statistic_afin_path,statistic_textblob_path):
        # All
        afin_sent_all.draw_sentiment_time("afin", fullpath=sentiment_afin_path)
        textblob_sent_all.draw_sentiment_time("textblob", fullpath=sentiment_textblob_path)
        afin_stat_all.draw_statistic_bars(afin_sent_all, path=statistic_afin_path)
        textblob_stat_all.draw_statistic_bars(textblob_sent_all, path=statistic_textblob_path)

        # Removed
        afin_sent_removed.draw_sentiment_time("afin", fullpath=sentiment_afin_path)
        textblob_sent_removed.draw_sentiment_time("textblob", fullpath=sentiment_textblob_path)
        afin_stat_removed.draw_statistic_bars(afin_sent_removed, path=statistic_afin_path)
        textblob_stat_removed.draw_statistic_bars(textblob_sent_removed, path=statistic_textblob_path)

        # NotRemoved
        afin_sent_notremoved.draw_sentiment_time("afin", fullpath=sentiment_afin_path)
        textblob_sent_notremoved.draw_sentiment_time("textblob", fullpath=sentiment_textblob_path)
        afin_stat_notremoved.draw_statistic_bars(afin_sent_notremoved, path=statistic_afin_path)
        textblob_stat_notremoved.draw_statistic_bars(textblob_sent_notremoved, path=statistic_textblob_path)



'''
 This function is creating a graph of 3 bars per month that indicate the amount of positive,netural
 and negetive posts.
 '''


def draw_bars():
    creatSentiment_and_statistic_Graph()
    stat_all.draw_statistic_bars(api_all)


def topic_4_account():
    path1 ="G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/topic_modeling/wallstreetbets/post/all"
    path2 = "G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/topic_modeling/wallstreetbets/post/removed"

    path_post_all =creat_new_folder_drive("sentiment for topics",path1+'/')
    path_post_removed = creat_new_folder_drive("sentiment for topics",path2+'/')

    creat_Sentiment_Graph_For_Topics(path_post_all)
    creat_Sentiment_Graph_For_Topics(path_post_removed)

def Alldata():
             # statistic path
    statistic_afin_path = creat_new_folder_drive("AllData", afin_statistic + '/')
    statistic_textblob_path = creat_new_folder_drive("AllData", textblob_statistic + '/')

            # sentiment  path
    sentiment_afin_path = creat_new_folder_drive("AllData", afin_sentiment + '/')
    sentiment_textblob_path = creat_new_folder_drive("AllData", textblob_sentiment + '/')

    for i in range(1, 5):
        print("start AUTH_DB{}".format(i))
        con.setAUTH_DB(i)
        postss = con.get_cursor_from_mongodb(collection_name="wallstreetbets")
        update_sentiment_afin_textblob(postss,
                                       afin_sent_all, textblob_sent_all, afin_stat_all, textblob_stat_all,
                                       afin_sent_removed, textblob_sent_removed, afin_stat_removed,
                                       textblob_stat_removed,
                                       afin_sent_notremoved, textblob_sent_notremoved, afin_stat_notremoved,
                                       textblob_stat_notremoved
                                       )

    draw_sentiment_afin_textblob(sentiment_afin_path,sentiment_textblob_path,statistic_afin_path,statistic_textblob_path)
    draw_sentiment_afin_textblob(statistic_afin_path,statistic_textblob_path,sentiment_afin_path,sentiment_textblob_path)

topic_4_account()
#Alldata()

# p = stat.get_percent()
# draw_bars()
# topic = creat_Sentiment_Graph_For_Topics(path_drive="")

# update_sentiment_afin_textblob()
# set = creatSentiment_and_statistic_Graph()

