from tqdm import tqdm
from statistics_analysis.Statistics import Statistic
from db_utils.Con_DB import Con_DB
from text_analysis.Sentiment import Sentiment
import csv
import os
import matplotlib.pyplot as plt
from matplotlib.dates import date2num
import datetime

if __name__ == '__main__':

    con = Con_DB()
    posts = con.get_cursor_from_mongodb(collection_name="wallstreetbets")
    # path = 'C:/Users/roik2/PycharmProjects/Reddit_Project/data/document_topic_table_general.csv'  # Change to your path in your computer
    path_csv = 'C:/Users/roik2/PycharmProjects/Reddit_Project/data/document_topic_table_general_best .csv'  # Change to your path in your computer
    path_drive = 'G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/'

    # define a objects for tests
    subreddit = "wallstreetbets"
    TypeOfPost = "All"  # All/removed/NotRemoved
    api_all = Sentiment(subreddit, "All")
    api_Removed = Sentiment(subreddit, "Removed")
    api_Not_Removed = Sentiment(subreddit, "NotRemoved")

    stat_all = Statistic(subreddit, "All")
    stat_api_Removed = Statistic(subreddit, "Removed")
    stat_Not_Removed = Statistic(subreddit, "NotRemoved")

    '''
    This function creating graphs for all the topics (graph per topic)
    :argument path - path to csv file that contains the data of all topics
    :return graphs -> graph per topic
    '''


    def creat_Sentiment_Graph_For_Topics(path_drive=""):
        full_path = path_drive + "sentiment for topics/"
        topic_set = Sentiment("wallstreetbets", "All")
        topic_csv = con.read_fromCSV(path_csv)
        for topic_id in tqdm(topic_csv["Dominant_Topic"].unique()):
            dff = topic_csv[topic_csv["Dominant_Topic"] == topic_id]
            for post_id in tqdm(dff['post_id']):
                post = posts.find({"post_id": post_id})
                if post.count() == 0: continue
                post = posts.find({"post_id": post_id})[0]
                topic_set.update_sentiment(post, "All", "month")  # iteration per month or per year

            # topic_set.draw_sentiment_time("polarity", "wallstreetbets", "All",topic_id,path_drive)
            # topic_set.draw_sentiment_time("subjectivity", "wallstreetbets", "All",topic_id,path_drive)

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

        # path_folder = path_drive + folderName
        # os.mkdir(path_folder)
        # return path_folder


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

    def create_sentiment_afin_textblob():

        afin = creat_new_folder_drive("afin", path_drive)
        textblob = creat_new_folder_drive("textblob", path_drive)

        # statistic path
        statistic_afin_path = creat_new_folder_drive("afin_statistic", afin + '/')
        statistic_textblob_path = creat_new_folder_drive("textblob_statistic", textblob + '/')

        # sentiment  path
        sentiment_afin_path = creat_new_folder_drive("afin_sentiment", afin + '/')
        sentiment_textblob_path = creat_new_folder_drive("textblob_sentiment", textblob + '/')

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

        #counter = 0
        for post in tqdm(posts.find({})):
            #if counter==10: break
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

            #counter+=1

        # All
        afin_sent_all.draw_sentiment_time("afin", fullpath=sentiment_afin_path)
        textblob_sent_all.draw_sentiment_time("textblob", fullpath=sentiment_textblob_path)
        afin_stat_all.draw_statistic_bars(afin_sent_all,path=statistic_afin_path)
        textblob_stat_all.draw_statistic_bars(textblob_sent_all,path=statistic_textblob_path)

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

# p = stat.get_percent()
# draw_bars()
# topic = creat_Sentiment_Graph_For_Topics(path_drive="")

create_sentiment_afin_textblob()
# set = creatSentiment_and_statistic_Graph()
