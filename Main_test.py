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
    #path = 'C:/Users/roik2/PycharmProjects/Reddit_Project/data/document_topic_table_general.csv'  # Change to your path in your computer
    path_csv = 'C:/Users/roik2/PycharmProjects/Reddit_Project/data/document_topic_table_general_best .csv'  # Change to your path in your computer
    path_drive = 'G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/'

    # define a objects for tests
    api_all = Sentiment("wallstreetbets","All")
    api_Removed = Sentiment("wallstreetbets","All")
    api_Not_Removed = Sentiment("wallstreetbets","All")

    stat_all = Statistic("wallstreetbets","All")
    stat_api_Removed = Statistic("wallstreetbets","Removed")
    stat_Not_Removed = Statistic("wallstreetbets","NotRemoved")

    '''
    This function creating graphs for all the topics (graph per topic)
    :argument path - path to csv file that contains the data of all topics
    :return graphs -> graph per topic
    '''
    def creat_Sentiment_Graph_For_Topics(path_drive=""):
        full_path = path_drive + "sentiment for topics/"
        topic_set = Sentiment()
        topic_csv = con.read_fromCSV(path_csv)
        for topic_id in tqdm(topic_csv["Dominant_Topic"].unique()):
            dff = topic_csv[topic_csv["Dominant_Topic"] == topic_id]
            for post_id in tqdm(dff['post_id']):
               post = posts.find({"post_id":post_id})
               if post.count() == 0: continue
               post = posts.find({"post_id":post_id})[0]
               topic_set.update_sentiment_values(post,"All","month") # iteration per month or per year

            # topic_set.draw_sentiment_time("polarity", "wallstreetbets", "All",topic_id,path_drive)
            # topic_set.draw_sentiment_time("subjectivity", "wallstreetbets", "All",topic_id,path_drive)

            # topic_set.draw_sentiment_time("polarity", "wallstreetbets", "All")
            # topic_set.draw_sentiment_time("subjectivity", "wallstreetbets", "All")


    '''
    Creating a new folder in Google drive. 
    :argument folderName - name of the new folder
    :return full path to the new folder
    '''
    def creat_new_folder_drive(folderName):
        path_folder = path_drive + folderName
        os.mkdir(path_folder)
        return path_folder


    '''
    Test function
    This function creating graphs for posts are Removed/NotRemove/All
    '''
    def creatSentiment_and_statistic_Graph():

        for post in tqdm(posts.find({})):
            # text = con.get_posts_text(posts,"title")
            api_all.update_sentiment_values(post, "All","month")
            stat_all.precentage_media(con, post,"All")

           # api_Removed.update_sentiment_values(post, "Removed","month")
           # stat_api_Removed.precentage_media(con, post,"Removed")

           # api_Not_Removed.update_sentiment_values(post, "NotRemoved","month")
           # stat_Not_Removed.precentage_media(con, post,"NotRemoved")

        stat_all.get_percent("All")
       # stat_api_Removed.get_percent("Removed")
       # stat_Not_Removed.get_percent("NotRemoved")


        api_all.draw_sentiment_time("polarity", "wallstreetbets", "All")
        #api_Removed.draw_sentiment_time("polarity", "wallstreetbets", "Removed")
        #api_Not_Removed.draw_sentiment_time("polarity", "wallstreetbets", "NotRemoved")

        api_all.draw_sentiment_time("subjectivity", "wallstreetbets", "All")
        #api_Removed.draw_sentiment_time("subjectivity", "wallstreetbets", "Removed")
       # api_Not_Removed.draw_sentiment_time("subjectivity", "wallstreetbets", "NotRemoved")

    def draw_bars():
        creatSentiment_and_statistic_Graph()
        stat_all.draw_statistic_bars(api_all)


# p = stat.get_percent()
#draw_bars()
#topic = creat_Sentiment_Graph_For_Topics(path_drive="")
set = creatSentiment_and_statistic_Graph()