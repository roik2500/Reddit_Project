from tqdm import tqdm
from statistics_analysis.Statistics import Statistic
from db_utils.Con_DB import Con_DB
from text_analysis.Sentiment import Sentiment
import csv

if __name__ == '__main__':
    con = Con_DB()
    posts = con.get_cursor_from_mongodb(collection_name="wallstreetbets")
    path = 'C:/Users/roik2/PycharmProjects/Reddit_Project/data/document_topic_table_general.csv'  # Change to your path in your computer


    '''
    This function creating graphs for all the topics (graph per topic)
    :argument path - path to csv file that contains the data of all topics
    :return graphs -> graph per topic
    '''
    def creat_Sentiment_Graph_For_Topics(path):
        topic_set = Sentiment()
        topic_csv = con.read_fromCSV(path)
        for topic_id in tqdm(topic_csv["Dominant_Topic"].unique()):
            dff = topic_csv[topic_csv["Dominant_Topic"] == topic_id]
            for post_id in tqdm(dff['post_id']):
               post = posts.find({"post_id":post_id})[0]
               topic_set.update_sentiment_values(post,"All","year") # iteration per month or per year
            topic_set.draw_sentiment_time("polarity", "wallstreetbets", "All")
            topic_set.draw_sentiment_time("subjectivity", "wallstreetbets", "All")
            break

    '''
    Test function
    This function creating graphs for posts are Removed/NotRemove/All
    '''
    def creatSentimentGraph():
        api_all = Sentiment()
        api_Removed = Sentiment()
        api_Not_Removed = Sentiment()

        stat_all = Statistic()
        stat_api_Removed = Statistic()
        stat_Not_Removed = Statistic()

        for post in tqdm(posts.find({})):
            # text = con.get_posts_text(posts,"title")
            api_all.update_sentiment_values(post, "All","year")
            stat_all.precentage_media(con, post)

            api_Removed.update_sentiment_values(post, "Removed","year")
            stat_api_Removed.precentage_media(con, post)

            api_Not_Removed.update_sentiment_values(post, "NotRemoved","year")
            stat_Not_Removed.precentage_media(con, post)

        api_all.draw_sentiment_time("polarity", "wallstreetbets", "All")
        api_Removed.draw_sentiment_time("polarity", "wallstreetbets", "Removed")
        api_Not_Removed.draw_sentiment_time("polarity", "wallstreetbets", "NotRemoved")

        api_all.draw_sentiment_time("subjectivity", "wallstreetbets", "All")
        api_Removed.draw_sentiment_time("subjectivity", "wallstreetbets", "Removed")
        api_Not_Removed.draw_sentiment_time("subjectivity", "wallstreetbets", "NotRemoved")

    # p = stat.get_percent()
topic = creat_Sentiment_Graph_For_Topics(path)