from tqdm import tqdm

from statistics_analysis.Statistics import Statistic
from db_utils.Con_DB import Con_DB
from text_analysis.Sentiment import Sentiment

if __name__ == '__main__':
    con = Con_DB()

    api_all = Sentiment()
    api_Removed = Sentiment()
    api_Not_Removed = Sentiment()

    stat_all = Statistic()
    stat_api_Removed = Statistic()
    stat_Not_Removed = Statistic()

    posts = con.get_cursor_from_mongodb(collection_name="wallstreetbets")

    for post in tqdm(posts.find({})):
        # text = con.get_posts_text(posts,"title")
        api_all.update_sentiment_values(post,"All")
        stat_all.precentage_media(con, post)

        api_Removed.update_sentiment_values(post,"Removed")
        stat_api_Removed.precentage_media(con, post)

        api_Not_Removed.update_sentiment_values(post,"NotRemoved")
        stat_Not_Removed.precentage_media(con, post)

    api_all.draw_sentiment_time("polarity", "wallstreetbets","All")
    api_Removed.draw_sentiment_time("polarity", "wallstreetbets","Removed")
    api_Not_Removed.draw_sentiment_time("polarity", "wallstreetbets","NotRemoved")

    api_all.draw_sentiment_time("subjectivity", "wallstreetbets","All")
    api_Removed.draw_sentiment_time("subjectivity", "wallstreetbets","Removed")
    api_Not_Removed.draw_sentiment_time("subjectivity", "wallstreetbets","NotRemoved")

    # p = stat.get_percent()
