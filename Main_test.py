from tqdm import tqdm

from statistics_analysis.Statistics import Statistic
from db_utils.Con_DB import Con_DB
from text_analysis.Sentiment import Sentiment

if __name__ == '__main__':

    api = Sentiment()
    con = Con_DB()
    stat = Statistic()

    posts = con.get_cursor_from_mongodb(collection_name="politics")
    for post in tqdm(posts.find({})):
        # text = con.get_posts_text(posts,"title")
        #api.update_sentiment_values(post)
        stat.precentage_media(con,post)

    #api.draw_sentiment_time()
    p = stat.get_percent()

