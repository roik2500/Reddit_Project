'''
This class are responsible on the connection to our DB.
'''
import pymongo


class Con_DB:
    def __init__(self):
        self.myclient = pymongo.MongoClient("mongodb+srv://roi:1234@redditdata.aav2q.mongodb.net/")

    def get_posts_text(self,posts):
        return posts.find({'title': {"$exists": True}})

    # def get_post_from_csv():
    #     df = pd.read_csv('./data/removed.csv')
    #     return df

    def get_posts_from_mongodb(self):
        '''
        This function is return the posts from mongoDB
        :return:
        '''
        mydb = self.myclient["reddit"]
        posts = mydb["politics_sample"]
        return posts

