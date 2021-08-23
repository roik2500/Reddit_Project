'''
This class are responsible on the connection to our DB.
'''


class Con_DB:
    def get_posts_text(posts):
        return posts.find({'title': {"$exists": True}})

    def get_post_from_csv():
        df = pd.read_csv('./data/removed.csv')
        return df

    def get_posts_from_mongodb():
        '''
        This function is return the posts from mongoDB
        :return:
        '''
        myclient = pymongo.MongoClient("mongodb+srv://roi:1234@redditdata.aav2q.mongodb.net/")
        mydb = myclient["reddit"]
        posts = mydb["politics_sample"]
        return posts
