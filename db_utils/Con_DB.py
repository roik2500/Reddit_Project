import pymongo
import os
from dotenv import load_dotenv

load_dotenv()

'''
This class are responsible on the connection to our DB.
'''

#AUTH_DB="mongodb+srv://roi:1234@redditdata.aav2q.mongodb.net/"

class Con_DB:

    def __init__(self):
        self.myclient = pymongo.MongoClient(os.getenv("AUTH_DB"))
        self.posts_cursor = None

    def get_posts_text(self, posts, name):
        return posts.find({'{}'.format(name): {"$exists": True}})

    def get_cursor_from_mongodb(self, db_name="reddit", collection_name="pushift_api"):
        '''
        This function is return the posts from mongoDB
        :argument db_name: name of the db that you want to connect. TYPE- str
        :argument collection_name: collection name inside your db name. TYPE- str
        :return: data from mongodb
        '''
        mydb = self.myclient[db_name]
        self.posts_cursor = mydb[collection_name]
        return self.posts_cursor

    def insert_to_db(self, reddit_post):
        self.posts_cursor.insert_one(reddit_post)

    def add_filed(self, data, filed_name='reddit_or_pushshift'):
        '''
           This function is adding filed to post at mongoDB
           :argument filed_name: name of the field you want to add. TYPE- str
           :argument data: TYPE- str
       '''
        posts_cursor = con_db.get_cursor_from_mongodb(collection_name="politics")
        for post in posts_cursor.find({}):
            posts_cursor.insert_one({"_id": post["_id"]}, {"$set": {filed_name: data}})

    def reoder_mongo(self, collection_name):
        posts_cursor = self.get_cursor_from_mongodb(collection_name=collection_name)
        relevent_data = {}
        for post in posts_cursor.find({}):
            print(post)
            relevent_data['t3_' + post['reddit_api'][0]['data']['children'][0]['data']['id']] = \
                post['reddit_api'][0]['data']['children'][0]['data']
            self.myclient.reddit_api.save({"_id": post['_id']}, {"$set": {"reddit_api": [{0: relevent_data}]}})
            relevent_data.clear()

    def chose_relevent_data(self, collection_name):
        posts_cursor = self.get_cursor_from_mongodb(collection_name=collection_name)
        for post in posts_cursor.find({}):

            reddit_post = post['reddit_api'][0]['data']['children'][0]['data']['selftext']
            pushshift_post = post['pushshift_api'][0]['data']['children'][0]['data']['selftext']

            reddit_condition = reddit_post == '' or reddit_post == '[deleted]' \
                               or reddit_post == '[removed]'

            pushshift_condition = pushshift_post != '' and pushshift_post != '[deleted]' \
                                  and pushshift_post != '[removed]'

            if reddit_condition and pushshift_condition:
                self.add_filed(data='pushshift')
            else:
                self.add_filed(data='reddit')


if __name__ == '__main__':
    con_db = Con_DB()
    # posts_cursor = con_db.get_cursor_from_mongodb(collection_name="politics")
    # con_db.reoder_mongo("wallstreetbets")
    con_db.add_filed(filed_name='most_updated_data', data=None)
