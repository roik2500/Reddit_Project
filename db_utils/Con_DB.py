import csv

import pymongo
import os
from dotenv import load_dotenv
import  pandas as pd

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

    '''
    :argument
    cursor -> The object from mongo to extract the relevent data from
    post_or_comment -> str that specify if to etract posts or comments from the cursor
    :returns
    post return Array that have the title and selftext
    comment return Array that the comments separated by '..', '...' 
    '''
    def get_text_from_post_OR_comment(self, object, post_or_comment):
        if post_or_comment == 'post':
            return [object['pushift_api']['title'], object['pushift_api']['selftext']]  # return array
        elif post_or_comment == 'comment':
            return [obj['data']['body'] for obj in object['reddit_api']['comments']]  # return array

    def insert_to_db(self, reddit_post):
        self.posts_cursor.insert_one(reddit_post)

    def add_filed(self, data, filed_name='reddit_or_pushshift', collection_name="wallstreetbets"):
        '''
           This function is adding filed to post at mongoDB
           :argument filed_name: name of the field you want to add. TYPE- str
           :argument data: TYPE- str
       '''
        posts_cursor = con_db.get_cursor_from_mongodb(collection_name=collection_name)
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

            reddit_post = post['reddit_api']['post']['selftext']
            pushshift_post = post['pushift_api']['selftext']

            reddit_condition = reddit_post == '' or reddit_post == '[deleted]' \
                               or reddit_post == '[removed]'

            pushshift_condition = pushshift_post != '' and pushshift_post != '[deleted]' \
                                  and pushshift_post != '[removed]'

            pushshift_removed = pushshift_post == '[removed]'

            if reddit_condition and pushshift_condition:
                self.add_filed(data='pushift_api')
            elif pushshift_removed and not reddit_condition:
                self.add_filed(data='reddit_api')
            else:
                self.add_filed(data='reddit_api')



    '''
    This function reading Data from CSV file
    :argument path - path to csv file in this computer
    :return rows from csv
    '''
    def read_fromCSV(self,path):
        df = pd.read_csv(path)
        return df
       # f = open(path,encoding='UTF8')
       # csv_reader = csv.reader(f)
       # return csv_reader



# if __name__ == '__main__':
#     con_db = Con_DB()
#     posts_cursor = con_db.get_cursor_from_mongodb(collection_name="wallstreetbets")
#     # con_db.reoder_mongo("wallstreetbets")
#     # con_db.add_filed(filed_name='most_updated_data', data=None)
#     for obj in posts_cursor.find({}):
#         a = con_db.get_text_from_post_OR_comment(object=obj, post_or_comment='comment')
#         print(a)