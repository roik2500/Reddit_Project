import pymongo
import os
from dotenv import load_dotenv
from pprint import pprint
import pandas as pd

load_dotenv()

'''
This class are responsible on the connection to our DB.
'''
'''
number of comments in all data = 107,365

dict key: post <-> value:comments number = 9858

removed posts = 7536

removed post that have comments = 4187
'''


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
            id = object['pushift_api']['id']
            created = object['pushift_api']['created_utc'][0]
            text = object['pushift_api']['title']

            if "selftext" in object['pushift_api'].keys() and not object['pushift_api']["selftext"].__contains__(
                    "[removed]") and object['pushift_api']["selftext"] != "[deleted]":
                text = text + " " + object['pushift_api']["selftext"]

            return [text, created, id]

        elif post_or_comment == 'comment':
            return [(obj['data']['body'], obj['data']['created_utc'][0], obj['data']['id']) for obj in
                    object['reddit_api']['comments']]  # return array

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

    def reorder_mongo(self, collection_name):
        posts_cursor = self.get_cursor_from_mongodb(collection_name=collection_name)
        relevent_data = {}
        for post in posts_cursor.find({}):
            print(post)
            relevent_data['t3_' + post['reddit_api'][0]['data']['children'][0]['data']['id']] = \
                post['reddit_api'][0]['data']['children'][0]['data']
            self.myclient.reddit_api.save({"_id": post['_id']}, {"$set": {"reddit_api": [{0: relevent_data}]}})
            relevent_data.clear()

    def chose_relevant_data(self, collection_name):
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

    def get_specific_items_by_post_ids(self, ids_list):
        cursor = self.posts_cursor.find(
            {u'pushift_api.id': {'$in': ids_list}}
        )
        # return cursor
        text_and_date_list = []
        for post in cursor:
            text_and_date_list.append(self.get_text_from_post_OR_comment(post, post_or_comment='post'))
        return text_and_date_list  # [title , selftext ,created_utc, 'id']

    ''' return posts by category'''

    def get_data_categories(self, category, collection_name):
        posts = self.get_cursor_from_mongodb(db_name="reddit", collection_name=collection_name)
        posts_to_return = []
        for post in posts.find({}):
            if 'selftext' in post['pushift_api'].keys():
                if category == "Removed":
                    if post['pushift_api']['selftext'] == "[removed]":
                        posts_to_return.append(post)

                elif category == "NotRemoved":
                    if post['reddit_api']['post']['selftext'] != "[removed]":
                        posts_to_return.append(post)

                elif category == "All":
                    posts_to_return.append(post)
        return posts_to_return

    def is_removed(self, post, category):

        if 'selftext' in post['pushift_api'].keys():

            if category == "Removed":
                if post['reddit_api']['selftext'] == "[removed]":
                    return True

            elif category == "NotRemoved":
                if post['reddit_api']['post']['selftext'] != "[removed]":
                    return True

        return False

    '''
       This function reading Data from CSV file
       :argument path - path to csv file in this computer
       :return rows from csv
       '''

    def read_fromCSV(self, path):
        df = pd.read_csv(path)
        return df
    # f = open(path,encoding='UTF8')
    # csv_reader = csv.reader(f)
    # return csv_reader


if __name__ == '__main__':
    con_db = Con_DB()
    posts_cursor = con_db.get_cursor_from_mongodb(collection_name="wallstreetbets")
    # con_db.reoder_mongo("wallstreetbets")
    # con_db.add_filed(filed_name='most_updated_data', data=None)
    # for obj in posts_cursor.find({}):
    #     a = con_db.get_text_from_post_OR_comment(object=obj, post_or_comment='comment')
    #     print(a)

    pprint(con_db.get_specific_items_by_post_ids(ids_list=['hjac6l', 'hjadxl', 'hjaa0v']))
