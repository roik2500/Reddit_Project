import json

import pymongo
import os
from datasets import tqdm, dumps
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
from db_utils.FileReader import FileReader

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
        self.myclient = pymongo.MongoClient(os.getenv("AUTH_DB1"))
        self.posts_cursor = None
        self.file_reader = FileReader()
        self.path = "/home/shouei/wallstreetbets_2020_full_.json"

    def setAUTH_DB(self, num):
        self.myclient = pymongo.MongoClient(os.getenv("AUTH_DB{}".format(num)))

    def get_posts_text(self, posts, name):
        return posts.find({'{}'.format(name): {"$exists": True}})

    def convert_time_format(self, comment_or_post):
        time = datetime.fromtimestamp(
            int(comment_or_post)).isoformat().split(
            "T")
        return time

    # def get_cursor_from_json(self, file_name):
    #     self.posts_cursor = json.load(file_name)
    #     return self.posts_cursor

    def get_cursor_from_mongodb(self, db_name="reddit", collection_name=os.getenv("COLLECTION_NAME")):
        '''
        This function is return the posts from mongoDB
        :argument db_name: name of the db that you want to connect. TYPE- str
        :argument collection_name: collection name inside your db name. TYPE- str
        :return: data from mongodb
        '''
        #         db_name = 'local'
        mydb = self.myclient[db_name]
        self.posts_cursor = mydb[collection_name]
        return self.posts_cursor
        # return self.file_reader.get_json_iterator(self.path)

    def searchConnectionByPostId(self, post_id):
        for i in range(1, 5):
            temp_id = self.setAUTH_DB(i)
            p = self.get_cursor_from_mongodb()
            post = p.find({"post_id": post_id})
            if post.count() == 0:
                continue
            else:
                return p

    '''
    :argument
    cursor -> The object from mongo to extract the relevent data from
    post_or_comment -> str that specify if to etract posts or comments from the cursor
    :returns
    post return Array that have the title and selftext
    comment return Array that the comments separated by '..', '...' 
    '''

    # {"body": comment['data']['body'],
    #             "created": self.convert_time_format(comment['data']['created_utc'])[0],
    #             "id": comment['data']['id'],
    #             "link_id": comment['data']['link_id'][2:],
    #             "is_removed": self.is_removed(comment, "comment", "Removed")}

    def comments_to_dicts(self, comments, created):
        results = []  # create list for results
        # created = '2020-10-10'
        for comment in comments:  # iterate over comments
            if 'created_utc' in comment['data']:
                created = self.convert_time_format(comment['data']['created_utc'])[0]
            id = comment['data']['id']
            is_removed = self.is_removed(comment, "comment", "Removed")
            text = ''
            if "body" in comment["data"] and not comment['data']['body'].__contains__(
                    "[removed]") and comment['data']['body'] != "[deleted]":
                text = comment['data']['body']
            link_id = ['link_id'][2:]

            item = [[text, created, id, comment['data'], link_id, is_removed]]  # create list from comment

            if 'replies' in comment["data"] and len(comment['data']['replies']) > 0:
                replies = comment['data']['replies']['data']['children']
                item += self.comments_to_dicts(replies,
                                               created=created)  # convert replies using the same function item["replies"] =

            results += item  # add converted item to results
        return results  # return all converted comments

    def get_text_from_post_OR_comment(self, object, post_or_comment):
        if post_or_comment == 'post':
            id = object['pushift_api']['id']
            created = object['reddit_api']['post']['created_utc'][0]
            text = object['pushift_api']['title']
            is_removed = self.is_removed(object, "post", "Removed")
            if "selftext" in object['pushift_api'].keys() and not object['pushift_api']["selftext"].__contains__(
                    "[removed]") and object['pushift_api']["selftext"] != "[deleted]":
                text = text + " " + object['pushift_api']["selftext"]

            return [[text, created, id, is_removed]]

        elif post_or_comment == 'comment':
            result = self.comments_to_dicts(object['reddit_api']['comments'], '2020-01-01')
            # res = []
            # created = ''
            # link_id = object['post_id']
            # for obj in object['reddit_api']['comments']:
            #         Id = obj['data']['id']
            #         if 'created_utc' in obj['data']:
            #             created = self.convert_time_format(obj['data']['created_utc'])[0]
            #         is_removed = self.is_removed(obj, "comment", "Removed")
            #         text = ''
            #         if "body" in obj["data"] and not obj['data']['body'].__contains__(
            #                 "[removed]") and obj['data']['body'] != "[deleted]":
            #             text = obj['data']['body']
            #         res.append([text, created, Id, link_id, is_removed])
            return result

    def insert_to_db(self, reddit_post):
        self.posts_cursor.insert_one(reddit_post)

    async def update_to_db(self, Id, reddit_post):
        myquery = {"post_id": Id}
        newvalues = {"$set": {"reddit_api": reddit_post}}
        self.posts_cursor.update_one(myquery, newvalues)

    def add_filed(self, data, filed_name='reddit_or_pushshift', collection_name=os.getenv("COLLECTION_NAME")):
        '''
           This function is adding filed to post at mongoDB
           :argument filed_name: name of the field you want to add. TYPE- str
           :argument data: TYPE- str
       '''
        posts_cursor = self.get_cursor_from_mongodb(collection_name=collection_name)
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

    def get_specific_items_by_post_ids(self, ids_list, post_or_comment_arg):
        # file_reader_new = FileReader()
        # data_cursor = file_reader_new.get_json_iterator(self.path)
        data_cursor = self.posts_cursor.find(
            {u'pushift_api.id': {'$in': ids_list}}
        )
        text_and_date_list = []
        for item in data_cursor:
            if item['pushift_api']['id'] in ids_list:
                text_and_date_list.append(self.get_text_from_post_OR_comment(item, post_or_comment=post_or_comment_arg))
        return text_and_date_list  # [title , selftext ,created_utc, 'id']

    ''' return posts by category'''

    def get_data_categories(self, category, collection_name):
        posts = self.get_cursor_from_mongodb(db_name="reddit", collection_name=collection_name)
        posts_to_return = []
        for post in posts.find({}):
            if 'selftext' in post['pushift_api'].keys():
                if category == "Removed":
                    if post['reddit_api']['post']['selftext'] == "[removed]":
                        posts_to_return.append(post)

                elif category == "NotRemoved":
                    if post['reddit_api']['post']['selftext'] != "[removed]":
                        posts_to_return.append(post)

                elif category == "All":
                    posts_to_return.append(post)
        return posts_to_return

    def is_removed(self, post, post_comment, category):
        if post_comment == "post":
            if 'selftext' in post['reddit_api']['post'].keys():
                if category == "Removed":
                    if post['reddit_api']['post']['selftext'].__contains__('[removed]'):
                        return True
                    return False
                elif category == "NotRemoved":
                    if not post['reddit_api']['post']['selftext'].__contains__('[removed]'):
                        return True
            return False
        elif post_comment == "comment":
            if 'body' in post["data"].keys():
                if category == "Removed":
                    if post["data"]['body'].__contains__('[removed]'):
                        return True
                    return False
                elif category == "NotRemoved":
                    if not post["data"]['body'].__contains__('[removed]'):
                        return True
            return True  # was False and shai change to True because if there is no body so it was removed

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

    def postsBYtopic(self, path_to_csv, path_to_save_json, collection_name):
        topic_csv = self.read_fromCSV(path_to_csv)
        posts = self.get_cursor_from_mongodb(collection_name=collection_name)
        for topic_id in tqdm(topic_csv["Dominant_Topic"].unique()):
            dff = list(topic_csv[topic_csv["Dominant_Topic"] == topic_id]['post_id'])
            cursor = posts.find({'post_id': {'$in': dff}})
            with open('{}/{}.json'.format(path_to_save_json, collection_name), 'w') as file:
                file.write('[')
                for document in cursor:
                    file.write(dumps(document))
                    file.write(',')
                file.write(']')
