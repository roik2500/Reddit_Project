import json
from pprint import pprint
import pymongo
import os
from datasets import tqdm
from bson.json_util import dumps
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
from db_utils.FileReader import FileReader
from api_utils.PushshiftApi import PushshiftApi
from api_utils.reddit_api import reddit_api
import json
from bson.json_util import dumps

from text_analysis.Parser import Parser

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
        self.file_reader = FileReader()
        # self.path = "/home/shouei/wallstreetbets_2020_full_.json"
        self.path = os.getenv('DATA_PATH')
        self.pushshift_api = PushshiftApi()
        self.reddit_api = reddit_api()
        self.parser = Parser()

    def setAUTH_DB(self, num):
        self.myclient = pymongo.MongoClient(os.getenv("AUTH_DB{}".format(num)))

    def get_posts_text(self, posts, name):
        return posts.find({'{}'.format(name): {"$exists": True}})

    def convert_time_format(self, comment_or_post):
        time = datetime.fromtimestamp(
            int(comment_or_post)).isoformat().split(
            "T")
        return time

    def get_data_cursor(self, source_name, source_type):
        if source_type == 'json':
            items = ijson.items(open("{}\{}.json".format(os.getenv("DATA_DIR"), source_name), 'rb'), 'item')
        elif source_type == 'mongo':
            items = self.myclient["reddit"][source_name]
        return items

    def get_cursor_from_mongodb(self, db_name="reddit", collection_name=os.getenv("COLLECTION_NAME")):
        '''
        This function is return the posts from mongoDB
        :argument db_name: name of the db that you want to connect. TYPE- str
        :argument collection_name: collection name inside your db name. TYPE- str
        :return: data from mongodb
        '''
        #         db_name = 'local'
        print("db_name", db_name)
        print("collection_name", collection_name)
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

    def get_collections_name_from_db(self, db_name):
        return [collection for collection in self.myclient[db_name].list_collection_names()]

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

    def comments_to_lists(self, comments, created, link_id):
        results = []  # create list for results
        for comment in comments:  # iterate over comments
            if 'created_utc' in comment['data']:
                created = self.convert_time_format(comment['data']['created_utc'])[0]
            _id = comment['data']['id']
            is_removed = self.is_removed(comment, "comment", "Removed")
            text = ''
            if "body" in comment["data"] and not comment['data']['body'] == "[removed]" and comment['data'][
                'body'] != "[deleted]":  # we comment text from reddit. Not Removed content
                text = comment['data']['body']

            elif "body" in comment["data"] and comment['data'][
                'body'] == "[removed]":  # the comment was removed by mods
                text = comment['data']['body']
                # print('comment_id',  comment['data']['id'])
                # print('reddit comment content', comment['data']['body'])
                # print('reddit user', comment['data']['author'])
                # pushshift_comment = self.pushshift_api.get_comments_by_comments_ids(ids_list=[id])
                # text = [c for c in pushshift_comment][0]['body']
                # if text != '[removed]' and comment['data']['author'] != '[deleted]':
                #    all_user_commnets = self.reddit_api.get_user_commnets(reddit_user_name=comment['data']['author'])
                #    text = self.reddit_api.get_removed_comment_from_reddit(user_comments=all_user_commnets, removed_comment_id=id)
                # print('retrived comment from user profile', text)

            item = [[text, created, _id, link_id, is_removed]]  # create list from comment
            # item = [comment]
            if 'replies' in comment["data"] and len(comment['data']['replies']) > 0:
                replies = comment['data']['replies']['data']['children']
                item += self.comments_to_lists(replies, created=created,
                                               link_id=link_id)  # convert replies using the same function item["replies"] = k4lz6m

            results += item  # add converted item to results
        return results  # return all converted comments

    def get_text_from_post_OR_comment(self, _object, post_or_comment):
        if post_or_comment == 'post':
            id = _object['post_id']
            created = _object['reddit_api']['post']['created_utc'][0]
            text = _object['reddit_api']['post']['title']
            is_removed = self.is_removed(_object, "comment", "Removed")
            if "selftext" in _object['pushift_api'].keys() and _object['pushift_api']["selftext"] != "[removed]" \
                    and _object['pushift_api']["selftext"] != "[deleted]":
                text = text + " " + _object['pushift_api']["selftext"]
            text = self.parser.remove_URL(text)
            return [[text, created, id, is_removed]]

        elif post_or_comment == 'comment':
            # result = self.comments_to_lists(_object['reddit_api']['comments'], '2020-01-01', _object['post_id'])
            # if 'link_id' in _object['data']:
            #     result = self.comments_to_lists([_object], '2020-01-01', _object['data']['link_id'])
            # else:
            #     result = self.comments_to_lists([_object], '2020-01-01', "000")
            Id = _object['data']['id']
            if 'created_utc' in _object['data']:
                created = self.convert_time_format(_object['data']['created_utc'])[0]
                text = _object['data']['body']
                if 'pushshift_body' in _object['data'].keys() and _object['data']['pushshift_body'] != '':
                    text = _object['data']['pushshift_body']
                is_removed = self.is_removed(_object, "comment", "Removed")
            else:
                return [None]
            return [[text, created, Id, is_removed]]

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

    ''' This function set the self text that was removed from reddit and insert it to the pushsift section'''

    def set_removed_comments_in_mongo(self):
        post_id_removed_comments = {}
        self.get_cursor_from_mongodb()
        update_cursor = self.posts_cursor
        posts_number = 0
        comment_number = 0
        # for obj in self.posts_cursor.find({}):
        #     post_id_removed_comments[obj['post_id']] = []
        #     comments = self.comments_to_lists(obj['reddit_api']['comments'], '2020-01-01', obj['post_id'])
        #     for comment in comments:
        #         if comment[0] == '[removed]' and comment[-1] == True:
        #             post_id_removed_comments[obj['post_id']].append(comment[2])
        #     if len(post_id_removed_comments[obj['post_id']]) == 0:
        #         del post_id_removed_comments[obj['post_id']]
        #         continue
        #     else:
        #         ids = post_id_removed_comments[obj['post_id']]
        #         try:
        #             pushshift_comments = self.pushshift_api.get_comments_by_comments_ids(ids_list=ids)
        #         except:
        #             continue
        #         pushshift_comments = [c for c in pushshift_comments]
        #         id_to_comment_body = {}
        #         for pushshift_comment in pushshift_comments:
        #             id_to_comment_body[pushshift_comment['id']] = pushshift_comment['body']
        #         posts_number += 1
        #         print("posts_number", posts_number)
        #         comment_number += len(pushshift_comments)
        #         print("comment_number", comment_number)
        #         update_cursor.update_one({"_id": obj['_id']}, {"$set": {"body_removed_comment": id_to_comment_body}})
        removed_comments_iter = self.posts_cursor.find({"data.body": "[removed]"})
        removed_comments_ids_from_pushshift = [c['data']['id'] for c in removed_comments_iter]
        pushshift_comments = self.pushshift_api.get_comments_by_comments_ids(
            ids_list=removed_comments_ids_from_pushshift)
        pushshift_comments = [(c['id'], c['body']) for c in pushshift_comments if
                              c['body'] != '[removed]' and (c['body'] != '')]
        for comment_id, comment_body in pushshift_comments:
            if comment_number % 1000 == 0:
                print("comment_number - retrived", comment_number)
            # post_id_removed_comments[obj['post_id']] = []
            # comments = self.comments_to_lists(obj['reddit_api']['comments'], '2020-01-01', obj['post_id'])
            # for comment in comments:
            #     if comment[0] == '[removed]' and comment[-1] == True:
            #         post_id_removed_comments[obj['post_id']].append(comment[2])
            # if len(post_id_removed_comments[obj['post_id']]) == 0:
            #     del post_id_removed_comments[obj['post_id']]
            #     continue
            # else:
            #     ids = post_id_removed_comments[obj['post_id']]
            #     try:
            #         pushshift_comments = self.pushshift_api.get_comments_by_comments_ids(ids_list=ids)
            #     except:
            #         continue
            #     pushshift_comments = [c for c in pushshift_comments]
            #     id_to_comment_body = {}
            #     for pushshift_comment in pushshift_comments:
            #         id_to_comment_body[pushshift_comment['id']] = pushshift_comment['body']
            #     posts_number += 1
            # print("posts_number", posts_number)
            comment_number += 1
            # print("comment_number", comment_number)
            update_cursor.update_one({"data.id": comment_id}, {"$set": {"data.pushshift_body": comment_body}})
        print("comment_number - retrived", comment_number)
        print("DONE")

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

    # def get_specific_items_by_post_ids(self, ids_list, post_or_comment_arg):
    #    # file_reader_new = FileReader()
    # data_cursor = file_reader_new.get_json_iterator(self.path)
    #    data_cursor = self.posts_cursor.find(
    #       {u'pushift_api.id': {'$in': ids_list}}
    #    )
    #    text_and_date_list = []
    #    for item in data_cursor:
    #        if item['pushift_api']['id'] in ids_list:
    #            text_and_date_list.append(self.get_text_from_post_OR_comment(item, post_or_comment=post_or_comment_arg))
    #    return text_and_date_list  # [title , selftext ,created_utc, 'id']

    def get_posts_by_ids(self, posts_ids):
        posts_cursor = self.get_cursor_from_mongodb(collection_name=os.getenv("COLLECTION_NAME"))
        return self.posts_cursor.find(
            {u'pushift_api.id': {'$in': posts_ids}}
        )

    def get_specific_items_by_object_ids(self, ids_list, post_or_comment_arg):
        if post_or_comment_arg == 'post':
            file_reader_new = FileReader()
            # db_name = 'local'
            # mydb = self.myclient[db_name]
            # posts_cursor = mydb[os.getenv("COLLECTION_NAME")]
            # posts_cursor = self.posts_cursor
            # data_cursor = file_reader_new.get_json_iterator(self.path)
            data_cursor = self.posts_cursor.find(
                {u'post_id': {'$in': ids_list}}
            )
            text_and_date_list = []
            for item in data_cursor:
                if item['post_id'] in ids_list:
                    text_and_date_list.append(
                        self.get_text_from_post_OR_comment(item, post_or_comment=post_or_comment_arg))
        else:
            text_and_date_list = []
            pushshift_comments = self.pushshift_api.get_comments_by_comments_ids(ids_list)
            created = ''
            for comment in pushshift_comments:
                if 'created_utc' in comment:
                    created = self.convert_time_format(comment['created_utc'])[0]
                id = comment['id']
                comment_data = {'data': comment}
                is_removed = self.is_removed(comment_data, "comment", "Removed")
                text = ''
                if "body" in comment and not comment['body'].__contains__(
                        "[removed]") and comment['body'] != "[deleted]":
                    text = comment['body']
                link_id = comment['link_id'][3:]
                text_and_date_list.append([text, created, link_id, id, is_removed])

        return text_and_date_list  # [text, created, link_id, id, is_removed]

    def get_specific_items_by_object_ids_from_json(self, ids_list, post_or_comment_arg):

        if post_or_comment_arg == 'post':
            file_reader_new = FileReader()
            data_cursor = file_reader_new.get_json_iterator(self.path)
            # data_cursor = self.posts_cursor.find(
            #    {u'pushift_api.id': {'$in': ids_list}}
            # )
            text_and_date_list = []
            for item in data_cursor:
                if item['post_id'] in ids_list:
                    text_and_date_list.append(
                        self.get_text_from_post_OR_comment(item, post_or_comment=post_or_comment_arg))
        else:
            text_and_date_list = []
            pushshift_comments = self.pushshift_api.get_comments_by_comments_ids(ids_list)
            created = ''
            for comment in pushshift_comments:
                if 'created_utc' in comment:
                    created = self.convert_time_format(comment['created_utc'])[0]
                id = comment['id']
                comment_data = {'data': comment}
                is_removed = self.is_removed(comment_data, "comment", "Removed")
                text = ''
                if "body" in comment and not comment['body'].__contains__(
                        "[removed]") and comment['body'] != "[deleted]":
                    text = comment['body']
                link_id = comment['link_id'][3:]
                text_and_date_list.append([text, created, link_id, id, is_removed])

        return text_and_date_list  # [text, created, link_id, id, is_removed]

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
                    if post['reddit_api']['post']['is_robot_indexable'] == False:
                        if post['reddit_api']['post']['selftext'] == "[removed]":
                            return True
                        elif post['reddit_api']['post']['selftext'].__contains__('[removed]'):
                            return True  # this is pool
                        elif post['reddit_api']['post']['selftext'] == '':
                            return True  # blanked
                        else:
                            return True  # shadow ban

                    return False  # exists

                    if not post['reddit_api']['post']['selftext'].__contains__('[deleted]'):
                        return not post['reddit_api']['post']['is_robot_indexable']
                    return False
            # if 'selftext' in post['reddit_api']['post'].keys():
            #     if category == "Removed":
            #         if post['reddit_api']['post']['selftext'].__contains__('[removed]'):
            #             return True
            #         return False
            #     elif category == "NotRemoved":
            #         if not post['reddit_api']['post']['selftext'].__contains__('[removed]'):
            #             return True
            return not post['reddit_api']['post']['is_robot_indexable']
        elif post_comment == "comment":
            if 'body' in post['data'].keys():
                if category == "Removed":
                    if post['data']['body'] == '[removed]':
                        return True
                    return False
                elif category == "NotRemoved":
                    if not post['data']['body'] == '[removed]':
                        return True
            return True  # was False and shai change to True because if there is no body so it was removed

    '''
       This function reading Data from CSV file
       :argument path - path to csv file in this computer
       :return rows from csv
    '''

    def read_fromCSV(self, path):
        df = pd.read_csv(path, encoding='UTF8')
        return df

    '''
    This function making a json file from topic's csv file.
    the function filtering the large collection in MongoDB of full posts wsb 2020 and export the collection after filtering to JSON file
    :argument path_to_csv - full path to csv file of specific topic
    :argument path_to_save_json - full path of directory of saving the json file 
    :argument collection_name - the name of collection(must be exactly the same name that wrote in MongoDB)
    '''

    def fromCSVtoJSON(self, path_to_csv, path_to_save_json, collection_name):
        topic_csv = self.read_fromCSV(path_to_csv)
        posts = self.get_cursor_from_mongodb(collection_name=collection_name)
        dff = list(topic_csv["post_id"].unique())
        with open('{}/{}.json'.format(path_to_save_json, collection_name), 'w') as file:
            cursor = posts.find({'post_id': {'$in': dff}}).max_time_ms(1000000)
            file.write('[')
            for document in cursor:
                file.write(dumps(document))
                file.write(',')
            file.write(']')

    def read_posts_from_mongo_write_to_json(self, ids_list, file_name):
        path = os.getenv('NER_POSTS_FOLDER') + file_name
        posts = self.get_specific_items_by_object_ids_from_mongodb(ids_list=ids_list, post_or_comment_arg='posts')
        self.file_reader.write_dict_to_json(path=path, file_name=file_name, dict_to_write=posts.__dict__)

    def get_post_id_from_json_and_write_full_posts(self):
        path = os.getenv('NER_POSTS_FOLDER') + 'True_NER_per_month.json'
        dict_file = self.file_reader.read_from_json_to_dict(path)
        for key, val in dict_file.items():
            for NER, v in val.items():
                cursor = self.get_cursor_from_mongodb()
                NER_TYPE = NER + "_" + v[0][0]
                post_from_mongo = self.get_full_data_by_object_ids_from_mongodb(ids_list=v[1][1], post_or_comment_arg='post',
                                                                                NER_TYPE=NER_TYPE)
                NER_POSTS_FOLDER=''
                path = NER_POSTS_FOLDER + "NER_posts\\Posts_full"
                file_name = NER_TYPE + ".json"
                self.file_reader.write_dict_to_json(path=path,file_name=file_name, dict_to_write=post_from_mongo)


    def get_NER_full_post_data(self, NER_TYPE):
        path = os.getenv('NER_POSTS_FOLDER') + NER_TYPE
        return self.file_reader.read_from_json_to_dict(PATH=path)

# con_db = Con_DB()
# cursor = con_db.get_cursor_from_mongodb()
# data = con_db.get_specific_items_by_object_ids(ids_list=['eici5m'], post_or_comment_arg='post')
# print(data)