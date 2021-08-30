'''
This class are responsible on the connection to our DB.
'''
import pymongo
import os
from dotenv import load_dotenv
load_dotenv()

class Con_DB:

    def __init__(self):
        self.myclient = pymongo.MongoClient("mongodb+srv://roi:1234@redditdata.aav2q.mongodb.net/")


    def get_posts_text(self,posts,name):
        return posts.find({'{}'.format(name): {"$exists": True}})


    def get_posts_from_mongodb(self, db_name="reddit", collection_name="pushift_api"):
        '''
        This function is return the posts from mongoDB
        :argument db_name: name of the db that you want to connect. TYPE- str
        :argument collection_name: collection name inside your db name. TYPE- str
        :return: data from mongodb
        '''
        myclient = pymongo.MongoClient(os.getenv("AUTH_DB"))
        mydb = myclient[db_name]
        posts = mydb[collection_name]
        return posts

    # def get_posts_from_mongodb(self,name_of_doc):
    #     '''
    #     This function is return the posts from mongoDB
    #     :return:
    #     '''
    #     mydb = self.myclient["reddit"]
    #     posts = mydb["{}".format(name_of_doc)]
    #     return posts