from psaw import PushshiftAPI
from pmaw import PushshiftAPI as PushshiftApiPmaw
import datetime
import requests
import json
import pandas as pd
import pymongo
import os
import csv

class FileReader:

    def read_from_json(self, PATH):
        df = pd.read_json(PATH, lines=True)
        df.head()
        return df

    def read_from_csv(self, path):
        df = pd.read_csv(path)
        return df

    def write_to_csv(self, path, file_name, df_to_write):
        df_to_write.to_csv(path + '\\' + file_name)

# Press the green button in the gutter to run the script.
# if __name__ == '__main__':
    # path = "C:/Users/User/Documents/FourthYear/Project/resources/RC_2020-01-01 - Minified.json"
    # # reader = Reader()
    # # submisstions = reader.read_from_json(path)
    # # Opening JSON file
    # # f = open(path,)
    # # submisstions = json.load(f)
    # # for submisstion in submisstions:
    # #     print(submisstion)
    # with open('C:/Users/User/Documents/FourthYear/Project/resources/RC_2020-01-01 - Minified.json', 'r') as fp:
    #     submisstions = fp.readlines()[0].replace('}{', '},{')
    #     # with open('C:/Users/User/Documents/FourthYear/Project/resources/most_active_subreddit.txt', 'r') as fp:
    # #         submisstions = fp.readlines()
    # #     # Now writing into the file with the prepend line + old file data
    #     with open('C:/Users/User/Documents/FourthYear/Project/resources/RC_2020-01-01 - Minified.json', "w+") as f:
    #         f.write('{"data":[' + submisstions + '}]}')
    #         # f.write( submisstions[0])
    # f = open(path,)
    # submisstions = json.load(f)
    # for submisstion in submisstions:
    #     print(submisstion)
    # # removed_counter, removed_and_selftext_counter = 0,0
    # # submisstions = pd.read_json(path, orient='split')
    #
    # # for submisstion in submisstions[0]:
    # #     print(submisstion)
    # #     if((submisstion["is_crosspostabe"] == False) and (submisstion["is_robot_indexable"] == False)):
    # #         removed_counter+=1
    # #         if(submisstion["selftext"] is not None):
    # #             removed_and_selftext_counter+=1




