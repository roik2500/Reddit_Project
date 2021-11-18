# from psaw import PushshiftAPI
# from pmaw import PushshiftAPI as PushshiftApiPmaw
import datetime
import requests
import json
import pandas as pd
import ijson
from tqdm import tqdm
import pymongo
import os
import csv


class FileReader:

    def __init__(self):
        self.json_open_file = None

    ''' :return dict '''
    def read_from_json_to_dict(self, PATH):
        with open(PATH) as json_file:
            data = json.load(json_file)
        return data

    ''' :return data frame '''
    def read_from_json_to_df(self, PATH):
        df = pd.read_json(PATH, lines=True)
        # df.head()
        return df

    def read_from_csv(self, path):
        df = pd.read_csv(path)
        return df

    def write_to_csv(self, path, file_name, df_to_write):
        if path[-1:] == '/':
            df_to_write.to_csv(path + file_name)
        else:
            df_to_write.to_csv(path + '/' + file_name)

    def write_dict_to_json(self, path, file_name, dict_to_write):
        file = path + file_name + '.json'
        with open(file, 'w') as fp:
            json.dump(dict_to_write, fp)

    def get_specific_items_by_post_ids_from_json(self, ids_list):
        text_and_date_list = []
        with open(os.getenv('DATA_PATH')) as json_file:
            data = json.load(json_file)
            for key_id in ids_list:
                post = data[key_id]
                text_and_date_list.append(self.get_text_from_post_OR_comment(post, post_or_comment='post'))
        return text_and_date_list  # [title , selftext ,created_utc, 'id']

    '''
    @:argument json_file - path to the file to read from
    @:return return json iterator to the file.
    '''
    def get_json_iterator(self, json_file):
        if self.json_open_file != None and self.json_open_file.open:
            self.json_open_file.closed()
        self.json_open_file = open(json_file, 'rb')
        items = ijson.items(self.json_open_file, 'item')
        return items


    def topic_number_connected_posts(self, path, folder_to_save, number_of_topic=20):
        topic_df = self.read_from_csv(path)
        topic_num_posts_dict = {}
        for topic_number in range(number_of_topic):
            topic_num_posts_dict[topic_number] = topic_df.loc[topic_df['Dominant_Topic'] == topic_number]['post_id'].to_list()
        return topic_num_posts_dict
        # self.write_dict_to_json(path=folder_to_save,
        #                         file_name='topic_posts', dict_to_write=topic_num_posts_dict)

    def write_dict_to_csv(self, file_name, dictionary):
        with open(file_name, 'w') as csv_file:
            writer = csv.writer(csv_file)
            for key, value in dictionary.items():
               writer.writerow([key, value])

    def read_dict_from_csv(self, file_name):
        with open(file_name) as csv_file:
            reader = csv.reader(csv_file)
            mydict = dict(reader)
        return mydict



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    path = 'C:\\Users\\User\\Documents\\FourthYear\\Project\\document_topic_table_general-1_updated\\document_topic_table_general-1_updated.csv'
    file_reader = FileReader()
    file_reader.topic_number_connected_posts(path)
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




