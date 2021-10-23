import spacy
# from spacy import displacy
from db_utils.FileReader import FileReader
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

import os
from db_utils.Con_DB import Con_DB
from collections import Counter
from tqdm import tqdm
from text_analysis.emotion_detection import EmotionDetection
from pprint import pprint

load_dotenv()


class NameEntity:
    def __init__(self):
        self.NER = spacy.load("en_core_web_lg")
        self.lst_NER_types = ["ORG", "GPE", "PRODUCT", "LOC", "DATE", "ORDINAL", "MONEY", "PERSON"]
        self.file_reader = FileReader()
        self.NER_per_month = {}

    def get_entites(self, raw_text):
        text = self.NER(raw_text)
        return [(word.text, word.label_) for word in text.ents]

    def help_get_NER_types(self):
        return self.lst_NER_types

    def type_explain(self, text_type):
        return spacy.explain(text_type)

    '''
    this method return the most N common NER in the data at the path in the formant: 
    [NER , [counting,list_post_ids]] 
    :argument N The Number of the common NER to return
    :argument path PATH to CSV file to read from
    '''

    def most_N_common_NER(self, N, path):
        df_NER_data = self.file_reader.read_from_csv(path=path)
        dict_title_selftext = dict()

        for index, row in df_NER_data.iterrows():
            row_title = row['title_and_selftext_NER'].replace("'", '')
            title_and_selftext_labels = row_title[2:-2].split('), (')

            for label in title_and_selftext_labels:
                if label in dict_title_selftext:
                    dict_title_selftext[label] = \
                        [dict_title_selftext[label][0] + 1, dict_title_selftext[label][1] + [row['id']]]
                elif label != '':
                    dict_title_selftext[label] = [1, [row['id']]]

        uniqe_NER_dict_title_selftext = self.reduce_duplicates(NER_dict=dict_title_selftext, delimiter=', ')

        N_uniqe_NER_dict_selftext = sorted(uniqe_NER_dict_title_selftext.items(), key=lambda item: item[1][1],
                                           reverse=True)[:N]
        # dict format -> { NER : [ NER TYPE, [counting, list_post_ids] ] }
        return N_uniqe_NER_dict_selftext

    def reduce_duplicates(self, NER_dict, delimiter):
        uniqe_NER = {}
        for key, val in NER_dict.items():
            NER, word_type = key.rsplit(delimiter, 1)
            NER = NER.lower()
            if NER not in uniqe_NER.keys():
                uniqe_NER[NER] = [[word_type], val]
            else:
                uniqe_NER[NER] = [uniqe_NER[NER][0] + [word_type],
                                  [uniqe_NER[NER][1][0] + val[0], uniqe_NER[NER][1][1] + val[1]]]
        return uniqe_NER  # dict format -> { NER : [ NER TYPE, [counting, list_post_ids] ] }

    def get_NER_BY_Type(self, NER_dict, *kwargs_type):
        NER_by_type = {}
        for word_type in kwargs_type:
            NER_by_type[word_type] = {}

        for key, value in NER_dict:
            for val_type in set(value[0]):
                if val_type not in kwargs_type:
                    continue
                if key not in NER_by_type[val_type].keys():
                    NER_by_type[val_type].update({key: value[1]})
                else:
                    NER_by_type[val_type][key] = value[1] + NER_by_type[val_type][key]

        return NER_by_type

    def extract_NER_from_data(self, posts, file_name_to_save, path_to_folder, save_deduced_data, con_db,
                              is_removed_bool, post_or_comment_arg):
        name_entity_list = []
        for post in tqdm(posts):
            objects = con_db.get_text_from_post_OR_comment(post_or_comment=post_or_comment_arg, object=post)
            for object in objects:
                if object != [] and object[-1] == is_removed_bool:  # is_removed_bool == TRUE == only Removed is_removed_bool == FALSE == only  NOT Removed
                    name_entity_list.append([object[2], self.get_entites(object[0])])
                else:  # The other option
                    continue
                for new_ner_key, NER_type in name_entity_list[-1][1]:
                    if NER_type in ['ORG', 'PERSON', 'PRODUCT']:
                        new_ner_post_id = [object[2]]
                        year = int(datetime.strptime(object[1], "%Y-%m-%d").date().year)
                        month = int(datetime.strptime(object[1], "%Y-%m-%d").date().month)
                        date_key = str(year) + "/" + str(month)
                        key = "{}-{}".format(new_ner_key, NER_type)
                        if date_key in self.NER_per_month.keys() and key in self.NER_per_month[date_key].keys():
                            self.NER_per_month[date_key].update(
                                {key: [self.NER_per_month[date_key][key][0] + 1, self.NER_per_month[date_key][key][1] +
                                       new_ner_post_id]})
                        else:

                            if date_key in self.NER_per_month.keys():
                                self.NER_per_month[date_key].update({key: [1, new_ner_post_id]})
                            else:
                                self.NER_per_month[date_key] = {key: [1, new_ner_post_id]}

        if save_deduced_data:
            print("save_deduced_data")
            self.save_to_csv(file_name_to_save, name_entity_list, path_to_folder)
            for key, val in self.NER_per_month.items():
                self.NER_per_month[key] = self.reduce_duplicates(NER_dict=val, delimiter='-')
            self.file_reader.write_dict_to_json(path=path_to_folder, file_name="{}_NER_per_month".format(is_removed_bool),
                                                dict_to_write=self.NER_per_month)

    def save_to_csv(self, file_name_to_save, name_entity_list, path_to_folder):
        df_name_entity = pd.DataFrame(data=name_entity_list, columns=['id', 'title_and_selftext_NER'])
        self.file_reader.write_to_csv(path=path_to_folder,
                                      file_name=file_name_to_save, df_to_write=df_name_entity)


if __name__ == '__main__':
    name_entity = NameEntity()
    file_reader = FileReader()
    # emotion_detection = EmotionDetection()
    # for k in range(1,5):
    #     con_db = Con_DB(k)

#
#     '''' extract NER from data'''
#
#     posts = con_db.get_cursor_from_mongodb(collection_name=os.getenv("COLLECTION_NAME"))
#     name_entity_list = []
#     for post in posts.find({}):
#         # if post['reddit_api'][0]['data']['children'][0]['data']['selftext'] == '[removed]':
#         keys = post['pushift_api'].keys()
#         if(('id' in keys) and ('selftext' in keys) and ('title' in keys)):
#             name_entity_list.append([post['pushift_api']['id'], name_entity.get_entites(post['pushift_api']['title']), name_entity.get_entites(post['pushift_api']['selftext'])])
#         elif(('id' in keys) and ('selftext' not in keys) and ('title' in keys)):
#             name_entity_list.append([post['pushift_api']['id'], name_entity.get_entites(post['pushift_api']['title']), name_entity.get_entites(post['reddit_api']['post']['selftext'])])
#     df_name_entity = pd.DataFrame(data=name_entity_list, columns=['id','title_NER','selftext_NER'])
#
#     file_reader.write_to_csv(path="C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\",
#                                 file_name='wallstreetbets_title_selftext_NER.csv', df_to_write=df_name_entity)
#
#     ''' get N common NER in data '''
#
#     # n_common_NER_title, n_common_NER_selftext = name_entity.most_N_common_NER(N=25, path='C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\wallstreetbets_title_selftext_NER.csv')
#
#     # print("n_common_NER_title \n {} \n  n_common_NER_selftext \n {}".format(n_common_NER_title, n_common_NER_selftext))
#
#     ''' get_NER_BY_Type '''
#
#     # pprint(name_entity.get_NER_BY_Type(n_common_NER_title, 'ORG', 'PERSON', 'PRODUCT', 'FAC'))
#
#
#     ''' conect NER and emotion detection'''
#
#     n_common_NER_title, n_common_NER_selftext = name_entity.most_N_common_NER(N=50, path='C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\wallstreetbets_title_selftext_NER.csv')
#     NER_BY_Type = name_entity.get_NER_BY_Type(n_common_NER_title, 'ORG', 'PERSON', 'PRODUCT', 'FAC')
#     con_db.get_cursor_from_mongodb(db_name="reddit", collection_name=os.getenv("COLLECTION_NAME"))
#     for type_item in NER_BY_Type:
#         for NER_item, posts_ids_list in NER_BY_Type[type_item].items():
#             relevant_posts = con_db.get_specific_items_by_post_ids(ids_list=posts_ids_list[1])
#             emotion_detection.extract_posts_emotion_rate(relevant_posts)
#             emotion_detection.calculate_post_emotion_rate_mean()
#             pprint(NER_item)
#             pprint(emotion_detection.emotion_posts_avg_of_subreddit)
#
#
#
#
# df = file_reader.read_from_csv('C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\topic_sentim_general-best.csv')
# name_entity_list = []
# for row_index, col in df.iterrows():
#     name_entity_list.append([name_entity.get_entites(col['topic'])])
# df_name_entity = pd.DataFrame(data=name_entity_list, columns=['Topic_NER'])
# df.insert(4, 'Topic_NER', df_name_entity)
# file_reader.write_to_csv(path="C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\", file_name='document_topic_table_with_NER.csv', df_to_write=df)
