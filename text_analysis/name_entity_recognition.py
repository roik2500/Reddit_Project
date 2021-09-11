import spacy
from spacy import displacy
from db_utils.FileReader import FileReader
from dotenv import load_dotenv
import pandas as pd
import os
from db_utils.Con_DB import Con_DB
from collections import Counter


load_dotenv()


class NameEntity:
    def __init__(self):
        self.NER = spacy.load("en_core_web_lg")
        self.lst_NER_types = ["ORG", "GPE", "PRODUCT", "LOC", "DATE", "ORDINAL", "MONEY", "PERSON"]

    def get_entites(self,raw_text):
        text1= self.NER(raw_text)
        return [(word.text, word.label_) for word in text1.ents]

    def help_get_NER_types(self):
        return self.lst_NER_types

    def type_explain(self,text_type):
        return spacy.explain(text_type)

    '''
    this method return the most N common NER in the data at rhe path 
    :argument N The Number of the common NER to return
    :argument path PATH to CSV file to read from
    '''
    def most_N_common_NER(self, N, path):
        df_NER_data = file_reader.read_from_csv(path=path)
        dict_title = dict()
        dict_selftext = dict()

        for index, row in df_NER_data.iterrows():
            row_title = row['title_NER'].replace("'", '')
            title_labels = row_title[2:-2].split('), (')
            row_selftext = row['selftext_NER'].replace("'", '')
            selftext_labels = row_selftext[2:-2].split('), (')

            for label in title_labels:
                if label in dict_title:
                    dict_title[label] = [dict_title[label][0] + 1, dict_title[label][1] + [row['id']]]
                elif label != '':
                    dict_title[label] = [1, [row['id']]]

            for label in selftext_labels:
                if label in dict_selftext:
                    dict_selftext[label] = [dict_selftext[label][0] + 1, dict_selftext[label][1] + [row['id']]]
                elif label != '':
                    dict_selftext[label] = [1, [row['id']]]

        uniqe_NER_dict_title = self.reduce_duplicates(dict_title)
        uniqe_NER_dict_selftext = self.reduce_duplicates(dict_selftext)

        N_uniqe_NER_dict_title = sorted(uniqe_NER_dict_title.items(), key=lambda item: item[1][1], reverse=True)[:N]
        N_uniqe_NER_dict_selftext = sorted(uniqe_NER_dict_selftext.items(), key=lambda item: item[1][1], reverse=True)[:N]

        return N_uniqe_NER_dict_title, N_uniqe_NER_dict_selftext

    def reduce_duplicates(self, NER_dict):
        uniqe_NER = {}
        for key, val in NER_dict.items():
            NER, word_type = key.rsplit(', ', 1)

            if NER not in uniqe_NER.keys():
                uniqe_NER[NER] = [[word_type],val]
            else:
                uniqe_NER[NER] = [uniqe_NER[NER][0]+[word_type], [uniqe_NER[NER][1][0]+val[0],uniqe_NER[NER][1][1] + val[1]]]
        return uniqe_NER

    def get_NER_BY_Type(self, NER_dict, *kwargs_type):
        NER_by_type = {}
        for word_type in kwargs_type:
            NER_by_type[word_type] = {}

        for key, value in NER_dict:
            for val_type in value[0]:
                if val_type not in kwargs_type:
                    continue
                if key not in NER_by_type[val_type].keys():
                    NER_by_type[val_type].update({key : value[1]})
                else:
                    NER_by_type[val_type][key] = value[1] + NER_by_type[val_type][key]

        return NER_by_type

if __name__ == '__main__':
    name_entity = NameEntity()
    file_reader = FileReader()
    con_db = Con_DB()

    '''' extract NER from data'''

    # posts = con_db.get_cursor_from_mongodb(collection_name="wallstreetbets")
    # name_entity_list = []
    # for post in posts.find({}):
    #     # if post['reddit_api'][0]['data']['children'][0]['data']['selftext'] == '[removed]':
    #     keys = post['pushift_api'].keys()
    #     if(('id' in keys) and ('selftext' in keys) and ('title' in keys)):
    #         name_entity_list.append([post['pushift_api']['id'], name_entity.get_entites(post['pushift_api']['title']), name_entity.get_entites(post['pushift_api']['selftext'])])
    #     elif(('id' in keys) and ('selftext' not in keys) and ('title' in keys)):
    #         name_entity_list.append([post['pushift_api']['id'], name_entity.get_entites(post['pushift_api']['title']), name_entity.get_entites(post['reddit_api']['post']['selftext'])])
    # df_name_entity = pd.DataFrame(data=name_entity_list, columns=['id','title_NER','selftext_NER'])

    # file_reader.write_to_csv(path="C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\",
    #                             file_name='wallstreetbets_title_selftext_NER.csv', df_to_write=df_name_entity)

    ''' get N common NER in data '''

    n_common_NER_title, n_common_NER_selftext = name_entity.most_N_common_NER(N=25, path='C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\wallstreetbets_title_selftext_NER.csv')

    print("n_common_NER_title \n {} \n  n_common_NER_selftext \n {}".format(n_common_NER_title, n_common_NER_selftext))


    ''' get_NER_BY_Type '''

    print(name_entity.get_NER_BY_Type(n_common_NER_title, 'ORG', 'PERSON', 'PRODUCT', 'FAC'))








    # df = file_reader.read_from_csv('C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\document_topic_table.csv')
    # name_entity_list = []
    # for row_index, col in df.iterrows():
    #     name_entity_list.append([name_entity.get_entites(col['Keywords'])])
    # df_name_entity = pd.DataFrame(data=name_entity_list, columns=['Keywords_NER'])
    # df.insert(6, 'Keywords_NER', df_name_entity)
    # file_reader.write_to_csv(path="C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\", file_name='document_topic_table_with_NER', df_to_write=df)