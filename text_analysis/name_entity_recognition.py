import spacy
from spacy import displacy
from db_utils.FileReader import FileReader
from dotenv import load_dotenv
import pandas as pd
import os
from db_utils.Con_DB import Con_DB

load_dotenv()


class NameEntity:
    def __init__(self):
        self.NER = spacy.load("en_core_web_lg")
        self.lst_NER_types = ["ORG", "GPE", "PRODUCT", "LOC", "DATE", "ORDINAL", "MONEY", "PERSON"]

    def get_entites(self,raw_text):
        text1= self.NER(raw_text)
        return [(word.text, word.label_) for word in text1.ents]
        # for word in text1.ents:
        #     print(word.text,word.label_)

    def help_get_NER_types(self):
        return self.lst_NER_types

    def type_explain(self,text_type):
        return spacy.explain(text_type)

if __name__ == '__main__':
    name_entity = NameEntity()
    file_reader = FileReader()
    # df = file_reader.read_from_csv('C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\document_topic_table.csv')
    # name_entity_list = []
    # for row_index, col in df.iterrows():
    #     name_entity_list.append([name_entity.get_entites(col['Keywords'])])
    # df_name_entity = pd.DataFrame(data=name_entity_list, columns=['Keywords_NER'])
    # df.insert(6, 'Keywords_NER', df_name_entity)
    # file_reader.write_to_csv(path="C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\", file_name='document_topic_table_with_NER', df_to_write=df)

    con_db = Con_DB()

    posts = con_db.get_cursor_from_mongodb(collection_name="politics")
    name_entity_list = []
    for post in posts.find({}):
        if post['reddit_api'][0]['data']['children'][0]['data']['selftext'] == '[removed]':
            name_entity_list.append([name_entity.get_entites(post['reddit_api'][0]['data']['children'][0]['data']['title'])])
    df_name_entity = pd.DataFrame(data=name_entity_list, columns=['Keywords_NER'])

    file_reader.write_to_csv(path="C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\",
                                file_name='politics_title_NER.csv', df_to_write=df_name_entity)


    load_dotenv()
    posts = file_reader.get_cursor_from_mongodb(auth=os.getenv('AUTH_DB'), db_name="reddit", collection_name="wallstreetbets1609452000")
    data_ = posts.find({}, {"reddit_api": 1, "pushshift_api": 1})
    # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(data_))
    for x in df['reddit_api']:
        print(x[0]['data']['children'][0]['data']['selftext'])
        print(x[0]['data']['children'][0]['data']['title'])
        print(x[0]['data']['children'][0]['data']['created_utc'])



