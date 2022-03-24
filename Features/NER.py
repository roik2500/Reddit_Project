from text_analysis.name_entity_recognition import NameEntity
import spacy
# from spacy import displacy
from db_utils.FileReader import FileReader
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import numpy as np
import os
from db_utils.Con_DB import Con_DB
from collections import Counter
from tqdm import tqdm
from text_analysis.emotion_detection import EmotionDetection
from pprint import pprint

load_dotenv()
spacy.prefer_gpu()

class NER:
    def __init__(self):
        self.ner = NameEntity()
        self.file_reader = FileReader()


    def add_n_most_common_ner_from_dict(self, path_to_csv, n):
        csv_file = self.file_reader.read_from_csv(path=path_to_csv)
        ners = self.ner.most_N_common_NER(n, path_to_csv)

        # convert ners to pandas
        ners_df = pd.DataFrame(ners.items(), columns=['NER', 'quantity'])

        # conect csv_file to ners_df
        csv_concat = pd.concat(csv_file, ners_df)
        return csv_file


    def add_n_most_common_ner_from_csv(self, path_to_csv, n):
        df_csv = pd.read_csv(path_to_csv, encoding='latin-1')
        print("a")
        # df_csv = self.file_reader.read_from_csv(path_to_csv)
        # NER_col = df_csv.head(2).values.tolist()[0]
        counter = Counter()
        ner_post_id_dict = {}
        for index, entity_list_post_id in df_csv[['ner', 'post_id']].iterrows():
            row_title = entity_list_post_id[0].replace("'", '')
            entity_list = [x.split(",")[0] for x in row_title[2:-2].split('), (')]
            for entity in entity_list:
                if entity == '' or entity == []:
                    continue
                counter[entity] += 1
                ner_post_id_dict[entity] = ner_post_id_dict.get(entity, []) + [entity_list_post_id[1]]
                # ner_post_id_dict[entity] = entity_list_post_id[1]
        commons = [ent[0] for ent in counter.most_common(n=n)]

        print("b")
        filtered_dict = {}
        a = 0
        for c in commons:
            a = ner_post_id_dict.pop(c)
            df_csv[c] = np.nan
            for index, row in df_csv.iterrows():
                if row['post_id'] in a:
                    df_csv.loc[index, c] = 1

        print("c")
        p = rf"G:\.shortcut-targets-by-id\1lJuBfy-iW6jibopA67C65lpds3B1Topb\Reddit Censorship Analysis\final_project\Features\testing\finalData_with_probas_with_ner.csv"
        df_csv.to_csv(p, encoding='latin-1')

        # ners_df = pd.DataFrame(filtered_dict.items(), columns=['NER', 'post_id'])

        #merge by post_id
        # df = df_csv.merge(ners_df, how='left', on='post_id')
        # self.file_reader.write_to_csv()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    path = rf"G:\.shortcut-targets-by-id\1lJuBfy-iW6jibopA67C65lpds3B1Topb\Reddit Censorship Analysis\final_project\BertTopic\data for bert\politics\model\model_15_300_0.6844\finalData_with_probas_with_time.csv"

    ner = NER()
    ner.add_n_most_common_ner_from_csv(path, 10)
