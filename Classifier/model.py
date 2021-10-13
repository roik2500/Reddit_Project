from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
import numpy as np
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, mean_squared_error
from sklearn.feature_selection import SelectKBest
from sklearn import preprocessing
from db_utils.FileReader import FileReader
import pandas as pd
from tqdm import tqdm
from db_utils.Con_DB import Con_DB
from Classifier.word_embeddings_spaCy import WordEmbedding
''' supervised model'''


class Model:

    def __init__(self, max_post_number, path_data, post_or_comment_model):
        self.MAX_POST_NUMBER = max_post_number  # insert number of the posts in the corpus
        self.df_train = pd.DataFrame()
        self.df_test = pd.DataFrame()
        self.train_labels = None
        self.test_labels = None
        file_reader = FileReader()
        self.con_db = Con_DB()
        self.json_data_cursor = file_reader.get_json_iterator(json_file=path_data)
        self.post_or_comment_model = post_or_comment_model
        self.word_embedding = WordEmbedding()


    def split_corpus(self):
        podtid_emdeding_label = []
        for item_cursor in tqdm(self.json_data_cursor):
            reddit_items = self.con_db.get_text_from_post_OR_comment(object=item_cursor, post_or_comment=self.post_or_comment_model)
            for reddit_item in reddit_items:
                embedding_vector_for_reddit_item = self.word_embedding.get_emdeding_post_vec_avg(reddit_item[0])
                podtid_emdeding_label.append((reddit_item[1], embedding_vector_for_reddit_item, reddit_item[-1]))
        input_df = pd.DataFrame(data=podtid_emdeding_label, columns=['Post_id', 'embeding', 'label'])
        border = int(self.MAX_POST_NUMBER*0.8)

        self.df_train = np.array(input_df.iloc[:border, :])
        print('self.df_train.shape', self.df_train.shape)

        self.df_test = np.array(input_df.iloc[border:, :])
        print('self.df_test.shape', self.df_test.shape)

        self.train_labels = self.df_train[:, 2]
        print('self.train_labels.shape', self.train_labels.shape)

        self.test_labels = self.df_test[:, 2]
        print('self.test_labels.shape', self.test_labels.shape)

        print("Done")



    def get_fit_model(self, model_name):
        if model_name == 'LogisticRegression':
            return LogisticRegression(random_state=0).fit(self.df_train, self.train_labels)

        elif model_name == 'RandomForestClassifier':
            return RandomForestClassifier(random_state=0).fit(self.df_train, self.train_labels)

        elif model_name == 'DecisionTreeClassifier':
            return DecisionTreeClassifier(random_state=0).fit(self.df_train, self.train_labels)

        else:
            return "invalid model"



