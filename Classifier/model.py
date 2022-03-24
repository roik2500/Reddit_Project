from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from gensim.models import TfidfModel
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from gensim.corpora import Dictionary
import numpy as np
import xgboost
from sklearn.model_selection import TimeSeriesSplit
from sklearn.model_selection import cross_val_score, cross_val_predict
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, f1_score, auc, roc_curve, roc_auc_score, \
    mean_squared_error
from sklearn.feature_selection import SelectKBest, chi2
from sklearn import preprocessing
from imblearn.under_sampling import RandomUnderSampler
from collections import Counter
from db_utils.FileReader import FileReader
import pandas as pd
from tqdm import tqdm
from db_utils.Con_DB import Con_DB
from Classifier.word_embeddings_spaCy import WordEmbedding
import os
from dotenv import load_dotenv
from collections import Counter
load_dotenv()

''' supervised model'''

'''
Creating a new folder in Google drive. 
:argument folderName - name of the new folder
:return full path to the new folder
'''


def create_new_folder_drive(path, new_folder):
    updated_path = "{}{}/".format(path, new_folder)
    if not os.path.exists(updated_path):
        os.makedirs(updated_path)
    return updated_path


class doc_2_vec:
    def __init__(self, corpus):
        self.corpus_tagged = [TaggedDocument(doc, [i]) for i, doc in enumerate(corpus)]
        self.model = None

    def train_model(self):
        self.model = Doc2Vec(self.corpus_tagged, vector_size=5, window=2, min_count=1, workers=4)

    def save_model_to_disk(self):
        self.model.save("test_doc2vec.model")

    def load_model_from_disk(self):
        self.model = Doc2Vec.load("test_doc2vec.model")




class tfidf:
    def __init__(self, corpus):
        self.file_reader = FileReader()
        corpus = [d.split() for d in corpus]
        self.dct = Dictionary(corpus)  # fit dictionary
        self._corpus = [self.dct.doc2bow(line) for line in corpus]  # convert corpus to BoW format
        self.model = TfidfModel(self._corpus)  # fit model

    def get_tfidf_by_post_text(self, post_text):
        post_split = [d.split() for d in [post_text]]
        doc_2_bow = [self.dct.doc2bow(line) for line in post_split]
        tfidf_score = self.model[doc_2_bow]
        return [x for x in zip(post_split, tfidf_score)]

    def explore_rare_words_in_removed_posts(self, df_removed_posts):
        removed_tfidf_dict = {}
        for index, row in tqdm(df_removed_posts.iterrows()):
            removed_tfidf_dict[row["post_id"]] = self.get_tfidf_by_post_text(row["title_selftext"]) # {post_id: (word, word_id, tf_idf_score)}
        csv_record_path = "G:\\.shortcut-targets-by-id\\1lJuBfy-iW6jibopA67C65lpds3B1Topb\\Reddit Censorship Analysis\\final_project\\Features\\testing\\"
        self.file_reader.write_dict_to_json(path=csv_record_path, file_name="tfidf_removed_posts", dict_to_write=removed_tfidf_dict)


class Model:

    def __init__(self, max_post_number, data, post_or_comment_model):
        self.MAX_POST_NUMBER = max_post_number  # insert number of the posts in the corpus
        self.df_train = pd.DataFrame()
        self.df_train_balanced = pd.DataFrame()
        self.df_test = pd.DataFrame()
        self.train_labels = None
        self.test_labels = None
        self.file_reader = FileReader()
        self.con_db = Con_DB()
        self.data = pd.read_csv(data, encoding='latin-1').dropna(subset=['anger'])
        # self.data_cursor = self.con_db.get_cursor_from_mongodb()
        self.post_or_comment_model = post_or_comment_model
        self.word_embedding = WordEmbedding()

    def reread_dataset(self, data_path):
        self.data = pd.read_csv(data_path, encoding='latin-1').dropna(subset=['anger'])

    # use this method after splitting the corpus
    def balance_data_Undersample_the_biggest_dataset(self, class_name):

        under_sampler = RandomUnderSampler(random_state=42)
        self.df_train, self.train_labels = under_sampler.fit_resample(self.df_train, self.train_labels)
        self.df_test, self.test_labels = under_sampler.fit_resample(self.df_test, self.test_labels)


    def split_corpus_binary(self, class_name, k_best_features):
        self.make_class_as_binary(class_name)
        self.data.created_date = pd.to_datetime(self.data.created_date)
        self.data.index = self.data.created_date
        X = self.data.loc[:, k_best_features]
        y = self.data["status"]
        tscv = TimeSeriesSplit(gap=0, max_train_size=None, n_splits=5, test_size=None)
        for train_index, test_index in tscv.split(X):
            continue

        self.df_train, self.train_labels = X.iloc[train_index, :], y[train_index]
        self.df_test, self.test_labels = X.iloc[test_index], y[test_index]

        # self.data["date"] = pd.to_datetime(self.data["created_date"])
        # self.data = self.data.sort_values(by="created_date")
        #
        # self.make_class_as_binary(class_name)
        # self.MAX_POST_NUMBER = self.data.shape[0]
        # border = int(self.MAX_POST_NUMBER * 0.8)
        # self.df_train = self.data.iloc[:border, :]
        # self.train_labels = self.df_train["status"]
        # self.df_train = self.df_train.loc[:, k_best_features]
        #
        # self.df_test = self.data.iloc[border:, :]
        # self.test_labels = self.df_test["status"]
        # self.df_test = self.df_test.loc[:, k_best_features]

    def make_class_as_binary(self, class_name):
        # self.data = self.data.sort_values(by="created_date")
        names = ["deleted", "removed", "exists", "shadow_ban"]
        names_dict = {"deleted": "0",
                      "removed": "1",
                      "exists": "2",
                      "shadow_ban": "3"}
        names.remove(class_name)
        names_dict.pop(class_name)
        neg_class = "not_" + class_name
        self.data["status"].replace(names, neg_class, inplace=True)

    def split_corpus_basic(self):
        self.data["date"] = pd.to_datetime(self.data["created_date"])

        self.data = self.data.sort_values(by="created_date")

        border = int(self.MAX_POST_NUMBER * 0.8)
        self.df_train = self.data.iloc[:border, :]
        self.train_labels = self.df_train["status"]
        self.df_train = self.df_train.loc[:, "anger":"offensive"]

        self.df_test = self.data.iloc[border:, :]
        self.test_labels = self.df_test["status"]
        self.df_test = self.df_test.loc[:, "anger":"offensive"]

    def split_corpus_all(self, k_best_features):
        self.data["date"] = pd.to_datetime(self.data["created_date"])

        self.data = self.data.sort_values(by="created_date")
        self.MAX_POST_NUMBER = self.data.shape[0]
        border = int(self.MAX_POST_NUMBER * 0.8)
        self.df_train = self.data.iloc[:border, :]
        self.train_labels = self.df_train["status"]
        self.df_train = self.df_train[k_best_features]

        self.df_test = self.data.iloc[border:, :]
        self.test_labels = self.df_test["status"]
        self.df_test = self.df_test[k_best_features]

        # for item_cursor in tqdm(self.data_cursor.find({})):
        #     reddit_items = self.con_db.get_text_from_post_OR_comment(_object=item_cursor, post_or_comment=self.post_or_comment_model)
        #     for reddit_item in reddit_items:
        #         embedding_vector_for_reddit_item = self.word_embedding.get_emdeding_post_vec_avg(reddit_item[0])
        #         postid_emdeding_label.append((reddit_item[1], embedding_vector_for_reddit_item, reddit_item[-1]))
        #     counter+=1
        #
        #     if counter == border:
        #         # input_df = pd.DataFrame(data=postid_emdeding_label, columns=['Post_id', 'embeding', 'label'])
        #         # border = int(self.MAX_POST_NUMBER*0.8)
        #
        #         # data_path = os.getenv('OUTPUTS_DIR') + 'data/'
        #         data_path = create_new_folder_drive(os.getenv('OUTPUTS_DIR') + 'data/', "Classifier_data")
        #
        #         # self.df_train = input_df.iloc[:border, [0, 1]]
        #         self.df_train = pd.DataFrame(data=postid_emdeding_label, columns=['Post_id', 'embeding', 'label']).iloc[:, [0,1]]
        #         self.df_train.to_pickle(data_path+'train_set.pkl', protocol=4)
        #         # self.file_reader.write_to_csv(path=data_path, file_name='train_set.csv', df_to_write=self.df_train)
        #         print('self.df_train.shape', self.df_train.shape)
        #
        #         self.train_labels = pd.DataFrame(data=postid_emdeding_label, columns=['Post_id', 'embeding', 'label']).iloc[:, 2]
        #         self.df_train.to_pickle(data_path + 'train_labels.pkl', protocol=4)
        #         # self.file_reader.write_to_csv(path=data_path, file_name='train_labels.csv', df_to_write=self.train_labels)
        #         print('self.train_labels.shape', self.train_labels.shape)
        #         self.df_train = None
        #         self.train_labels = None
        #         postid_emdeding_label = []
        #
        #     elif counter == self.MAX_POST_NUMBER-1:
        #         self.df_test = pd.DataFrame(data=postid_emdeding_label, columns=['Post_id', 'embeding', 'label']).iloc[:, [0,1]]
        #         self.df_test.to_pickle(data_path+'test_set.pkl', protocol=4)
        #         # self.file_reader.write_to_csv(path=data_path, file_name='test_set.csv', df_to_write=self.df_test)
        #         print('self.df_test.shape', self.df_test.shape)
        #
        #         self.test_labels = pd.DataFrame(data=postid_emdeding_label, columns=['Post_id', 'embeding', 'label']).iloc[:, 2]
        #         self.df_test.to_pickle(data_path+'test_labels.pkl', protocol=4)
        #         # self.file_reader.write_to_csv(path=data_path, file_name='test_labels.csv', df_to_write=self.test_labels)
        #         print('self.test_labels.shape', self.test_labels.shape)
        #
        # print("Done")

    def k_best_features(self, k):
        selector = SelectKBest(chi2, k=k)
        selector.fit_transform(self.df_train, self.train_labels)
        cols = selector.get_support(indices=True)
        features_df_new = self.df_train.iloc[:, cols]
        return features_df_new  # ,train_new

    def train_model(self, model_name):
        if model_name == 'LogisticRegression':
            return LogisticRegression(random_state=0).fit(self.df_train, self.train_labels)

        elif model_name == 'RandomForestClassifier':
            return RandomForestClassifier(max_depth=5, random_state=0).fit(self.df_train, self.train_labels)

        elif model_name == 'DecisionTreeClassifier':
            model = DecisionTreeClassifier(random_state=0)  # .fit(self.df_train, self.train_labels)
            cross_val_predict(model, self.df_train, self.train_labels, cv=10)
            return model.fit(self.df_train, self.train_labels)

        elif model_name == "XGBClassifier":
            model_xgboost = xgboost.XGBClassifier(random_state=42,
                                                  objective='binary:logistic')  # , use_label_encoder=False)
            eval_set = [(self.df_test, self.test_labels)]
            model_xgboost.fit(self.df_train,
                              self.train_labels,
                              early_stopping_rounds=10,
                              eval_set=eval_set,
                              verbose=True)

            y_train_pred = model_xgboost.predict_proba(self.df_train)[:, 1]
            y_valid_pred = model_xgboost.predict_proba(self.df_test)[:, 1]

            print("AUC Train: {:.4f}\nAUC Valid: {:.4f}".format(roc_auc_score(self.train_labels, y_train_pred),
                                                                roc_auc_score(self.test_labels, y_valid_pred)))

            return model_xgboost

        else:
            return "invalid model"

    def get_class_size(self, data):
        return list(data.status.value_counts().to_dict().items())

    def evaluation_indices(self, prediction, prediction_prob, _class):

        accuracy = accuracy_score(self.test_labels, prediction)
        precision_recall_fscore = precision_recall_fscore_support(self.test_labels, prediction, average='weighted')
        f1 = f1_score(self.test_labels, prediction, average="macro")
        if _class == "all":  # mean class is all
            fpr = 0
            tpr = 0
            thresholds = 0
            _auc = 0
            # auc_roc_score = roc_auc_score(self.test_labels, prediction_prob, multi_class='ovo', average=None)
            # print("auc_roc_score", auc_roc_score)
        else:
            # fpr, tpr, thresholds = roc_curve(self.test_labels.replace("not_{}".format(_class), 0).replace(_class, 1),
            #                                  prediction_prob, pos_label=2)
            # print("AUC Train: {:.4f}\nAUC Valid: {:.4f}".format(roc_auc_score(self.train_labels, y_train_pred),
            _auc = roc_auc_score(self.test_labels, prediction_prob)
            # _auc = auc(tpr, tpr)
        print("Accuracy: %.2f%%" % (accuracy * 100.0))
        print("precision: %.2f%%" % (precision_recall_fscore[0] * 100),
              "\nrecall: %.2f%%" % (precision_recall_fscore[1] * 100)
              , "\nfscore: %.2f%%" % (f1 * 100), "\nAUC %.4f%%" % (_auc * 100))

        return [accuracy, precision_recall_fscore[0], precision_recall_fscore[1], f1, _auc]  # [fpr, tpr, thresholds]
