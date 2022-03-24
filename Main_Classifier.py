from Classifier.model import Model, tfidf
import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay

from collections import Counter
import os
from dotenv import load_dotenv
import pandas as pd
import ijson

from db_utils.Con_DB import Con_DB

load_dotenv()

PATH_DRIVE = os.getenv("OUTPUTS_DIR")

data_path_drive = rf"G:\.shortcut-targets-by-id\1lJuBfy-iW6jibopA67C65lpds3B1Topb\Reddit Censorship Analysis\final_project\BertTopic\data for bert\politics\model\model_15_300_0.6844\finalData_with_probas_with_time.csv"
data_path = rf"/sise/home/shouei/reddit_code_shai/in_out_put _classifier/finalData_with_probas_with_time.csv"

def run_model(model, _class, Classifier_option, model_name, k_best_features):
    print("model_name: {}".format(model_name))
    if Classifier_option == "binary":
        model.split_corpus_binary(_class, k_best_features)
        model.balance_data_Undersample_the_biggest_dataset(_class)
    elif Classifier_option == "all":
        model.split_corpus_all(k_best_features)
        _class = "all"
    print("---------------------- " + _class + " -------------------------")
    trined_model = model.train_model(model_name=model_name)
    print("k_feature_names {} : {}".format(len(k_feature_names), k_feature_names))
    # if model_name != 'DecisionTreeClassifier':
    y_predict = trined_model.predict(model.df_test)
    y_predict_prob = trined_model.predict_proba(model.df_test)[:, 1]
    # else:
    #     y_predict = trined_model
    #     y_predict_prob = ""
    evaluation_res = model.evaluation_indices(y_predict, y_predict_prob, _class)
    csv_record_path_dirve = rf"G:\.shortcut-targets-by-id\1lJuBfy-iW6jibopA67C65lpds3B1Topb\Reddit Censorship Analysis\final_project\Features\testing\test_xl.csv"
    csv_record_path = rf"/sise/home/shouei/reddit_code_shai/in_out_put _classifier/test_xl.csv"
    classifier_data = [model_name, Classifier_option, _class]
    classifier_data.append(model.get_class_size(model.df_train))
    classifier_data.append(model.get_class_size(model.df_test))
    if _class != 'all':
        classifier_data.append(len(model.data[model.data['status'] == _class]))
        tn, fp, fn, tp = confusion_matrix(model.test_labels, y_predict, labels=["not_"+_class, _class]).ravel()
        print("TN: ", tn, "FP: ", fp, " FN: ", fn, " TP: ", tp)
        # cm = confusion_matrix(model.test_labels, y_predict, labels=["not_"+_class, _class])
        # disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=["not_"+_class, _class])
        # disp.plot()
        # plt.show()
    else:
        classifier_data.append(len(model.data.index))
    features_data = [k_best_features, len(k_best_features)]
    print("classifier_data: ", classifier_data)
    model.file_reader.testing_recorder(features_data, evaluation_res, classifier_data, csv_record_path)


model = Model(max_post_number=359411, data=data_path, post_or_comment_model='post')  # politics

# TF-IDF section
model.data = model.data[model.data['status'] != 'deleted']
class_sized = model.get_class_size()
summ=0
for _class, val in class_sized:
    summ+=val
print(class_sized, summ)
TfIdf = tfidf(model.data.title_selftext.to_list())
model.make_class_as_binary(class_name="exists")
print("---------------------------------------------------------------------")
removed_tfidf_dict_sorted = TfIdf.explore_rare_words_in_removed_posts(model.data)

print(removed_tfidf_dict_sorted)


# all classes

models_name = ["XGBClassifier", 'DecisionTreeClassifier', 'LogisticRegression', "RandomForestClassifier"]
classes = ['deleted', 'removed', 'exists', 'shadow_ban']

for k in range(5, 11):  # k_features number
    model.split_corpus_basic()
    k_feature_names = model.k_best_features(k)
    k_feature_names = list(k_feature_names.head(0).columns)
    for _class in classes:
        # run_model(model, "", "all", model_name, k_feature_names)
        for model_name in models_name:
            run_model(model, _class, "binary", model_name, k_feature_names)
        model.reread_dataset(data_path=data_path)


# without deleted class

classes = ['removed', 'exists', 'shadow_ban']
print("1", model.data.shape)
model.data = model.data[model.data['status'] != 'deleted']
print("2", model.data.shape)

for k in range(5, 11):
    model.split_corpus_basic()
    k_feature_names = model.k_best_features(k)
    k_feature_names = list(k_feature_names.head(0).columns)
    for _class in classes:
        # run_model(model, "", "all", model_name, k_feature_names)
        for model_name in models_name:
            run_model(model, _class, "binary", model_name, k_feature_names)
        model.reread_dataset(data_path=data_path)
