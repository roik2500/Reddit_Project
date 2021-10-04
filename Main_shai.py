from tqdm import tqdm

from db_utils.FileReader import FileReader
from statistics_analysis.Statistics import Statistic
from db_utils.Con_DB import Con_DB
from text_analysis.Sentiment import Sentiment
import csv
import os
import pandas as pd
from text_analysis.emotion_detection import EmotionDetection
from text_analysis.name_entity_recognition import NameEntity

# import matplotlib.pyplot as plt
# from matplotlib.dates import date2num
import datetime


'''
Creating a new folder in Google drive. 
:argument folderName - name of the new folder
:return full path to the new folder
'''
def creat_new_folder_drive(folderName):
    path_folder = path_drive + folderName
    os.mkdir(path_folder)
    return path_folder


'''
:argument This method get path to data after that was go through name_entity_recognition.get_entites()
:argument n is the number of NER to return 
:argument  collection name is the subreddit name that save in mnongoDB

This method extract NER emotion on plot the x axis is month and y axis is emotion rate.
the plot is over a  months in one year.

:returns emotion analysis for the n NERS and save it to csv file.
'''

def explore_data_with_NER_and_emotion(n, path_to_read_data, collection_name, path_to_save_plots,
                                      Con_DB, category, name_entity):
    NER_emotion_df = None
    n_common_NER_title_selftext = name_entity.most_N_common_NER(N=n, path=path_to_read_data)
    NER_BY_Type = name_entity.get_NER_BY_Type(n_common_NER_title_selftext, 'ORG', 'PERSON', 'PRODUCT')
    NER_emotion_df = get_emotion_to_NER(Con_DB, NER_BY_Type, NER_emotion_df, category, collection_name,
                                        path_to_save_plots)
    # example name of file - "Removed_NER_emotion_rate_mean_wallstreetbets.csv"
    File = "{}_NER_emotion_rate_mean_{}.csv".format(category, collection_name)
    file_reader.write_to_csv(df_to_write=NER_emotion_df, path=resource_path,
                             file_name=File)


def get_emotion_to_NER(Con_DB, NER_BY_Type, NER_emotion_df, category, collection_name, path_to_save_plots):
    flag = True
    for type_item in NER_BY_Type:
        new_file = "{}{}\\".format(path_to_save_plots, str(type_item.replace("\\", "-")))
        if not os.path.exists(new_file):
            os.makedirs(new_file)
        for NER_item, posts_ids_list in NER_BY_Type[type_item].items():
            relevant_posts = Con_DB.get_specific_items_by_post_ids(ids_list=posts_ids_list[1])
            emotion_detection.extract_posts_emotion_rate(relevant_posts, Con_DB)
            emotion_detection.calculate_post_emotion_rate_mean()
            emotion_detection.emotion_plot_for_posts_in_subreddit(date_format='%Y/%m', subreddit_name=collection_name
                                                                  , NER=NER_item, path_to_save_plt=new_file,
                                                                  category=category)
            temp_df = pd.DataFrame({
                "entity": [NER_item],
                "Angry": [list(emotion_detection.emotion_posts_avg_of_subreddit["Angry"].items())],
                "Fear": [list(emotion_detection.emotion_posts_avg_of_subreddit["Fear"].items())],
                "Happy": [list(emotion_detection.emotion_posts_avg_of_subreddit["Happy"].items())],
                "Sad": [list(emotion_detection.emotion_posts_avg_of_subreddit["Sad"].items())],
                "Surprise": [list(emotion_detection.emotion_posts_avg_of_subreddit["Surprise"].items())]
            })
            if flag:
                NER_emotion_df = temp_df
                flag = False
            else:
                NER_emotion_df = pd.concat([NER_emotion_df, temp_df], ignore_index=True)
            # pprint(NER_item)
            # pprint(emotion_detection.emotion_posts_avg_of_subreddit)
    return NER_emotion_df


def NER_post_quantity(category, path_to_read_data, n, name_entity, resource_path):

    n_common_NER_title_selftext = name_entity.most_N_common_NER(N=n, path=path_to_read_data)
    list_to_save = [(object[0], object[1][0], object[1][1][0]) for object in n_common_NER_title_selftext]

    df = pd.DataFrame(data=list_to_save, columns=['NER', 'NER_TYPE', 'number_of_posts'])
    file_reader.write_to_csv(path=resource_path,
                             file_name=category + 'NER_quantity.csv', df_to_write=df)

    NER_BY_Type = name_entity.get_NER_BY_Type(n_common_NER_title_selftext, 'ORG', 'PERSON', 'PRODUCT', 'FAC')
    file_reader.write_dict_to_json(path=resource_path,
                                   file_name=category + 'NER_quantity_byType', dict_to_write=NER_BY_Type)


def explore_data_with_emotion_to_NER_per_month(Con_DB, file_reader,  path_to_read_data, category, collection_name,
                                               path_to_save_plots):
    NER_emotion_df = None
    # should return python dict when key is date and values another diat that keys is NERs and value is ids_list
    items = file_reader.read_from_json_to_dict(path_to_read_data)
    NER_emotion_df = get_emotion_to_NER(Con_DB, items, NER_emotion_df, category, collection_name,
                                            path_to_save_plots)
    File = category + "_NER_emotion_per_month.csv"
    file_reader.write_to_csv(df_to_write=NER_emotion_df, path=resource_path,
                             file_name=File)


def connect_NER_Topic_Emotion(path_to_topic_csv, path_to_removed_NER_emotion_csv, path_to_folder, file_name_to_save):
    global data_category_df
    df = file_reader.read_from_csv(path_to_topic_csv)
    name_entity_list = [[name_entity_removed.get_entites(col['topic'])] for row_index, col in df.iterrows()]
    df_name_entity = pd.DataFrame(data=name_entity_list, columns=['entity'])
    df.insert(4, 'Topic_NER', df_name_entity)
    entity_list = []
    for entities in name_entity_list:
        if entities != [[]]:
            entity_list += [entity[0][0] for entity in entities]
    data_category_df = file_reader.read_from_csv(path_to_removed_NER_emotion_csv)
    # not_removed_df = pd.read_csv(folder_path + "NotRemoved_NER_emotion_rate_mean_{}.csv".format(COLLECTION_NAME))
    matches = data_category_df[data_category_df.iloc[:, 1].isin(entity_list)]
    res = [name_entity_removed.get_entites(col['entity'])[0] for row_index, col in matches.iterrows()]
    index = matches.index
    for row_index, col in df_name_entity.iterrows():
        for c in col['entity']:
            if c in res:
                condition = matches['entity'] == c[0]
                entity_indices = index[condition]
                matches._set_value(entity_indices, 'Unnamed: 0', row_index)
    matches.reindex(matches['Unnamed: 0'])
    df = df.merge(matches, how='left', on='Unnamed: 0')
    # df = pd.concat([df, matches], axis=1)
    file_reader.write_to_csv(path=path_to_folder,
                             file_name=file_name_to_save, df_to_write=df)


if __name__ == '__main__':

    name_entity_not_removed = NameEntity()
    name_entity_removed = NameEntity()
    name_entity_quantity_removed = NameEntity()
    name_entity_quantity_not_removed = NameEntity()
    name_entity_quantity_all = NameEntity()

    file_reader = FileReader()
    emotion_detection = EmotionDetection()
    emotion_detection_removed = EmotionDetection()
    emotion_detection_not_removed = EmotionDetection()
    con_db = Con_DB()

    COLLECTION_NAME = os.getenv("COLLECTION_NAME")
    PATH_DRIVE = os.getenv("OUTPUTS_DIR") + 'emotion_detection/'

    data_cursor = con_db.get_cursor_from_mongodb(db_name='reddit', collection_name=COLLECTION_NAME).find({})

    resource_path = PATH_DRIVE + 'resources\\'
    plots_folder_path = PATH_DRIVE + 'plots\\'
    emotion_plot_folder_path = PATH_DRIVE + 'emotion_plots\\'
    word_cloud_folder_path = PATH_DRIVE + 'word_cloud\\'
    removed_plots_folder_path = plots_folder_path + "Removed\\"
    not_removed_plots_folder_path = plots_folder_path + "NotRemoved\\"
    all_plots_folder_path = plots_folder_path + "All\\"

    print("start Removed")
    print("1")
    emotion_detection_removed.get_plot_and_emotion_rate_from_all_posts_in_category(data_cursor=data_cursor,
                                                                           Con_DB=Con_DB,
                                                                           path_to_read_data=resource_path,
                                                                           path_to_save_plt=plots_folder_path,
                                                                           category="Removed",
                                                                           subreddit_name=os.getenv("COLLECTION_NAME"))

    name_entity_removed.extract_NER_from_data(posts=data_cursor, con_db=con_db,
                                              file_name_to_save='Removed_{}_title_selftext_NER.csv'.format(COLLECTION_NAME),
                                              path_to_folder=resource_path, save_deduced_data=True,
                                              is_removed_bool=True)

    print("2")
    path = resource_path + 'Removed_{}_title_selftext_NER.csv'.format(COLLECTION_NAME)
    explore_data_with_NER_and_emotion(n=50, collection_name=COLLECTION_NAME, path_to_read_data=path,
                                      path_to_save_plots=removed_plots_folder_path, Con_DB=con_db,
                                      category="Removed", name_entity=name_entity_removed)

    topic_analisis_path = PATH_DRIVE + '...topic_sentim_general-best.csv'
    connect_NER_Topic_Emotion(
        path_to_topic_csv=topic_analisis_path,
        path_to_removed_NER_emotion_csv=resource_path + "{}_NER_emotion_rate_mean_{}.csv".format("Removed", COLLECTION_NAME),
        path_to_folder=resource_path,
        file_name_to_save='document_topic_table_with_NER.csv'
    )

    # explore_data_with_emotion_to_NER_per_month(category="Removed", Con_DB=con_db,
    #                                            path_to_read_data=folder_path + "NER_per_month.json",
    #                                            path_to_save_plots=plots_folder_path,
    #                                            collection_name=os.getenv("COLLECTION_NAME"),
    #                                            file_reader=file_reader)

    "--------------------------------------------------------------------------------------------------------------"

    print("start Not Removed")
    print("1")
    name_entity_not_removed.extract_NER_from_data(posts=data_cursor, con_db=con_db,
                                                  file_name_to_save='NotRemoved_{}_title_selftext_NER.csv'.format(COLLECTION_NAME),
                                                  path_to_folder=resource_path, save_deduced_data=True,
                                                  is_removed_bool=False)

    print("2")
    path = resource_path + 'NotRemoved_{}_title_selftext_NER.csv'.format(COLLECTION_NAME)
    explore_data_with_NER_and_emotion(n=50, collection_name=COLLECTION_NAME, path_to_read_data=path,
                                      path_to_save_plots=not_removed_plots_folder_path, Con_DB=con_db
                                      , category="NotRemoved", name_entity=name_entity_removed)

    # explore_data_with_emotion_to_NER_per_month(category="NotRemoved", Con_DB=con_db,
    #                                            path_to_read_data=folder_path+"NER_per_month.json",
    #                                            path_to_save_plots=plots_folder_path,
    #                                            collection_name=os.getenv("COLLECTION_NAME"), file_reader=file_reader)

    "--------------------------------------------------------------------------------------------------------------"

    print("start All")

    print("1")
    name_entity_quantity_all.extract_NER_from_data(posts=data_cursor,
                                                   file_name_to_save='All_{}_title_selftext_NER.csv'.format(COLLECTION_NAME),
                                                   path_to_folder=resource_path)

    print("2")
    path = resource_path + 'All_{}_title_selftext_NER.csv'.format(COLLECTION_NAME)
    explore_data_with_NER_and_emotion(n=50, collection_name=COLLECTION_NAME, path_to_read_data=path,
                                      path_to_save_plots=all_plots_folder_path, Con_DB=con_db
                                      , category="All")

    # explore_data_with_emotion_to_NER_per_month(category="All", Con_DB=con_db,
    #                                            path_to_read_data=folder_path + "NER_per_month.json",
    #                                            path_to_save_plots=plots_folder_path,
    #                                            collection_name=os.getenv("COLLECTION_NAME"),
    #                                            file_reader=file_reader)

    ########################## NER QUANTITY ###############################

    path = resource_path + 'Removed_{}_title_selftext_NER.csv'.format(COLLECTION_NAME)
    NER_post_quantity(path_to_read_data=path, category="Removed", n=50, name_entity=name_entity_quantity_removed,
                      resource_path=resource_path)

    path = resource_path + 'NotRemoved_{}_title_selftext_NER.csv'.format(COLLECTION_NAME)
    NER_post_quantity(path_to_read_data=path, category="NotRemoved", n=50, name_entity=name_entity_quantity_not_removed,
                      resource_path=resource_path)

    path = resource_path + 'All_{}_title_selftext_NER.csv'.format(COLLECTION_NAME)
    NER_post_quantity(path_to_read_data=path, category="All", n=50, name_entity=name_entity_quantity_all,
                      resource_path=resource_path)


    ####################### NER per emotion with TOPIC NALYSIS ##############################

    emotions = ["Angry", "Fear", "Happy", "Sad", "Surprise"]
    data_category_df = pd.read_csv(resource_path + "{}_NER_emotion_rate_mean_{}.csv".format("Removed", COLLECTION_NAME))
    not_removed_df = pd.read_csv(resource_path + "{}_NER_emotion_rate_mean_{}.csv".format("NotRemoved", COLLECTION_NAME))

    # all_df = pd.read_csv(folder_path + "All_NER_emotion_rate_mean_wallstreetbets.csv")

    r_entities_set = set([entity[1] for index, entity in data_category_df.iterrows()])
    nr_entities_set = set([entity[1] for index, entity in not_removed_df.iterrows()])
    entities_set = r_entities_set.intersection(nr_entities_set)

    for entity in entities_set:
        emotion_detection.emotion_plot_per_NER(date_format='%Y/%m', subreddit_name=COLLECTION_NAME,
                                               NER=entity,
                                               path_to_save_plt=emotion_plot_folder_path,
                                               removed_df=data_category_df.loc[data_category_df['entity'] == entity],
                                               not_removed_df=not_removed_df.loc[not_removed_df['entity'] == entity],
                                               # all_df=all_df.loc[all_df['entity'] == entity],
                                               emotions_list_category=emotions)

    ########################## WORD CLOUD ###################################

        # from wordcloud import WordCloud
        # from matplotlib import pyplot as plt
        # from db_utils.FileReader import FileReader
        # file_reader = FileReader()
        # df = file_reader.read_from_csv(path= resource_path + 'RemovedNER_quantity.csv')
        # lst = []
        # for index, row in df.iterrows():
        #     lst.append(row['NER'])
        # NER_words = " ".join(lst)
        # wordcloud = WordCloud().generate(NER_words)
        # # Display the generated image:
        # plt.imshow(wordcloud)
        # plt.savefig( word_cloud_folder_path + "word_cloud_removed")

        # def draw_bars():
        #     creatSentiment_and_statistic_Graph()
        #     stat_all.draw_statistic_bars(api_all)