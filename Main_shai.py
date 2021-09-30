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

import matplotlib.pyplot as plt
from matplotlib.dates import date2num
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

:returns emotion analysis for the n NERS and save it to csv file.
'''

def explore_data_with_NER_and_emotion(n, path_to_read_data, collection_name, path_to_save_plots,
                                      Con_DB, category, name_entity):
    NER_emotion_df = None
    n_common_NER_title_selftext = name_entity.most_N_common_NER(N=n, path=path_to_read_data)
    NER_BY_Type = name_entity.get_NER_BY_Type(n_common_NER_title_selftext, 'ORG', 'PERSON', 'PRODUCT')
    NER_emotion_df = get_emotion_to_NER(Con_DB, NER_BY_Type, NER_emotion_df, category, collection_name,
                                        path_to_save_plots)
    File = category + "_NER_emotion_rate_mean_wallstreetbets.csv"
    file_reader.write_to_csv(df_to_write=NER_emotion_df, path=folder_path,
                             file_name=File)


def get_emotion_to_NER(Con_DB, NER_BY_Type, NER_emotion_df, category, collection_name, path_to_save_plots):
    flag = True
    for type_item in NER_BY_Type:
        newpath = "{}{}\\".format(path_to_save_plots, str(type_item))
        if not os.path.exists(newpath):
            os.makedirs(newpath)
        for NER_item, posts_ids_list in NER_BY_Type[type_item].items():
            relevant_posts = Con_DB.get_specific_items_by_post_ids(ids_list=posts_ids_list)
            emotion_detection.extract_posts_emotion_rate(relevant_posts, Con_DB)
            emotion_detection.calculate_post_emotion_rate_mean()
            emotion_detection.emotion_plot_for_posts_in_subreddit(date_format='%Y/%m', subreddit_name=collection_name
                                                                  , NER=NER_item, path_to_save_plt=newpath,
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


def NER_post_quantity(category, path_to_read_data, n, name_entity):

    n_common_NER_title_selftext = name_entity.most_N_common_NER(N=n, path=path_to_read_data)
    list_to_save = [(object[0], object[1][0], object[1][1][0]) for object in n_common_NER_title_selftext]

    df = pd.DataFrame(data=list_to_save, columns=['NER', 'NER_TYPE', 'number_of_posts'])
    file_reader.write_to_csv(path="C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\files\\",
                             file_name=category + 'NER_quantity.csv', df_to_write=df)

    NER_BY_Type = name_entity.get_NER_BY_Type(n_common_NER_title_selftext, 'ORG', 'PERSON', 'PRODUCT', 'FAC')
    file_reader.write_dict_to_json(path="C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\files\\",
                                   file_name=category + 'NER_quantity_byType', dict_to_write=NER_BY_Type)


def explore_data_with_emotion_to_NER_per_month(Con_DB, file_reader,  path_to_read_data, category, collection_name,
                                               path_to_save_plots):
    NER_emotion_df = None
    # should return python dict when key is date and values another diat that keys is NERs and value is ids_list
    items = file_reader.read_from_json_to_dict(path_to_read_data)
    NER_emotion_df = get_emotion_to_NER(Con_DB, items, NER_emotion_df, category, collection_name,
                                            path_to_save_plots)
    File = category + "_NER_emotion_per_month.csv"
    file_reader.write_to_csv(df_to_write=NER_emotion_df, path=folder_path,
                             file_name=File)

if __name__ == '__main__':

    path_drive = 'G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/'

    name_entity_not_removed = NameEntity()
    name_entity_removed = NameEntity()
    name_entity_quantity_removed = NameEntity()
    name_entity_quantity_not_removed = NameEntity()
    name_entity_quantity_all = NameEntity()

    file_reader = FileReader()
    emotion_detection = EmotionDetection()
    con_db = Con_DB()
    for k in range(2, 3):
        con_db.set_client(k)
        data_cursor = con_db.get_cursor_from_mongodb(db_name='reddit', collection_name='wallstreetbets').find({})
        folder_path = 'C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\files\\'
        plots_folder_path = 'C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\plts\\'
        removed_plots_folder_path = plots_folder_path + "Removed\\"
        not_removed_plots_folder_path = plots_folder_path + "NotRemoved\\"
        all_plots_folder_path = plots_folder_path + "All\\"

        print("start Removed")
        print("1")
        name_entity_removed.extract_NER_from_data(posts=data_cursor,
                                          file_name_to_save='Removed_wallstreetbets_title_selftext_NER.csv',
                                          path_to_folder=folder_path, save_deduced_data=True)

        print("2")
        path = folder_path + 'Removed_wallstreetbets_title_selftext_NER.csv'
        explore_data_with_NER_and_emotion(n=50, collection_name="wallstreetbets", path_to_read_data=path,
                                          path_to_save_plots=removed_plots_folder_path, Con_DB=con_db
                                          , category="Removed", name_entity=name_entity_removed)


        print("start Not Removed")
        print("1")
        name_entity_not_removed.extract_NER_from_data(posts=data_cursor, con_db=con_db,
                                          file_name_to_save='NotRemoved_wallstreetbets_title_selftext_NER.csv',
                                          path_to_folder=folder_path, save_deduced_data=True)

        print("2")
        path = folder_path + 'NotRemoved_wallstreetbets_title_selftext_NER.csv'
        explore_data_with_NER_and_emotion(n=50, collection_name="wallstreetbets", path_to_read_data=path,
                                          path_to_save_plots=not_removed_plots_folder_path, Con_DB=con_db
                                          , category="NotRemoved", name_entity=name_entity_removed)

        explore_data_with_emotion_to_NER_per_month(category="NotRemoved", Con_DB=con_db,
                                                   path_to_read_data=folder_path+"NER_per_month2.json",
                                                   path_to_save_plots=plots_folder_path,
                                                   collection_name="wallstreetbets", file_reader=file_reader)

        print("start All")

        print("1")
        name_entity_quantity_all.extract_NER_from_data(posts=data_cursor,
                                          file_name_to_save='All_wallstreetbets_title_selftext_NER.csv',
                                          path_to_folder=folder_path)

        print("2")
        path = folder_path + 'All_wallstreetbets_title_selftext_NER.csv'
        explore_data_with_NER_and_emotion(n=50, collection_name="wallstreetbets", path_to_read_data=path,
                                          path_to_save_plots=all_plots_folder_path, Con_DB=con_db
                                          , category="All")


    ########################## NER QUANTITY ###############################

        path = folder_path + 'Removed_wallstreetbets_title_selftext_NER.csv'
        NER_post_quantity(path_to_read_data=path, category="Removed", n=50, name_entity=name_entity_quantity_removed)

        path = folder_path + 'NotRemoved_wallstreetbets_title_selftext_NER.csv'
        NER_post_quantity(path_to_read_data=path, category="NotRemoved", n=50, name_entity=name_entity_quantity_not_removed)

        path = folder_path + 'All_wallstreetbets_title_selftext_NER.csv'
        NER_post_quantity(path_to_read_data=path, category="All", n=50)


    ########################## WORD CLOUD ###################################

        # from wordcloud import WordCloud
        # from matplotlib import pyplot as plt
        # from db_utils.FileReader import FileReader
        # file_reader = FileReader()
        # df = file_reader.read_from_csv(path='C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\files\\RemovedNER_quantity.csv')
        # lst = []
        # for index, row in df.iterrows():
        #     lst.append(row['NER'])
        # NER_words = " ".join(lst)
        # wordcloud = WordCloud().generate(NER_words)
        # # Display the generated image:
        # plt.imshow(wordcloud)
        # plt.savefig("C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\word_cloud_removed")

        # def draw_bars():
        #     creatSentiment_and_statistic_Graph()
        #     stat_all.draw_statistic_bars(api_all)