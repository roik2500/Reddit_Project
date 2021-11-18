from tqdm import tqdm
from db_utils.FileReader import FileReader
from db_utils.Con_DB import Con_DB
import csv
import os
import pandas as pd
from text_analysis.emotion_detection import EmotionDetection
from text_analysis.name_entity_recognition import NameEntity

# from matplotlib.dates import date2num
import datetime
from wordcloud import WordCloud
from matplotlib import pyplot as plt


'''
Creating a new folder in Google drive. 
:argument folderName - name of the new folder
:return full path to the new folder
'''
def create_new_folder_drive(path, new_folder):
    updated_path = "{}{}/".format(path,new_folder)
    if not os.path.exists(updated_path):
        os.makedirs(updated_path)
    return updated_path

'''
:argument This method get path to data after that was go through name_entity_recognition.get_entites()
:argument n is the number of NER to return 
:argument  collection name is the subreddit name that save in mnongoDB

This method extract NER emotion on plot the x axis is month and y axis is emotion rate.
the plot is over a  months in one year.

:returns emotion analysis for the n NERS and save it to csv file.
'''

def explore_data_with_NER_and_emotion(n, path_to_read_data, collection_name, path_to_save_plots,
                                      Con_DB, category, name_entity, file_reader, post_or_comment_arg):
    NER_emotion_df = None
    n_common_NER_title_selftext = name_entity.most_N_common_NER(N=n, path=path_to_read_data)
    NER_BY_Type = name_entity.get_NER_BY_Type(n_common_NER_title_selftext, 'ORG', 'PERSON', 'PRODUCT')
    NER_emotion_df = get_emotion_to_NER(Con_DB, NER_BY_Type, NER_emotion_df, category, collection_name,
                                        path_to_save_plots, post_or_comment_arg)
    # example name of file - "Removed_NER_emotion_rate_mean_wallstreetbets.csv"
    File = "{}_NER_emotion_rate_mean_{}_{}_NERS.csv".format(category, collection_name,n)
    file_reader.write_to_csv(df_to_write=NER_emotion_df, path=resource_path,
                             file_name=File)


def get_emotion_to_NER(Con_DB, NER_BY_Type, NER_emotion_df, category, collection_name, path_to_save_plots,
                       post_or_comment_arg):
    flag = True
    for type_item in NER_BY_Type:
        new_file = "{}{}/".format(path_to_save_plots, str(type_item.replace("\\", "-")))
        if not os.path.exists(new_file):
            os.makedirs(new_file)
        for NER_item, posts_ids_list in NER_BY_Type[type_item].items():
            relevant_posts = Con_DB.get_specific_items_by_object_ids(ids_list=posts_ids_list[1], post_or_comment_arg=post_or_comment_arg)
            emotion_detection.extract_posts_emotion_rate(relevant_posts, Con_DB, post_need_to_extract=False,
                                                         category=category, post_or_comment_arg=post_or_comment_arg)
            emotion_detection.calculate_post_emotion_rate_mean()
            emotion_detection.emotion_plot_for_posts_in_subreddit(date_format='%Y/%m', subreddit_name=collection_name
                                                                  , NER=NER_item, path_to_save_plt=new_file,
                                                                  category=category)
            temp_df = pd.DataFrame({
                "entity": [NER_item],
                "Disgust": [list(emotion_detection.emotion_posts_avg_of_subreddit["Disgust"].items())],
                # "Others": [list(emotion_detection.emotion_posts_avg_of_subreddit["Others"].items())],
                "Anger": [list(emotion_detection.emotion_posts_avg_of_subreddit["Anger"].items())],
                "Fear": [list(emotion_detection.emotion_posts_avg_of_subreddit["Fear"].items())],
                "Surprise": [list(emotion_detection.emotion_posts_avg_of_subreddit["Surprise"].items())],
                "Sadness": [list(emotion_detection.emotion_posts_avg_of_subreddit["Sadness"].items())],
                "Joy": [list(emotion_detection.emotion_posts_avg_of_subreddit["Joy"].items())]
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
                             file_name='{}_NER_quantity.csv'.format(category), df_to_write=df)

    NER_BY_Type = name_entity.get_NER_BY_Type(n_common_NER_title_selftext, 'ORG', 'PERSON', 'PRODUCT')
    file_reader.write_dict_to_json(path=resource_path,
                                   file_name='{}_NER_quantity_byType'.format(category), dict_to_write=NER_BY_Type)


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


def get_wordcloud_from_csv(csv_file_name_to_read, category):
    new_file = "{}{}/".format(word_cloud_folder_path, category)
    if not os.path.exists(new_file):
        os.makedirs(new_file)
    df = file_reader.read_from_csv(path=csv_file_name_to_read)
    lst = []
    for index, row in df.iterrows():
        lst.append(row['NER'])
    NER_words = " ".join(lst)
    wordcloud = WordCloud().generate(NER_words)
    # Display the generated image:
    plt.imshow(wordcloud)
    plt.savefig(new_file + '{}_word_cloud'.format(category))

def get_wordcloud_from_json(csv_file_name_to_read, category):
        new_file = "{}{}/".format(word_cloud_folder_path, category)
        if not os.path.exists(new_file):
            os.makedirs(new_file)
        relevent_NER_dict = file_reader.read_from_json_to_dict(PATH=csv_file_name_to_read)
        lst = []
        for NER_Type_key in relevent_NER_dict.keys():
            new_file_NER_type = "{}{}/".format(new_file, NER_Type_key)
            if not os.path.exists(new_file_NER_type):
                os.makedirs(new_file_NER_type)
            for NER_key, value in relevent_NER_dict[NER_Type_key].items():
                lst.append(NER_key)

            NER_words = " ".join(lst)
            wordcloud = WordCloud().generate(NER_words)
            # Display the generated image:
            plt.imshow(wordcloud)
            plt.savefig(new_file_NER_type + '{}_word_cloud'.format(category))




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
    PATH_DRIVE_EMOTION_DETECTION = create_new_folder_drive(path=os.getenv("OUTPUTS_DIR"), new_folder='emotion_detection')
    PATH_DRIVE_COMMENTS = create_new_folder_drive(path=PATH_DRIVE_EMOTION_DETECTION, new_folder='posts')

    data_cursor = con_db.get_cursor_from_mongodb(db_name='local', collection_name=COLLECTION_NAME) #.find({})

    ''' CREATE FOLDERS IF NEEDED'''
    resource_path = create_new_folder_drive(PATH_DRIVE_COMMENTS, 'resources/')
    plots_folder_path = create_new_folder_drive(PATH_DRIVE_COMMENTS, 'plots/')
    emotion_plot_folder_path = create_new_folder_drive(PATH_DRIVE_COMMENTS, 'emotion_plots/')
    word_cloud_folder_path = create_new_folder_drive(PATH_DRIVE_COMMENTS, 'word_cloud/')

    removed_plots_folder_path = create_new_folder_drive(plots_folder_path, "Removed/")
    not_removed_plots_folder_path = create_new_folder_drive(plots_folder_path, "NotRemoved/")
    all_plots_folder_path = create_new_folder_drive(plots_folder_path, "All/")

    removed_resource_path = create_new_folder_drive(resource_path, "Removed/")
    not_removed_resource_path = create_new_folder_drive(resource_path, "NotRemoved/")
    all_resource_path = create_new_folder_drive(resource_path, "All/")

    print("start Removed")
    print("1")
    emotion_detection_removed.get_plot_and_emotion_rate_from_all_posts_in_category(data_cursor=data_cursor,
                                                                           Con_DB=con_db,
                                                                           path_to_read_data=removed_resource_path,
                                                                           path_to_save_plt=removed_plots_folder_path,
                                                                           category="Removed",
                                                                           subreddit_name=COLLECTION_NAME,
                                                                           file_reader=file_reader,
                                                                           post_or_comment_arg='comment')

    print("2")
    data_cursor = con_db.get_cursor_from_mongodb(db_name='reddit', collection_name=COLLECTION_NAME).find({})

    name_entity_removed.extract_NER_from_data(posts=data_cursor, con_db=con_db,
                                              file_name_to_save='Removed_{}_title_selftext_NER.csv'.format(COLLECTION_NAME),
                                              path_to_folder=removed_resource_path, save_deduced_data=True,
                                              is_removed_bool=True, post_or_comment_arg='comment')

    print("3")
    path = removed_resource_path + 'Removed_{}_title_selftext_NER.csv'.format(COLLECTION_NAME)
    explore_data_with_NER_and_emotion(n=50, collection_name=COLLECTION_NAME, path_to_read_data=path,
                                      path_to_save_plots=removed_plots_folder_path, Con_DB=con_db,
                                      post_or_comment_arg='comment',
                                      file_reader=file_reader, category="Removed", name_entity=name_entity_removed)


    print("DONE")
    # topic_analisis_path = PATH_DRIVE + '...topic_sentim_general-best.csv'
    # connect_NER_Topic_Emotion(
    #     path_to_topic_csv=topic_analisis_path,
    #     path_to_removed_NER_emotion_csv=resource_path + "{}_NER_emotion_rate_mean_{}.csv".format("Removed", COLLECTION_NAME),
    #     path_to_folder=resource_path,
    #     file_name_to_save='document_topic_table_with_NER.csv'
    # )

    # explore_data_with_emotion_to_NER_per_month(category="Removed", Con_DB=con_db,
    #                                            path_to_read_data=removed_resource_path + "Removed_NER_per_month.json",
    #                                            path_to_save_plots=plots_folder_path,
    #                                            collection_name=os.getenv("COLLECTION_NAME"),
    #                                            file_reader=file_reader)

    "--------------------------------------------------------------------------------------------------------------"

    print("start Not Removed")
    print("1")
    emotion_detection_removed.get_plot_and_emotion_rate_from_all_posts_in_category(data_cursor=data_cursor,
                                                                                   Con_DB=con_db,
                                                                                   path_to_read_data=not_removed_resource_path,
                                                                                   path_to_save_plt=not_removed_plots_folder_path,
                                                                                   category="NotRemoved",
                                                                                   subreddit_name=COLLECTION_NAME,
                                                                                   file_reader=file_reader,
                                                                                   post_or_comment_arg='comment')
    print("2")
    name_entity_not_removed.extract_NER_from_data(posts=data_cursor, con_db=con_db,
                                                  file_name_to_save='NotRemoved_{}_title_selftext_NER.csv'.format(COLLECTION_NAME),
                                                  path_to_folder=not_removed_resource_path, save_deduced_data=True,
                                                  is_removed_bool=False, post_or_comment_arg='comment')

    print("3")
    path = not_removed_resource_path + 'NotRemoved_{}_title_selftext_NER.csv'.format(COLLECTION_NAME)
    explore_data_with_NER_and_emotion(n=50, collection_name=COLLECTION_NAME, path_to_read_data=path,
                                      path_to_save_plots=not_removed_plots_folder_path, Con_DB=con_db,
                                      post_or_comment_arg='comment',
                                      file_reader=file_reader, category="NotRemoved", name_entity=name_entity_removed)

    print("DONE")

    # explore_data_with_emotion_to_NER_per_month(category="NotRemoved", Con_DB=con_db,
    #                                             path_to_read_data=not_removed_resource_path+"NotRemoved_NER_per_month.json",
    #                                             path_to_save_plots=plots_folder_path,
    #                                             collection_name=os.getenv("COLLECTION_NAME"), file_reader=file_reader)

    "--------------------------------------------------------------------------------------------------------------"

    print("start All")
    print("1")
    emotion_detection_removed.get_plot_and_emotion_rate_from_all_posts_in_category(data_cursor=data_cursor,
                                                                                   Con_DB=con_db,
                                                                                   path_to_read_data=all_resource_path,
                                                                                   path_to_save_plt=all_plots_folder_path,
                                                                                   category="All",
                                                                                   subreddit_name=COLLECTION_NAME,
                                                                                   file_reader=file_reader,
                                                                                   post_or_comment_arg='comment')

    print("2")
    name_entity_quantity_all.extract_NER_from_data(posts=data_cursor,
                                                   file_name_to_save='All_{}_title_selftext_NER.csv'.format(COLLECTION_NAME),
                                                   path_to_folder=all_resource_path, post_or_comment_arg='comment')

    print("3")
    path = all_resource_path + 'All_{}_title_selftext_NER.csv'.format(COLLECTION_NAME)
    explore_data_with_NER_and_emotion(n=50, collection_name=COLLECTION_NAME, path_to_read_data=path,
                                      path_to_save_plots=all_plots_folder_path, Con_DB=con_db
                                      , category="All")

    # explore_data_with_emotion_to_NER_per_month(category="All", Con_DB=con_db,
    #                                            path_to_read_data=all_resource_path + "All_NER_per_month.json",
    #                                            path_to_save_plots=plots_folder_path,
    #                                            collection_name=os.getenv("COLLECTION_NAME"),
    #                                            file_reader=file_reader)

    # ########################## NER QUANTITY ###############################

    path = removed_resource_path + 'Removed_{}_title_selftext_NER.csv'.format(COLLECTION_NAME)
    NER_post_quantity(path_to_read_data=path, category="Removed", n=50, name_entity=name_entity_quantity_removed,
                      resource_path=removed_resource_path)

    path = not_removed_resource_path + 'NotRemoved_{}_title_selftext_NER.csv'.format(COLLECTION_NAME)
    NER_post_quantity(path_to_read_data=path, category="NotRemoved", n=50, name_entity=name_entity_quantity_not_removed,
                      resource_path=not_removed_resource_path)

    path = all_resource_path + 'All_{}_title_selftext_NER.csv'.format(COLLECTION_NAME)
    NER_post_quantity(path_to_read_data=path, category="All", n=50, name_entity=name_entity_quantity_all,
                      resource_path=all_resource_path)


    # ####################### NER per emotion with##############################

    # emotions = ["Disgust", "Others", "Anger", "Fear", "Surprise", "Sadness", "Joy"]
    # data_category_df = pd.read_csv(resource_path + "{}_NER_emotion_rate_mean_{}_50_NERS.csv".format("Removed", COLLECTION_NAME))
    # not_removed_df = pd.read_csv(resource_path + "{}_NER_emotion_rate_mean_{}_50_NERS.csv".format("NotRemoved", COLLECTION_NAME))
    # # all_df = pd.read_csv(resource_path + "All_NER_emotion_rate_mean_wallstreetbets.csv")
    #
    # r_entities_set = set([entity[1] for index, entity in data_category_df.iterrows()])
    # nr_entities_set = set([entity[1] for index, entity in not_removed_df.iterrows()])
    # entities_set = r_entities_set.intersection(nr_entities_set)
    #
    # for entity in entities_set:
    #     emotion_detection.emotion_plot_per_NER(date_format='%Y/%m', subreddit_name=COLLECTION_NAME,
    #                                            NER=entity,
    #                                            path_to_save_plt=emotion_plot_folder_path,
    #                                            removed_df=data_category_df.loc[data_category_df['entity'] == entity],
    #                                            not_removed_df=not_removed_df.loc[not_removed_df['entity'] == entity],
    #                                            # all_df=all_df.loc[all_df['entity'] == entity],
    #                                            emotions_list_category=emotions)




    # ########################## WORD CLOUD ###################################


    # file_reader = FileReader()
    # path = resource_path + 'RemovedNER_quantity.csv'
    # get_wordcloud_from_csv(csv_file_name_to_read=path, category="Removed")
    #
    # path = resource_path + 'NotRemovedNER_quantity.csv'
    # get_wordcloud_from_csv(csv_file_name_to_read=path, category="NotRemoved")
    #
    # path = resource_path + 'RemovedNER_quantity_byType.json'
    # get_wordcloud_from_json(csv_file_name_to_read=path, category="Removed")
    #
    # path = resource_path + 'NotRemovedNER_quantity_byType.json'
    # get_wordcloud_from_json(csv_file_name_to_read=path, category="NotRemoved")
