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
from wordcloud import WordCloud, STOPWORDS
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

def explore_data_with_NER_and_emotion(n, path_to_read_data, collection_name, path_to_save_plots, post_or_comment_arg,
                                      Con_DB, category, name_entity, file_reader):
    NER_emotion_df = None
    n_common_NER_title_selftext = name_entity.most_N_common_NER(N=n, path=path_to_read_data)
    NER_BY_Type = name_entity.get_NER_BY_Type(n_common_NER_title_selftext, 'ORG', 'PERSON', 'PRODUCT')
    NER_emotion_df = get_emotion_to_NER(Con_DB, NER_BY_Type, NER_emotion_df, category, collection_name,
                                        path_to_save_plots, post_or_comment_arg)
    # example name of file - "Removed_NER_emotion_rate_mean_wallstreetbets.csv"
    # File = "{}_NER_emotion_rate_mean_{}_{}_NERS.csv".format(category, collection_name,n)
    # file_reader.write_to_csv(df_to_write=NER_emotion_df, path=resource_path,
    #                          file_name=File)


def get_emotion_to_NER(Con_DB, NER_BY_Type, NER_emotion_df, category, collection_name, path_to_save_plots,
                       post_or_comment_arg, date_format='%Y/%m'):
    flag = True
    for type_item in NER_BY_Type:
        new_file = "{}{}/".format(path_to_save_plots, str(type_item.replace("\\", "-")))
        if not os.path.exists(new_file):
            os.makedirs(new_file)
        for NER_item, posts_ids_list in NER_BY_Type[type_item].items():
            if len(posts_ids_list[1]) > 1000:
                continue
            posts_ids_set = set(posts_ids_list[1])
            # relevant_posts = Con_DB.get_NER_full_post_data(NER_TYPE=NER_item+"_"+type_item)
            # relevant_posts = Con_DB.get_specific_items_by_object_ids(ids_list=list(posts_ids_set), post_or_comment_arg=post_or_comment_arg)
            relevant_posts = Con_DB.get_specific_items_by_object_ids(ids_list=list(posts_ids_set), post_or_comment_arg=post_or_comment_arg)
            emotion_detection.extract_posts_emotion_rate(relevant_posts, Con_DB, post_need_to_extract=False,
                                                         category=category, post_or_comment_arg=post_or_comment_arg)
            emotion_detection.calculate_post_emotion_rate_mean()
            emotion_detection.emotion_plot_for_posts_in_subreddit(date_format=date_format, subreddit_name=collection_name
                                                                  , NER=NER_item, path_to_save_plt=new_file,
                                                                  category=category)
            temp_df = pd.DataFrame({
                "entity": [NER_item],
                "Disgust": [list(emotion_detection.emotion_posts_avg_of_subreddit["Disgust"].items())],
                "Neutral": [list(emotion_detection.emotion_posts_avg_of_subreddit["Neutral"].items())],
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
                                               path_to_save_plots, post_or_comment_arg, resource_folder):
    NER_emotion_df = None
    # should return python dict when key is date and values another dict that keys is NERs and value is ids_list
    items = file_reader.read_from_json_to_dict(path_to_read_data)
    NER_emotion_df = get_emotion_to_NER(Con_DB, items, NER_emotion_df, category, collection_name,
                                        path_to_save_plots, post_or_comment_arg, date_format='%Y/%m/%d')
    File = category + "_NER_emotion_rate_per_month.csv"
    file_reader.write_to_csv(df_to_write=NER_emotion_df, path=resource_folder,
                             file_name=File)


def get_wordcloud_of_NER_per_month_based_on_post_quantity(file_reader, path_to_read_data, category, path_to_save_plots):
    NER_quantity_df = None
    objects = file_reader.read_from_json_to_dict(path_to_read_data)
    new_folder = "{}{}/".format(path_to_save_plots, category)
    if not os.path.exists(new_folder):
        os.makedirs(new_folder)
    for date_key, NER_dict in objects.items():
        inside_folder = "{}{}/".format(new_folder, date_key)
        if not os.path.exists(inside_folder):
            os.makedirs(inside_folder)
        NERS_lst = []
        count = []
        counter = 0
        for NER_key, val in NER_dict.items():
            TYPES = set(val[0])
            number_of_instance = val[1][0]
            if len(TYPES.intersection({"ORG", "PERSON", "PRODUCT"})) > 0:
                NERS_lst.append(NER_key)
                count.append(number_of_instance)
                if len(NERS_lst) % 15 == 0:
                    data = dict(zip(NERS_lst, count))
                    get_wordcloud(category=category, dictinary=data, folder_path=inside_folder, number=counter)
                    counter += 1
                    NERS_lst, count = [], []


    File = category + "_NER_post_quantity_per_month.csv"
    file_reader.write_to_csv(df_to_write=NER_quantity_df, path=resource_path,
                             file_name=File)

def connect_NER_Topic_Emotion(path_to_topic_csv, path_to_removed_NER_emotion_csv, path_to_folder, file_name_to_save,
                              name_entity):
    global removed_df
    df = file_reader.read_from_csv(path_to_topic_csv)
    name_entity_list = [[name_entity.get_entites(col['topic'])] for row_index, col in df.iterrows()]
    df_name_entity = pd.DataFrame(data=name_entity_list, columns=['entity'])
    df.insert(4, 'Topic_NER', df_name_entity)
    entity_list = []
    for entities in name_entity_list:
        if entities != [[]]:
            entity_list += [entity[0][0] for entity in entities]
    removed_df = file_reader.read_from_csv(path_to_removed_NER_emotion_csv)
    # not_removed_df = pd.read_csv(folder_path + "NotRemoved_NER_emotion_rate_mean_{}.csv".format(COLLECTION_NAME))
    matches = removed_df[removed_df.iloc[:, 1].isin(entity_list)]
    res = [name_entity.get_entites(col['entity'])[0] for row_index, col in matches.iterrows()]
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
    lst = [row['NER'] for index, row in df.iterrows()]
    # lst = []
    # for index, row in df.iterrows():
    #     lst.append(row['NER'])
    get_wordcloud(category, lst, new_file,1)


def get_wordcloud(category, dictinary, folder_path, number):
    # joined_NER_words = " ".join(dictinary)
    wordcloud = WordCloud(background_color='black', collocations=False).generate_from_frequencies(dictinary)
    # Display the generated image:
    plt.imshow(wordcloud)
    plt.savefig(folder_path + '{}_word_cloud_{}'.format(category, number))


def get_wordcloud_from_json(csv_file_name_to_read, category):
        new_folder = "{}{}/".format(word_cloud_folder_path, category)
        if not os.path.exists(new_folder):
            os.makedirs(new_folder)
        relevent_NER_dict = file_reader.read_from_json_to_dict(PATH=csv_file_name_to_read)
        lst = []
        for NER_Type_key in relevent_NER_dict.keys():
            new_file_NER_type = "{}{}/".format(new_folder, NER_Type_key)
            if not os.path.exists(new_file_NER_type):
                os.makedirs(new_file_NER_type)
            for NER_key, value in relevent_NER_dict[NER_Type_key].items():
                lst.append(NER_key)

            NER_words = " ".join(lst)
            wordcloud = WordCloud().generate(NER_words)
            # Display the generated image:
            plt.imshow(wordcloud)
            plt.savefig(new_file_NER_type + '{}_word_cloud'.format(category))


def topic_emotion_detection(path_to_topic_csv, resource, file_reader, Con_DB, folder_to_save_plots, emotion_detection):
    topic_num_posts_dict = file_reader.topic_number_connected_posts(path_to_topic_csv, resource)
    for topic_number, objects_list in topic_num_posts_dict.items():
        full_objects = con_db.get_specific_items_by_object_ids_from_mongodb(ids_list=objects_list[0:100],
                                                                            post_or_comment_arg='post')
        emotion_detection.extract_posts_emotion_rate(
                                                    posts=full_objects, con_DB=Con_DB,
                                                    post_need_to_extract=False, category="All",
                                                    post_or_comment_arg='post')

        emotion_detection.calculate_post_emotion_rate_mean()
        emotion_detection.emotion_plot_for_posts_in_subreddit(date_format='%Y/%m',
                                                              subreddit_name='',
                                                              NER='topic_{}'.format(topic_number), path_to_save_plt=folder_to_save_plots,
                                                              category='All')

        emotion_detection.emotion_dict = {}
        emotion_detection.emotion_posts_avg_of_subreddit = {'Anger': {}, 'Disgust': {}, 'Fear': {}, 'Joy': {},
                                                            'Sadness': {}, 'Surprise': {}, 'Neutral': {}}
        break



if __name__ == '__main__':

    name_entity_not_removed = NameEntity()
    name_entity_removed = NameEntity()
    name_entity_all = NameEntity()
    name_entity_quantity_removed = NameEntity()
    name_entity_quantity_not_removed = NameEntity()
    name_entity_quantity_all = NameEntity()

    file_reader = FileReader()
    emotion_detection = EmotionDetection()
    emotion_detection_removed = EmotionDetection()
    emotion_detection_not_removed = EmotionDetection()
    emotion_detection_all = EmotionDetection()
    con_db = Con_DB()

    COLLECTION_NAME = os.getenv("COLLECTION_NAME")
    PATH_DRIVE_EMOTION_DETECTION = create_new_folder_drive(path=os.getenv("OUTPUTS_DIR"), new_folder='emotion_detection/wallstreetbets')
    PATH_DRIVE_POSTS = create_new_folder_drive(path=PATH_DRIVE_EMOTION_DETECTION, new_folder='posts')

    data_cursor = con_db.get_cursor_from_mongodb(db_name='reddit', collection_name=COLLECTION_NAME).find({}) #.limit(30000)

    ''' CREATE FOLDERS IF NEEDED'''
    resource_path = create_new_folder_drive(PATH_DRIVE_POSTS, 'resources/')
    plots_folder_path = create_new_folder_drive(PATH_DRIVE_POSTS, 'plots/')
    plots_per_month_folder_path = create_new_folder_drive(PATH_DRIVE_POSTS, 'plots_per_month/')
    emotion_plot_folder_path = create_new_folder_drive(PATH_DRIVE_POSTS, 'emotion_plots/')
    word_cloud_folder_path = create_new_folder_drive(PATH_DRIVE_POSTS, 'word_cloud/')

    removed_plots_folder_path = create_new_folder_drive(plots_folder_path, "Removed/")
    not_removed_plots_folder_path = create_new_folder_drive( plots_folder_path, "NotRemoved/")
    all_plots_folder_path = create_new_folder_drive(plots_folder_path, "All/")

    removed_plots_per_month_folder_path = create_new_folder_drive(plots_per_month_folder_path, "Removed/")
    not_removed_plots_per_month_folder_path = create_new_folder_drive(plots_per_month_folder_path, "NotRemoved/")
    all_plots_per_month_folder_path = create_new_folder_drive(plots_per_month_folder_path, "All/")

    removed_resource_path = create_new_folder_drive(resource_path, "Removed/")
    not_removed_resource_path = create_new_folder_drive(resource_path, "NotRemoved/")
    all_resource_path = create_new_folder_drive(resource_path, "All/")
    #
    print("start Removed")
    print("1")
    emotion_detection_removed.get_plot_and_emotion_rate_from_all_posts_in_category(data_cursor=data_cursor,
                                                                           Con_DB=con_db,
                                                                           path_to_save_plt=removed_plots_folder_path,
                                                                           category="Removed",
                                                                           resources=removed_resource_path,
                                                                           subreddit_name=COLLECTION_NAME,
                                                                           file_reader=file_reader,
                                                                           post_need_to_extract=True,
                                                                           post_or_comment_arg='post')

    print("2")
    name_entity_removed.extract_NER_from_data(posts=data_cursor, con_db=con_db,
                                              file_name_to_save='Removed_{}_title_selftext_NER.csv'.format(COLLECTION_NAME),
                                              path_to_folder=removed_resource_path, category='Removed',
                                              is_removed_bool=True, post_or_comment_arg='post')

    print("3")
    path = removed_resource_path + 'Removed_{}_title_selftext_NER.csv'.format(COLLECTION_NAME)
    explore_data_with_NER_and_emotion(n=50, collection_name=COLLECTION_NAME, path_to_read_data=path,
                                      path_to_save_plots=removed_plots_folder_path, Con_DB=con_db,
                                      post_or_comment_arg='post',
                                      file_reader=file_reader, category="Removed", name_entity=name_entity_removed)

    print("4")
    # get_wordcloud_of_NER_per_month_based_on_post_quantity(file_reader=file_reader,
    #                                                       path_to_read_data=removed_resource_path + "NER_per_month.json",
    #                                                       category="Removed",
    #                                                       # collection_name=os.getenv("COLLECTION_NAME"),
    #                                                       path_to_save_plots=word_cloud_folder_path)
    #
    # print("DONE")
    # topic_analisis_path = os.getenv("OUTPUTS_DIR") + '...topic_sentim_general-best.csv'
    # topic_analisis_path = 'C:\\Users\\User\\Documents\\FourthYear\\Project\\document_topic_table_general-0_updated.csv'
    # plots_folder = create_new_folder_drive(plots_folder_path, 'topic_emotion_plots/')
    # topic_emotion_detection(
    #     path_to_topic_csv=topic_analisis_path,
    #     resource=resource_path,
    #     file_reader=file_reader,
    #     Con_DB=con_db,
    #     folder_to_save_plots=plots_folder,
    #     emotion_detection=emotion_detection_removed
    # )
    # connect_NER_Topic_Emotion(
    #     path_to_topic_csv=topic_analisis_path,
    #     path_to_removed_NER_emotion_csv=removed_resource_path + "{}_NER_emotion_rate_mean_{}_{}_NERS.csv".format("Removed", COLLECTION_NAME, 50),
    #     path_to_folder=resource_path,
    #
    #     name_entity=name_entity_removed,
    #     file_name_to_save='document_topic_table_with_NER.csv'
    # )
    #
    # explore_data_with_emotion_to_NER_per_month(category="Removed", Con_DB=con_db,
    #                                            path_to_read_data=removed_resource_path + "Removed_NER_per_month.json",
    #                                            path_to_save_plots=removed_plots_folder_path, post_or_comment_arg='post',
    #                                            collection_name=os.getenv("COLLECTION_NAME"),
    #                                            file_reader=file_reader)
    #
    "--------------------------------------------------------------------------------------------------------------"

    data_cursor = con_db.get_cursor_from_mongodb(db_name='local', collection_name=COLLECTION_NAME)  # .find({}).limit(30000)

    print("start Not Removed")
    print("1")
    # emotion_detection_not_removed.get_plot_and_emotion_rate_from_all_posts_in_category(data_cursor=data_cursor,
    #                                                                        Con_DB=con_db,
    #                                                                        # path_to_read_data=not_removed_resource_path,
    #                                                                        path_to_save_plt=not_removed_plots_folder_path,
    #                                                                        category="NotRemoved",
    #                                                                        resources=not_removed_resource_path,
    #                                                                        subreddit_name=COLLECTION_NAME,
    #                                                                        file_reader=file_reader,
    #                                                                        post_need_to_extract=True,
    #                                                                        post_or_comment_arg='post')
    #
    print("2")
    emotion_detection_not_removed = None
    name_entity_not_removed.extract_NER_from_data(posts=data_cursor, con_db=con_db,
                                                 file_name_to_save='NotRemoved_{}_title_selftext_NER.csv'.format(COLLECTION_NAME),
                                                 path_to_folder=not_removed_resource_path, category='NotRemoved',
                                                 is_removed_bool=False, post_or_comment_arg='post')

    print("3")
    path = not_removed_resource_path + 'NotRemoved_{}_title_selftext_NER.csv'.format(COLLECTION_NAME)
    explore_data_with_NER_and_emotion(n=50, collection_name=COLLECTION_NAME, path_to_read_data=path,
                                     path_to_save_plots=not_removed_plots_folder_path, Con_DB=con_db,
                                     post_or_comment_arg='post',
                                     file_reader=file_reader, category="NotRemoved",
                                     name_entity=name_entity_not_removed)

    name_entity_not_removed = None
    print("DONE")
    #
    # explore_data_with_emotion_to_NER_per_month(category="NotRemoved", Con_DB=con_db,
    #                                            path_to_read_data=not_removed_resource_path + "NotRemoved_NER_per_month.json",
    #                                            path_to_save_plots=not_removed_plots_per_month_folder_path, post_or_comment_arg='post',
    #                                            collection_name=os.getenv("COLLECTION_NAME"),
    #                                            resource_folder=not_removed_resource_path,
    #                                            file_reader=file_reader)
    #
    # "--------------------------------------------------------------------------------------------------------------"

    data_cursor = con_db.get_cursor_from_mongodb(db_name='local', collection_name=COLLECTION_NAME)  # .find({}).limit(30000)
    print("start All")
    print("1")
    # emotion_detection_all.get_plot_and_emotion_rate_from_all_posts_in_category(data_cursor=data_cursor,
    #                                                                      Con_DB=con_db,
    #                                                                      path_to_save_plt=all_plots_folder_path,
    #                                                                      category="All",
    #                                                                      resources=all_resource_path,
    #                                                                      subreddit_name=COLLECTION_NAME,
    #                                                                      file_reader=file_reader,
    #                                                                      post_need_to_extract = True,
    #                                                                      post_or_comment_arg='post')
    #
    # print("2")
    # emotion_detection_not_removed = None
    # name_entity_all.extract_NER_from_data(posts=data_cursor, con_db=con_db,
    #                                      file_name_to_save='All_{}_title_selftext_NER.csv'.format(COLLECTION_NAME),
    #                                      path_to_folder=all_resource_path, category='All',
    #                                      is_removed_bool=False, post_or_comment_arg='post')

    print("3")
    path = all_resource_path + 'All_{}_title_selftext_NER.csv'.format(COLLECTION_NAME)
    explore_data_with_NER_and_emotion(n=50, collection_name=COLLECTION_NAME, path_to_read_data=path,
                                     path_to_save_plots=all_plots_folder_path, Con_DB=con_db,
                                     post_or_comment_arg='post',
                                     category="All", file_reader=file_reader, name_entity=name_entity_all)
    print('DONE')
    # explore_data_with_emotion_to_NER_per_month(category="All", Con_DB=con_db,
    #                                             path_to_read_data=resource_path + "NER_per_month.json",
    #                                             path_to_save_plots=plots_folder_path,
    #                                             collection_name=os.getenv("COLLECTION_NAME"),
    #                                             file_reader=file_reader)

    # # ########################## WORD CLOUD ###################################
    #
    #
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



    #  ########################## NER QUANTITY ###############################

    # path = removed_resource_path + 'Removed_{}_title_selftext_NER.csv'.format(COLLECTION_NAME)
    # NER_post_quantity(path_to_read_data=path, category="Removed", n=50, name_entity=name_entity_quantity_removed,
    #                   resource_path=removed_resource_path)
    #
    # path = not_removed_resource_path + 'NotRemoved_{}_title_selftext_NER.csv'.format(COLLECTION_NAME)
    # NER_post_quantity(path_to_read_data=path, category="NotRemoved", n=50, name_entity=name_entity_quantity_not_removed,
    #                   resource_path=not_removed_resource_path)
    #
    # path = all_resource_path + 'All_{}_title_selftext_NER.csv'.format(COLLECTION_NAME)
    # NER_post_quantity(path_to_read_data=path, category="All", n=50, name_entity=name_entity_quantity_all,
    #                   resource_path=all_resource_path)
    #
    #
    # #  ####################### NER per emotion with ##############################
    #
    # emotions = ["Disgust", "Anger", "Fear", "Surprise", "Sadness", "Joy"]
    # removed_df = pd.read_csv(removed_resource_path + "{}_NER_emotion_rate_mean_{}_{}_NERS.csv".format("Removed", COLLECTION_NAME), 50)
    # not_removed_df = pd.read_csv(not_removed_resource_path + "{}_NER_emotion_rate_mean_{}_{}_NERS.csv".format("NotRemoved", COLLECTION_NAME), 50)
    # all_df = pd.read_csv(all_resource_path + "All_NER_emotion_rate_mean_wallstreetbets.csv")
    #
    # r_entities_set = set([entity[1] for index, entity in removed_df.iterrows()])
    # nr_entities_set = set([entity[1] for index, entity in not_removed_df.iterrows()])
    # all_entities_set = set([entity[1] for index, entity in all_df.iterrows()])
    #
    # entities_set = r_entities_set.intersection(nr_entities_set, all_entities_set)
    #
    # for entity in entities_set:
    #     emotion_detection.emotion_plot_per_NER(date_format='%Y/%m', subreddit_name=COLLECTION_NAME,
    #                                            NER=entity,
    #                                            path_to_save_plt=emotion_plot_folder_path,
    #                                            removed_df=removed_df.loc[removed_df['entity'] == entity],
    #                                            not_removed_df=not_removed_df.loc[not_removed_df['entity'] == entity],
    #                                            all_df=all_df.loc[all_df['entity'] == entity],
    #                                            emotions_list_category=emotions)
