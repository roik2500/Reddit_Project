from tqdm import tqdm

from db_utils.FileReader import FileReader
from statistics_analysis.Statistics import Statistic
from db_utils.Con_DB import Con_DB
from text_analysis.Sentiment import Sentiment
import csv
import os
import matplotlib.pyplot as plt
from matplotlib.dates import date2num
import datetime

if __name__ == '__main__':

    con = Con_DB()
    posts = con.get_cursor_from_mongodb(collection_name="wallstreetbets")
    # path = 'C:/Users/roik2/PycharmProjects/Reddit_Project/data/document_topic_table_general.csv'  # Change to your path in your computer
    path_csv = 'C:/Users/roik2/PycharmProjects/Reddit_Project/data/document_topic_table_general_best .csv'  # Change to your path in your computer
    path_drive = 'G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/'

    # define a objects for tests
    subreddit = "wallstreetbets"
    TypeOfPost = "All"  # All/removed/NotRemoved
    api_all = Sentiment(subreddit, "All")
    api_Removed = Sentiment(subreddit, "Removed")
    api_Not_Removed = Sentiment(subreddit, "NotRemoved")

    stat_all = Statistic(subreddit, "All")
    stat_api_Removed = Statistic(subreddit, "Removed")
    stat_Not_Removed = Statistic(subreddit, "NotRemoved")

    '''
    This function creating graphs for all the topics (graph per topic)
    :argument path - path to csv file that contains the data of all topics
    :return graphs -> graph per topic
    '''


    def creat_Sentiment_Graph_For_Topics(path_drive=""):
        full_path = path_drive + "sentiment for topics/"
        topic_set = Sentiment("wallstreetbets", "All")
        topic_csv = con.read_fromCSV(path_csv)
        for topic_id in tqdm(topic_csv["Dominant_Topic"].unique()):
            dff = topic_csv[topic_csv["Dominant_Topic"] == topic_id]
            for post_id in tqdm(dff['post_id']):
                post = posts.find({"post_id": post_id})
                if post.count() == 0: continue
                post = posts.find({"post_id": post_id})[0]
                topic_set.update_sentiment(post, "All", "month")  # iteration per month or per year

            # topic_set.draw_sentiment_time("polarity", "wallstreetbets", "All",topic_id,path_drive)
            # topic_set.draw_sentiment_time("subjectivity", "wallstreetbets", "All",topic_id,path_drive)

            # topic_set.draw_sentiment_time("polarity", "wallstreetbets", "All")
            # topic_set.draw_sentiment_time("subjectivity", "wallstreetbets", "All")


    '''
    Creating a new folder in Google drive. 
    :argument folderName - name of the new folder
    :return full path to the new folder
    '''


    def creat_new_folder_drive(folderName, oldpath):
        # newpath = r'{}'.format(path_drive)+'/{}'.format(folderName)
        path = os.path.join(oldpath, folderName)
        if not os.path.exists(path):
            os.mkdir(path)
        return path

        # path_folder = path_drive + folderName
        # os.mkdir(path_folder)
        # return path_folder


    '''
    Test function
    This function creating graphs for posts are Removed/NotRemove/All
    '''


    def creatSentiment_and_statistic_Graph():
        for post in tqdm(posts.find({})):
            # text = con.get_posts_text(posts,"title")
            api_all.update_sentiment(post, "month", "textblob")
            stat_all.precentage_media(con, post)

            api_Removed.update_sentiment(post, "month", "textblob")
            stat_api_Removed.precentage_media(con, post)

            api_Not_Removed.update_sentiment(post, "month", "textblob")
            stat_Not_Removed.precentage_media(con, post)

        stat_all.get_percent()
        stat_api_Removed.get_percent()
        stat_Not_Removed.get_percent()

        api_all.draw_sentiment_time("textblob", "polarity")
        api_Removed.draw_sentiment_time("textblob", "polarity")
        api_Not_Removed.draw_sentiment_time("textblob", "polarity")

        api_all.draw_sentiment_time("textblob", "subjectivity")
        api_Removed.draw_sentiment_time("textblob", "subjectivity")


    # api_Not_Removed.draw_sentiment_time("subjectivity", "wallstreetbets", "NotRemoved")

    def create_sentiment_afin_textblob():

        afin = creat_new_folder_drive("afin", path_drive)
        textblob = creat_new_folder_drive("textblob", path_drive)

        # statistic path
        statistic_afin_path = creat_new_folder_drive("afin_statistic", afin + '/')
        statistic_textblob_path = creat_new_folder_drive("textblob_statistic", textblob + '/')

        # sentiment  path
        sentiment_afin_path = creat_new_folder_drive("afin_sentiment", afin + '/')
        sentiment_textblob_path = creat_new_folder_drive("textblob_sentiment", textblob + '/')

        # All
        afin_sent_all = Sentiment("wallstreetbets", "All")
        textblob_sent_all = Sentiment("wallstreetbets", "All")
        afin_stat_all = Statistic("wallstreetbets", "All")
        textblob_stat_all = Statistic("wallstreetbets", "All")

        # Removed
        afin_sent_removed = Sentiment("wallstreetbets", "Removed")
        textblob_sent_removed = Sentiment("wallstreetbets", "Removed")
        afin_stat_removed = Statistic("wallstreetbets", "Removed")
        textblob_stat_removed = Statistic("wallstreetbets", "Removed")

        # NotRemoved
        afin_sent_notremoved = Sentiment("wallstreetbets", "NotRemoved")
        textblob_sent_notremoved = Sentiment("wallstreetbets", "NotRemoved")
        afin_stat_notremoved = Statistic("wallstreetbets", "NotRemoved")
        textblob_stat_notremoved = Statistic("wallstreetbets", "NotRemoved")

        #counter = 0
        for post in tqdm(posts.find({})):
            #if counter==10: break
            # All
            afin_sent_all.update_sentiment(post, "month", "afin")
            textblob_sent_all.update_sentiment(post, "month", "textblob")
            afin_stat_all.precentage_media(con, post)
            textblob_stat_all.precentage_media(con, post)

            # Removed
            afin_sent_removed.update_sentiment(post, "month", "afin")
            textblob_sent_removed.update_sentiment(post, "month", "textblob")
            afin_stat_removed.precentage_media(con, post)
            textblob_stat_removed.precentage_media(con, post)

            # NotRemoved
            afin_sent_notremoved.update_sentiment(post, "month", "afin")
            textblob_sent_notremoved.update_sentiment(post, "month", "textblob")
            afin_stat_notremoved.precentage_media(con, post)
            textblob_stat_notremoved.precentage_media(con, post)

            #counter+=1

        # All
        afin_sent_all.draw_sentiment_time("afin", fullpath=sentiment_afin_path)
        textblob_sent_all.draw_sentiment_time("textblob", fullpath=sentiment_textblob_path)
        afin_stat_all.draw_statistic_bars(afin_sent_all,path=statistic_afin_path)
        textblob_stat_all.draw_statistic_bars(textblob_sent_all,path=statistic_textblob_path)

        # Removed
        afin_sent_removed.draw_sentiment_time("afin", fullpath=sentiment_afin_path)
        textblob_sent_removed.draw_sentiment_time("textblob", fullpath=sentiment_textblob_path)
        afin_stat_removed.draw_statistic_bars(afin_sent_removed, path=statistic_afin_path)
        textblob_stat_removed.draw_statistic_bars(textblob_sent_removed, path=statistic_textblob_path)

        # NotRemoved
        afin_sent_notremoved.draw_sentiment_time("afin", fullpath=sentiment_afin_path)
        textblob_sent_notremoved.draw_sentiment_time("textblob", fullpath=sentiment_textblob_path)
        afin_stat_notremoved.draw_statistic_bars(afin_sent_notremoved, path=statistic_afin_path)
        textblob_stat_notremoved.draw_statistic_bars(textblob_sent_notremoved, path=statistic_textblob_path)


    '''
    This function is creating a graph of 3 bars per month that indicate the amount of positive,netural
    and negetive posts.
    '''



'''
:argument This method get path to data after that was go through name_entity_recognition.get_entites()
:argument n is the number of NER to return 
:argument  collection name is the subreddit name that save in mnongoDB

:returns emotion analysis for the n NERS and save it to csv file.
'''


def explore_data_with_NER_and_emotion(n, path_to_read_data, collection_name, path_to_save_plots, Con_DB, category):
    NER_emotion_df = None
    flag = True
    n_common_NER_title_selftext = name_entity.most_N_common_NER(N=n, path=path_to_read_data)
    NER_BY_Type = name_entity.get_NER_BY_Type(n_common_NER_title_selftext, 'ORG', 'PERSON', 'PRODUCT')
    for type_item in NER_BY_Type:
        for NER_item, posts_ids_list in NER_BY_Type[type_item].items():
            relevant_posts = Con_DB.get_specific_items_by_post_ids(ids_list=posts_ids_list[1])
            emotion_detection.extract_posts_emotion_rate(relevant_posts)
            emotion_detection.calculate_post_emotion_rate_mean()
            # emotion_detection.emotion_plot_for_posts_in_subreddit(date_format='%Y/%m', subreddit_name=collection_name
            #                                                       , NER=NER_item, path_to_save_plt=path_to_save_plots
            #    "emotion_rate_mean": [emotion_detection.emotion_posts_avg_of_subreddit.items()],                                                   , category=category)
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
    File = category + "_NER_emotion_rate_mean_wallstreetbets.csv"
    file_reader.write_to_csv(df_to_write=NER_emotion_df, path=folder_path,
                             file_name=File)


# def NER_post_quantity(category, path_to_read_data, n):
#
#     n_common_NER_title_selftext = name_entity.most_N_common_NER(N=n, path=path_to_read_data)
#     list_to_save = [(object[0], object[1][0], object[1][1][0]) for object in n_common_NER_title_selftext]
#
#     df = pd.DataFrame(data=list_to_save, columns=['NER', 'NER_TYPE', 'number_of_posts'])
#     file_reader.write_to_csv(path="C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\files\\",
#                              file_name=category + 'NER_quantity.csv', df_to_write=df)
#
#     NER_BY_Type = name_entity.get_NER_BY_Type(n_common_NER_title_selftext, 'ORG', 'PERSON', 'PRODUCT', 'FAC')
#     file_reader.write_dict_to_json(path="C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\files\\",
#                                    file_name=category + 'NER_quantity_byType', dict_to_write=NER_BY_Type)

if __name__ == '__main__':

    # con = Con_DB()
    # posts = con.get_cursor_from_mongodb(collection_name="wallstreetbets")
    # path = 'C:/Users/roik2/PycharmProjects/Reddit_Project/data/document_topic_table_general.csv'  # Change to your path in your computer
    # # p = stat.get_percent()
    # topic = creat_Sentiment_Graph_For_Topics(path)

    ##################### SHAI ####################

    name_entity = NameEntity()
    file_reader = FileReader()
    con_db = Con_DB()
    emotion_detection = EmotionDetection()
    # #
    folder_path = 'C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\files\\'
    plots_folder_path = 'C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\plots\\'
    removed_plots_folder_path = plots_folder_path + "Removed\\"
    not_removed_plots_folder_path = plots_folder_path + "NotRemoved\\"
    all_plots_folder_path = plots_folder_path + "All\\"

    print("start Removed")
    posts_list = con_db.get_data_categories(category="Removed", collection_name="wallstreetbets")

    print("1")

    name_entity.extract_NER_from_data(posts=posts_list,
                                      file_name_to_save='Removed_wallstreetbets_title_selftext_NER.csv',
                                      path_to_folder=folder_path)
    print("2")
    path = folder_path + 'Removed_wallstreetbets_title_selftext_NER.csv'
    explore_data_with_NER_and_emotion(n=50, collection_name="wallstreetbets", path_to_read_data=path,
                                      path_to_save_plots=removed_plots_folder_path, Con_DB=con_db
                                      , category="Removed")


    print("start Not Removed")
    posts_list = con_db.get_data_categories(category="NotRemoved", collection_name="wallstreetbets")

    print("1")
    name_entity.extract_NER_from_data(posts=posts_list,
                                      file_name_to_save='NotRemoved_wallstreetbets_title_selftext_NER.csv',
                                      path_to_folder=folder_path)

    print("2")
    path = folder_path + 'NotRemoved_wallstreetbets_title_selftext_NER.csv'
    explore_data_with_NER_and_emotion(n=50, collection_name="wallstreetbets", path_to_read_data=path,
                                      path_to_save_plots=not_removed_plots_folder_path, Con_DB=con_db
                                      , category="NotRemoved")

    print("start All")
    posts_list = con_db.get_data_categories(category="All", collection_name="wallstreetbets")

    print("1")
    name_entity.extract_NER_from_data(posts=posts_list,
                                      file_name_to_save='All_wallstreetbets_title_selftext_NER.csv',
                                      path_to_folder=folder_path)

    print("2")
    path = folder_path + 'All_wallstreetbets_title_selftext_NER.csv'
    explore_data_with_NER_and_emotion(n=50, collection_name="wallstreetbets", path_to_read_data=path,
                                      path_to_save_plots=all_plots_folder_path, Con_DB=con_db
                                      , category="All")


    ########################## NER QUANTITY ###############################

    # path = folder_path + 'Removed_wallstreetbets_title_selftext_NER.csv'
    # NER_post_quantity(path_to_read_data=path, category="Removed", n=50)
    #
    # path = folder_path + 'NotRemoved_wallstreetbets_title_selftext_NER.csv'
    # NER_post_quantity(path_to_read_data=path, category="NotRemoved", n=50)
    #
    # path = folder_path + 'All_wallstreetbets_title_selftext_NER.csv'
    # NER_post_quantity(path_to_read_data=path, category="All", n=50)
    #
    # print("start Removed")
    # posts_list = con_db.get_data_categories(category="Removed", collection_name="wallstreetbets")
    # print("1")
    # name_entity.extract_NER_from_data(posts=posts_list,
    #                                   file_name_to_save='Removed_wallstreetbets_title_selftext_NER.csv',
    #                                   path_to_folder=folder_path)

    # print("start Not Removed")
    # posts_list = con_db.get_data_categories(category="NotRemoved", collection_name="wallstreetbets")
    # print("1")
    # name_entity.extract_NER_from_data(posts_list, 'NotRemoved_wallstreetbets_title_selftext_NER.csv', folder_path)
    #
    # print("start All")
    # posts_list = con_db.get_data_categories(category="All", collection_name="wallstreetbets")
    # print("1")
    # name_entity.extract_NER_from_data(posts_list, 'All_wallstreetbets_title_selftext_NER.csv', folder_path)


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

    def draw_bars():
        creatSentiment_and_statistic_Graph()
        stat_all.draw_statistic_bars(api_all)

# p = stat.get_percent()
# draw_bars()
# topic = creat_Sentiment_Graph_For_Topics(path_drive="")

create_sentiment_afin_textblob()
# set = creatSentiment_and_statistic_Graph()
