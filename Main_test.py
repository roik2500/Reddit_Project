import csv
import ijson
from aiohttp import streamer
from tqdm import tqdm
from statistics_analysis.Statistics import Statistic
from db_utils.Con_DB import Con_DB
from text_analysis.Sentiment import Sentiment
import os
import pandas as pd

# "mongodb+srv://roi:1234@redditdata.aav2q.mongodb.net/"
con = Con_DB()
posts = con.get_cursor_from_mongodb(collection_name="wallstreetbets")
# path = 'C:/Users/roik2/PycharmProjects/Reddit_Project/data/document_topic_table_general.csv'  # Change to your path in your computer
path_csv = 'C:/Users/roik2/PycharmProjects/Reddit_Project/data/document_topic_table_general_best .csv'  # Change to your path in your computer
path_drive = 'G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/'
path_csv_topic = 'G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs' \
                 '/topic_modeling/wallstreetbets/post/all/general/best/document_topic_table_general-best.csv '

pysentimiento_path = 'G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs' \
                     '/Sentiment Anylasis/pysentimiento'

# outputs
afin_sentiment = 'G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/Sentiment Anylasis/afin/afin_sentiment'
afin_statistic = 'G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/Sentiment Anylasis/afin/afin_statistic'
textblob_sentiment = 'G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/Sentiment Anylasis/textblob/textblob_sentiment'
textblob_statistic = 'G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/Sentiment Anylasis/textblob/textblob_statistic'

# define a objects for tests
subreddit = "wallstreetbets"
TypeOfPost = "All"  # All/removed/NotRemoved
api_all = Sentiment(subreddit, "All")
api_Removed = Sentiment(subreddit, "Removed")
api_Not_Removed = Sentiment(subreddit, "NotRemoved")

stat_all = Statistic(subreddit, "All")
stat_api_Removed = Statistic(subreddit, "Removed")
stat_Not_Removed = Statistic(subreddit, "NotRemoved")

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

'''
This funtion collectiong all the posts that including at least one word from the listOfWords per specific topic
'''


def sent_including_words_Pertopic(topic_id, listOfWords):
    # posts = con.get_cursor_from_mongodb(collection_name="wallstreetbets")
    topic_set = Sentiment("wallstreetbets", "All")
    topic_csv = con.read_fromCSV(path_csv_topic)
    numofpost = 0
    dff = topic_csv[topic_csv["Dominant_Topic"] == topic_id]
    for post_id in (dff['post_id']):
        post_connection = con.searchConnectionByPostId(post_id)
        post = post_connection.find({"post_id": post_id})
        if post.count() == 0: continue

        post_keys = post[0]['pushift_api'].keys()

        contain = False
        diaply_word = {'self_text': [], 'title': []}
        if 'selftext' in post_keys:
            for word in listOfWords:
                if word in post[0]['pushift_api']['selftext'].split(' '):
                    diaply_word['self_text'].append(word)
                    contain = True

        if 'title' in post_keys:
            for word in listOfWords:
                if word in post[0]['pushift_api']['title'].split(' '):
                    diaply_word['title'].append(word)
                    contain = True

        if contain:
            post_title = [post[0]['pushift_api']['selftext'], post[0]['pushift_api']['title']]
            print(" topic number: {} \n post id: {} ".format(topic_id, post_id))
            for sentence in post_title:
                sc = topic_set.get_post_sentiment_pysentimiento(sentence)
                print(sc)
            # sentiment_vader(post_title, topic_set)
            numofpost += 1
            print("num of posts: {}".format(numofpost))
            print(diaply_word)


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


'''
Test function
This function creating graphs for posts are Removed/NotRemove/All
'''


def creatSentiment_and_statistic_Graph():
    for post in tqdm(posts.find({})):
        # text = con.get_posts_text(posts,"title")
        api_all.update_sentiment(post, "month")
        stat_all.precentage_media(con, post)

        api_Removed.update_sentiment(post, "month")
        stat_api_Removed.precentage_media(con, post)

        api_Not_Removed.update_sentiment(post, "month")
        stat_Not_Removed.precentage_media(con, post)

    stat_all.get_percent()
    stat_api_Removed.get_percent()
    stat_Not_Removed.get_percent()

    api_all.draw_sentiment_time("textblob", "polarity")
    api_Removed.draw_sentiment_time("textblob", "polarity")
    api_Not_Removed.draw_sentiment_time("textblob", "polarity")

    api_all.draw_sentiment_time("textblob", "subjectivity")
    api_Removed.draw_sentiment_time("textblob", "subjectivity")


#
# # api_Not_Removed.draw_sentiment_time("subjectivity", "wallstreetbets", "NotRemoved")
# def update_sentiment_afin_textblob(posts,
#                                    afin_sent_all, textblob_sent_all, afin_stat_all, textblob_stat_all,
#                                    afin_sent_removed, textblob_sent_removed, afin_stat_removed,
#                                    textblob_stat_removed,
#                                    afin_sent_notremoved, textblob_sent_notremoved, afin_stat_notremoved,
#                                    textblob_stat_notremoved
#                                    ):
#     # afin = creat_new_folder_drive("afin", path_drive)
#     # textblob = creat_new_folder_drive("textblob", path_drive)
#
#     # # statistic path
#     # statistic_afin_path = creat_new_folder_drive("AllData", afin_statistic + '/')
#     # statistic_textblob_path = creat_new_folder_drive("AllData", textblob_statistic + '/')
#     #
#     # # sentiment  path
#     # sentiment_afin_path = creat_new_folder_drive("AllData", afin_sentiment + '/')
#     # sentiment_textblob_path = creat_new_folder_drive("AllData", textblob_sentiment + '/')
#     for post in tqdm(posts.find({})):
#         # All
#         afin_sent_all.update_sentiment(post, "month", "afin")
#         textblob_sent_all.update_sentiment(post, "month", "textblob")
#         afin_stat_all.precentage_media(con, post)
#         textblob_stat_all.precentage_media(con, post)
#
#         # Removed
#         afin_sent_removed.update_sentiment(post, "month", "afin")
#         textblob_sent_removed.update_sentiment(post, "month", "textblob")
#         afin_stat_removed.precentage_media(con, post)
#         textblob_stat_removed.precentage_media(con, post)
#
#         # NotRemoved
#         afin_sent_notremoved.update_sentiment(post, "month", "afin")
#         textblob_sent_notremoved.update_sentiment(post, "month", "textblob")
#         afin_stat_notremoved.precentage_media(con, post)
#         textblob_stat_notremoved.precentage_media(con, post)
#     # draw_sentiment_afin_textblob()
#
#
# def draw_sentiment_afin_textblob(sentiment_afin_path, sentiment_textblob_path, statistic_afin_path,
#                                  statistic_textblob_path):
#     # All
#     afin_sent_all.draw_sentiment_time("afin", fullpath=sentiment_afin_path)
#     textblob_sent_all.draw_sentiment_time("textblob", fullpath=sentiment_textblob_path)
#     afin_stat_all.draw_statistic_bars(afin_sent_all, path=statistic_afin_path)
#     textblob_stat_all.draw_statistic_bars(textblob_sent_all, path=statistic_textblob_path)
#
#     # Removed
#     afin_sent_removed.draw_sentiment_time("afin", fullpath=sentiment_afin_path)
#     textblob_sent_removed.draw_sentiment_time("textblob", fullpath=sentiment_textblob_path)
#     afin_stat_removed.draw_statistic_bars(afin_sent_removed, path=statistic_afin_path)
#     textblob_stat_removed.draw_statistic_bars(textblob_sent_removed, path=statistic_textblob_path)
#
#     # NotRemoved
#     afin_sent_notremoved.draw_sentiment_time("afin", fullpath=sentiment_afin_path)
#     textblob_sent_notremoved.draw_sentiment_time("textblob", fullpath=sentiment_textblob_path)
#     afin_stat_notremoved.draw_statistic_bars(afin_sent_notremoved, path=statistic_afin_path)
#     textblob_stat_notremoved.draw_statistic_bars(textblob_sent_notremoved, path=statistic_textblob_path)
#

'''
This function creating graphs for all the topics (graph per topic)
:argument path - path to csv file that contains the data of all topics
:return graphs -> graph per topic
'''


def creat_Sentiment_Graph_For_Topics(path, collection_name, type_of_post):
    posts = con.get_cursor_from_mongodb(collection_name=collection_name)
    # topic_set = Sentiment(collection_name, type_of_post)
    topic_csv = con.read_fromCSV(path_csv)
    for topic_id in tqdm(topic_csv["Dominant_Topic"].unique()):
        dff = topic_csv[topic_csv["Dominant_Topic"] == topic_id]
        topic_set = Sentiment(collection_name, type_of_post)
        for post_id in tqdm(dff['post_id']):
            post = posts.find({"post_id": post_id})
            if post.count() == 0:
                continue
            post = posts.find({"post_id": post_id})[0]
            topic_set.update_sentiment(post, "year")  # iteration per month or per year

        topic_set.draw_sentiment_time("pysentimiento", topic=topic_id, fullpath=path)

        # path_textblob_polarity = creat_new_folder_drive("textblob_polarity", path_drive + '/')
        # path_afin_polarity = creat_new_folder_drive("afin_polarity", path_drive + '/')
        # path_textblob_subjectivity = creat_new_folder_drive("textblob_subjectivity", path_drive + '/')

        # topic_set.draw_sentiment_time("textblob", "polarity", topic_id, path_textblob_polarity)
        # topic_set.draw_sentiment_time("textblob", "subjectivity", topic_id, path_textblob_subjectivity)
        # topic_set.draw_sentiment_time("afin", "polarity", topic_id, path_afin_polarity)

        # topic_set.draw_sentiment_time("polarity", "wallstreetbets", "All")
        # topic_set.draw_sentiment_time("subjectivity", "wallstreetbets", "All")


def update_sentiment_statistic_pysentimiento_path(json_file):
    collection_name = "wallstreetbets"
    # json_file = 'G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/data/wallstreetbets_2020_full_.json'
   # path_save_sentiment = "/storage/users/dt-reddit/wallstreetbets/sentiment_analysis/post/sentiment/"
    #path_save_statistic = "/storage/users/dt-reddit/wallstreetbets/sentiment_analysis/post/statistic/"
    path_save_sentiment = "/home/roikreme/"
    path_save_statistic = "/home/roikreme/"

    fj = open(json_file, 'rb')
    items = ijson.items(fj, 'item')

    # All
    sentiment_all = Sentiment(collection_name, "All")
    statistic_all = Statistic(collection_name, "All")
    # Removed
    sentiment_removed = Sentiment(collection_name, "Removed")
    statistic_removed = Statistic(collection_name, "Removed")
    # NotRemoved
    sentiment_notremoved = Sentiment(collection_name, "NotRemoved")
    statistic_notremoved = Statistic(collection_name, "NotRemoved")


    for post in tqdm(items):

        # All
        sentiment_all.update_sentiment(post, "date", "post")
        statistic_all.precentage_media(con, post)

        # Removed
        sentiment_removed.update_sentiment(post, "date", "post")
        statistic_removed.precentage_media(con, post)

        # NotRemoved
        sentiment_notremoved.update_sentiment(post, "date", "post")
        statistic_notremoved.precentage_media(con, post)


    # All
    sentiment_all.draw_sentiment_time('pysentimiento', fullpath=path_save_sentiment)
    statistic_all.draw_statistic_bars(sentiment_all, path_save_statistic)
    statistic_all.get_percent()

    # Removed
    sentiment_removed.draw_sentiment_time('pysentimiento', fullpath=path_save_sentiment)
    statistic_removed.draw_statistic_bars(sentiment_removed, path_save_statistic)
    statistic_removed.get_percent()

    # NotRemoved
    sentiment_notremoved.draw_sentiment_time('pysentimiento', fullpath=path_save_sentiment)
    statistic_notremoved.draw_statistic_bars(sentiment_notremoved, path_save_statistic)
    statistic_notremoved.get_percent()


def CreateCommentDict_And_Sentiment(json_file="", post_id=""):
    collection_name = "wallstreetbets"
    type = "All"
    # path_save = "/storage/users/dt-reddit/wallstreetbets/sentiment_analysis/comment"
    path_save = "G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/Sentiment Anylasis/pysentimiento/wallstreetbets 2020/comment"
    comments_post = {}
    fj = open(json_file, 'rb')
    items = ijson.items(fj, 'item')
    dataforcsv = []
    # if post_id:
    #     for o in items:
    #         if o['post_id'] == post_id:
    #             items = [o]
    #             break

    for post in tqdm(items):
        comment_sentiment = Sentiment(collection_name, type)
        comments = post['reddit_api']['comments']
        comment_dict = comments_to_dicts(comments)
        comments_post[post[
            'post_id']] = comment_dict  ## {'postid1:[{ "id": id1,"created":created1,"text": text1,"link_id": link_id1,"is_removed":is_removed1,"replies":[]},..]

        # After the collection of all comments of specific post, we are doing a sentiment analysis of this
        # post's comments ans also drawing a graph
        if comments_post[post['post_id']]:
            for item in comment_dict:  # item = {"id": eidu9,"created":'2021-01-01',"text": 'a b c d..","link_id": link_id,"is_removed": True}
                comment_sentiment.update_sentiment(item, "year", "comment")
                # saving the drawing - the name of each document is the created_utc of the posts
                # path = creat_new_folder_drive(item['created'], path_save + '/')
                # path = creat_new_folder_drive("post_id - "+post['post_id'], path + '/')
            dataforcsv.append([post['post_id'], post['pushift_api']['selftext'], comment_sentiment.percent_positive,
                               comment_sentiment.percent_negative, comment_sentiment.neutral,
                               comment_sentiment.positive, comment_sentiment.negative, comment_sentiment.neutral,
                               comment_sentiment.text_per_month])
        return dataforcsv



'''
Helper recursion function that creating a dict that contains the whole comments 
'''
def comments_to_dicts(comments,dict_or_list):
    results = []  # create list for results
    for comment in comments:  # iterate over comments
        p = comment['data']
        if 'created_utc' in comment['data']:
            created = con.convert_time_format(comment['data']['created_utc'])[0]

        id = comment['data']['id']
        is_removed = con.is_removed(comment, "comment", "Removed")
        text = ''

        if "body" in comment["data"] and not comment['data']['body'].__contains__("[removed]") and comment['data'][
            'body'] != "[deleted]":
            text = comment['data']['body']

        link_id = comment["data"]['link_id'][3:]

        item = {
            "id": id,
            "created": created,
            "text": text,
            "link_id": link_id,
            "is_removed": is_removed

        }  # create dict from comment

        # check if there is a sub_comments and also check the method of the recursion (dict by hierarchy or list)
        if len(comment['data']['replies']) > 0 and dict_or_list == 'dict':
            item["replies"] = comments_to_dicts(
                comment['data']['replies']['data']['children'], 'dict')  # convert replies using the same function
        elif len(comment['data']['replies']) > 0 and dict_or_list == 'list':
            comments_to_dicts(comment['data']['replies']['data']['children'], 'list')

        results.append(item)  # add converted item to results

    return results  # return all converted comments


###########################################################################################################################
if __name__ == '__main__':
    '''
     This function is creating a graph of 3 bars per month that indicate the amount of positive,netural
     and negetive posts.
     '''


    def draw_bars():
        creatSentiment_and_statistic_Graph()
        stat_all.draw_statistic_bars(api_all)


    def topic_4_account():
        path1 = "G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/topic_modeling/wallstreetbets/post/all"
        path2 = "G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/topic_modeling/wallstreetbets/post/removed"

        path_post_all = creat_new_folder_drive("sentiment for topics", path1 + '/')
        path_post_removed = creat_new_folder_drive("sentiment for topics", path2 + '/')

        creat_Sentiment_Graph_For_Topics(path_post_all, subreddit, "ALL")
        creat_Sentiment_Graph_For_Topics(path_post_removed, subreddit, "ALL")


    def Alldata():
        # statistic path
        statistic_afin_path = creat_new_folder_drive("AllData", afin_statistic + '/')
        statistic_textblob_path = creat_new_folder_drive("AllData", textblob_statistic + '/')

        # sentiment  path
        sentiment_afin_path = creat_new_folder_drive("AllData", afin_sentiment + '/')
        sentiment_textblob_path = creat_new_folder_drive("AllData", textblob_sentiment + '/')

        for i in range(1, 5):
            print("start AUTH_DB{}".format(i))
            con.setAUTH_DB(i)
            postss = con.get_cursor_from_mongodb(collection_name="wallstreetbets")
            update_sentiment_afin_textblob(postss,
                                           afin_sent_all, textblob_sent_all, afin_stat_all, textblob_stat_all,
                                           afin_sent_removed, textblob_sent_removed, afin_stat_removed,
                                           textblob_stat_removed,
                                           afin_sent_notremoved, textblob_sent_notremoved, afin_stat_notremoved,
                                           textblob_stat_notremoved
                                           )

        draw_sentiment_afin_textblob(sentiment_afin_path, sentiment_textblob_path, statistic_afin_path,
                                     statistic_textblob_path)
        draw_sentiment_afin_textblob(statistic_afin_path, statistic_textblob_path, sentiment_afin_path,
                                     sentiment_textblob_path)


    def add_csv(path_csv):
        csv_file = pd.read_csv(path_csv)
        loc = 0
        for post_id in tqdm(csv_file["post_id"].unique()):
            for i in range(1, 5):
                con.setAUTH_DB(i)
                posts = con.get_cursor_from_mongodb(collection_name="wallstreetbets")
                post = posts.find({"post_id": post_id})
                if post.count() == 0:
                    continue
                p = post[0]
                key = post[0]['pushift_api'].keys()
                if 'selftext' not in key: continue
                print("loc: " + loc)
                csv_file.loc[loc, 'self_text'] = post[0]['pushift_api']['selftext']
                csv_file.loc[loc, 'title'] = post[0]['pushift_api']['title']
                csv_file.to_csv(path_csv, index=False)
                break
            loc += 1


    def createCSVstatisticComments(topic_id):
        json_file = 'G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/data/wallstreetbets_2020_full_.json'
        #json_file = '/storage/users/dt-reddit/wallstreetbets_2020_full_.json'
        #path_save = "/storage/users/dt-reddit/wallstreetbets/sentiment_analysis/comment"
        #pathcsv = '/storage/users/dt-reddit/document_topic_table_general_best .csv'
        path_save = "G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/Sentiment Anylasis/pysentimiento/wallstreetbets 2020/comment"

        topic_csv = con.read_fromCSV(path_csv)
        dff = topic_csv[topic_csv["Dominant_Topic"] == topic_id]
        header = ['post_id', 'post self_text', '%Positive', '%Negative', '%Netural', 'Num of positive',
                  ' Num of negative',
                  'Num of neutral', 'textDict']
        df = pd.DataFrame(columns=header)

        for post_id in tqdm(dff):
            data = CreateCommentDict_And_Sentiment(json_file=json_file, post_id=post_id)

            ## create CSV file
            if data:
                df.append(data)

        df.to_csv(r'{}/statisticsComment.csv'.format(path_save),encoding='UTF-8')

# #createCSVstatisticComments(21)
# json_file = '/home/roikreme/wallstreetbets_expr_shimon.json'
# update_sentiment_statistic_pysentimiento_path(json_file)

collection_name = 'wallstreetbets'
#path_to_save = 'G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/Outputs from cpu/wallstreetbets/post/all/general/4'
path_to_save = '/home/roikreme'
#path_csv = 'G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/outputs/Outputs from cpu/wallstreetbets/post/all/general/4/document_topic_table_general-4_updated.csv'
path_csv = '/home/roikreme/document_topic_table_general-4_updated.csv'
con.fromCSVtoJSON(path_csv, path_to_save, collection_name=collection_name)

# type_of_post = "All"
# path = creat_new_folder_drive(type_of_post, pysentimiento_path + '/')
# creat_Sentiment_Graph_For_Topics(pysentimiento_path, subreddit, "All")
# update_sentiment_statistic_pysentimiento_path(pysentimiento_path, subreddit, "All")
# add_csv('C:/document_topic_table_general-best.csv')
# list_of_words = ["Robinhood","Robinhood".lower(),"Robinhood".upper(), "e-trade","e-trade".lower(),"e-trade".upper(), "options","options".lower(),"options".upper(),
# "Trading","Trading".lower(),"Trading".upper(), "E-Trade","E-Trade".lower(),"E-Trade".upper()]
# sent_including_words_Pertopic(3, list_of_words)
# Alldata()
# p = stat.get_percent()
# draw_bars()
# topic = creat_Sentiment_Graph_For_Topics(path_drive="")
# update_sentiment_afin_textblob()
# set = creatSentiment_and_statistic_Graph()
