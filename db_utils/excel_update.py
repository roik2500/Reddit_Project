import ijson
import numpy as np
import pandas as pd
from datasets import tqdm
import os
import re
import datetime
from Con_DB import Con_DB

db_utills = Con_DB()
curs = db_utills.get_cursor_from_mongodb(collection_name="politics")


def concat_csv_from_mongo(path_from_mongo, path_from_running):
    mongo_file = pd.read_csv(path_from_mongo)
    output_file = pd.read_csv(path_from_running)
    df3 = output_file.merge(mongo_file, on="post_id", how='inner')
    df3.to_csv("{}_updated.csv".format(path_from_running.split('.csv')[0]))


def extract_info(f_name, root, flag):
    file_name = '{}{}'.format(root, f_name)
    if flag:
        topics_doc = pd.read_csv(file_name)
        topics_doc["selftext"] = np.zeros(len(topics_doc))
        topics_doc["title"] = np.zeros(len(topics_doc))
        topics_doc["removed_by_category"] = np.zeros(len(topics_doc))
        topics_doc["date"] = np.zeros(len(topics_doc))
        topics_doc["time"] = np.zeros(len(topics_doc))
        topics_doc["richtext"] = np.zeros(len(topics_doc))
        topics_doc["numOfComments"] = np.zeros(len(topics_doc))
        topics_doc["banned_by"] = np.zeros(len(topics_doc))
        topics_doc["author"] = np.zeros(len(topics_doc))
        topics_doc["author_fullname"] = np.zeros(len(topics_doc))
        topics_doc["selftext_reddit"] = np.zeros(len(topics_doc))
        # print(file_name)
        # with open("wallstreetbets_2020_full_.json", 'rb') as fh:
        #     items = ijson.items(fh, 'item')
        #     for post in tqdm(items):
        for post in tqdm(curs.find({})):
            # print(post["post_id"])
            row = topics_doc.loc[topics_doc["post_id"] == post["post_id"]]
            ps = post["pushift_api"]
            rd = post["reddit_api"]
            row["title"] = ps["title"]
            # print(type(ps["created_utc"]))
            if type(ps["created_utc"]) != list:
                ps["created_utc"] = datetime.datetime.fromtimestamp(int(ps["created_utc"])).isoformat().split("T")
            row["date"] = ps["created_utc"][0]
            row["time"] = ps["created_utc"][1]
            row["numOfComments"] = len(post["reddit_api"]["comments"])
            if "author_flair_richtext" in ps and len(ps["author_flair_richtext"]) > 0:
                row["richtext"] = ps["author_flair_richtext"].__str__()

            if "author" in ps:
                row["author"] = ps["author"]

            if "author_fullname" in ps:
                row["author_fullname"] = ps["author_fullname"]

            if "selftext" in ps:
                row["selftext"] = ps["selftext"]

            if "removed_by_category" in ps:
                row["removed_by_category"] = ps["removed_by_category"]

            if "selftext" in rd["post"]:
                if rd["post"]["selftext"].__contains__("removed") or rd["post"]["selftext"].__contains__("deleted"):
                    row["selftext_reddit"] = rd["post"]["selftext"]

            if "banned_by" in rd["post"]:
                row["banned_by"] = rd["post"]["banned_by"]
            topics_doc.loc[topics_doc["post_id"] == post["post_id"]] = row

        topics_doc.to_csv("{}_updated".format(file_name))
        return topics_doc[
            ["post_id", "selftext", "title", "removed_by_category", "date", "time", "richtext", "numOfComments",
             "banned_by", "author", "author_fullname", "selftext_reddit"]]
    else:
        mstr_csv = pd.read_csv("{}_updated".format(file_name))
        return mstr_csv[
            ["post_id", "selftext", "title", "removed_by_category", "date", "time", "richtext", "numOfComments",
             "banned_by", "author", "author_fullname", "selftext_reddit"]]


regex_csv = re.compile('(document_topic_table*)')
regex_lda = re.compile('(^model.*gensim$)')

# file_name = "C:/Users/shimon/Downloads/document_topic_table_general-best.csv"
# src_name = 'politics'
# rng = range(2, 13)
# for i in rng:
#     for root, dirs, files in tqdm(
#             os.walk(r"G:\.shortcut-targets-by-id\1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d\final_project\outputs\Outputs from cpu\politics_2020_full_\post\all\{}".format(i))):
#         for direc in dirs:
#             number = -1
#
#             for r, d, f in tqdm(os.walk(
#                     r"G:\.shortcut-targets-by-id\1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d\final_project\outputs\Outputs from cpu\politics_2020_full_\post\all\{}\{}".format(i,
#                         direc))):
#                 for file in tqdm(f):
#                     if regex_lda.match(file):
#                         number = int(file.split('model_')[1].split('topic')[0])
#                         break
#
#                 counter = 0
#                 for file in tqdm(f):
#                     if regex_csv.match(file):
#                         # concat_csv_from_mongo(r"G:\.shortcut-targets-by-id\1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d\final_project\data"
#                         #                       r"\wallstreetbets.csv", root+"\\"+file)
#                         if counter == 1:
#                             file_split = file.split('{}'.format(i))
#                             # os.rename(r + "\\" + file, r + "\\" + file_split[0] + '{}_'.format(i) + str(number) + "_updated.csv")
#                         else:
#                             file_split = file.split('{}'.format(i))
#                             os.rename(r + "\\" + file, r + "\\" + file_split[0] + '{}_'.format(i) + str(number) + ".csv")
#                             counter += 1
#             if number > -1:
#                 os.rename(root + "\\" + direc, root + "\\" + str(number))

src_name = ''
for i in range(13,-1, -1):
    for root, dirs, files in tqdm(
            os.walk(r"G:\.shortcut-targets-by-id\1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d\final_project\outputs\Outputs from cpu\politics_2020_full_\post\all\{}".format(i))):
        for file in files:
            if regex_csv.match(file):
                concat_csv_from_mongo(r"G:\.shortcut-targets-by-id\1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d\final_project\data"
                                      r"\politicss.csv", root+"\\"+file)


# master_csv = extract_info("document_topic_table_general-20.csv", "", True)
# for i, row in tqdm(topics_doc.iterrows()):
# for root, dirs, files in os.walk("/home/shouei/wallstreetbets/post/all"):
#     file_name = '?'
#     for file in files:
#         if regex_lda.match(file):
#             print(file)
#             if not file.__contains__("updated"):
#                 file_name = '{}/{}'.format(root, file)
#     if file_name != '?':
#         topics_doc = pd.read_csv(file_name)
#         df3 = topics_doc.merge(master_csv, on="post_id", how='inner')
#         df3.to_csv("{}_updated.csv".format(file_name.split('.')[0]))
