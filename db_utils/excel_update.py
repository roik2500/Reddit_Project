import ijson
import numpy as np
import pandas as pd
from datasets import tqdm
import os
import re
import datetime

def extract_info(f_name, root, flag):
    file_name = '{}/{}'.format(root, f_name)
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
      print(file_name)
      with open("wallstreetbets_2020_full_.json", 'rb') as fh:
              items = ijson.items(fh, 'item')
              for post in tqdm(items):
                  print(post["post_id"])
                  row = topics_doc.loc[topics_doc["post_id"] == post["post_id"]]
                  ps = post["pushift_api"]
                  rd = post["reddit_api"]
                  row["title"] = ps["title"]
                  print(type( ps["created_utc"]))
                  if type( ps["created_utc"]) != list:
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
      return topics_doc[["post_id", "selftext", "title", "removed_by_category", "date", "time", "richtext", "numOfComments", "banned_by", "author", "author_fullname", "selftext_reddit"]]
    else:
      master_csv = pd.read_csv("{}_updated".format(file_name))
      return master_csv[["post_id", "selftext", "title", "removed_by_category", "date", "time", "richtext", "numOfComments", "banned_by", "author", "author_fullname", "selftext_reddit"]]

regex_lda = re.compile('(document_topic_table*)')

#file_name = "C:/Users/shimon/Downloads/document_topic_table_general-best.csv"
src_name = 'wallstreetbets'

master_csv = extract_info("document_topic_table_general-0.csv", "/home/shouei/wallstreetbets/post/all/general/0/", True)
# for i, row in tqdm(topics_doc.iterrows()):
for root, dirs, files in os.walk("/home/shouei/wallstreetbets/post/all"):
    file_name = '?'
    for file in files:
      if regex_lda.match(file):
        print(file)
        if not file.__contains__("updated"):
          file_name = '{}/{}'.format(root, file)
    if file_name != '?':
      topics_doc = pd.read_csv(file_name)
      df3 = topics_doc.merge(master_csv, on="post_id", how='inner')
      df3.to_csv("{}_updated.csv".format(file_name.split('.')[0]))