import ijson
import numpy as np
import pandas as pd
from datasets import tqdm

from Con_DB import Con_DB

file_name = "C:/Users/shimon/Downloads/document_topic_table_general-best.csv"
src_name = 'wallstreetbets'
con_db = Con_DB()
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

# for i, row in tqdm(topics_doc.iterrows()):
with open(
        "G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/data/wallstreetbets_2020_full_.json",
        'rb') as fh:
        items = ijson.items(fh, 'item')
        for post in tqdm(items):
            row = topics_doc.loc[topics_doc["post_id"] == post["post_id"]]
            ps = post["pushift_api"]
            rd = post["reddit_api"]
            row["title"] = ps["title"]
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

            if "selftext" in rd:
                if rd["selftext"].__contains__("removed") or rd["selftext"].__contains__("deleted"):
                    row["selftext_reddit"] = rd["selftext"]

            if "banned_by" in rd:
                row["banned_by"] = rd["banned_by"]

            topics_doc.loc[topics_doc["post_id"] == post["post_id"]] = row


topics_doc.to_csv("{}_updated".format(file_name))

# import numpy as np
# import pandas as pd
# from datasets import tqdm
#
# from Con_DB import Con_DB
#
# file_name = "C:/Users/shimon/Downloads/document_topic_table_general-best.csv"
# src_name = 'wallstreetbets'
# con_db = Con_DB()
# topics_doc = pd.read_csv(file_name)
# topics_doc["selftext"] = np.zeros(len(topics_doc))
# topics_doc["title"] = np.zeros(len(topics_doc))
# topics_doc["removed_by_category"] = np.zeros(len(topics_doc))
# topics_doc["date"] = np.zeros(len(topics_doc))
# topics_doc["time"] = np.zeros(len(topics_doc))
# topics_doc["richtext"] = np.zeros(len(topics_doc))
# topics_doc["numOfComments"] = np.zeros(len(topics_doc))
#
# for i, row in tqdm(topics_doc.iterrows()):
#     for k in range(1, 5):
#         con_db.setAUTH_DB(k)
#         data_cursor = con_db.get_cursor_from_mongodb(collection_name=src_name).find({"post_id": row["post_id"]})
#         for post in data_cursor:
#             ps = post["pushift_api"]
#             row["title"] = ps["title"]
#             row["date"] = ps["created_utc"][0]
#             row["time"] = ps["created_utc"][1]
#             row["numOfComments"] = len(post["reddit_api"]["comments"])
#             row["richtext"] = ps["author_flair_richtext"]
#
#             if "selftext" in ps:
#                 row["selftext"] = ps
#
#             if "removed_by_category" in ps:
#                 row["removed_by_category"] = ps["removed_by_category"]
# topics_doc.to_csv("{}_updated".format(file_name))
