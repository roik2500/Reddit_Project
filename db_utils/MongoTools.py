# # fields = []
# # with open(r"G:\.shortcut-targets-by-id\1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d\final_project\outputs\filter_fields.txt") as f:
# #     line = f.readline()
# #     while not line.__contains__("pushshift_api:"):
# #         line = f.readline()
# #         continue
# #     line = f.readline()
# #     while not line.__contains__("pushshift_api:"):
# #         fields.append(line.split('":')[0].strip(' "'))
# #         line = f.readline()
# #
import pymongo
from pymongo import WriteConcern
from pymongo.errors import BulkWriteError
from tqdm import tqdm
# # good_fields = ["can_mod_post",
# # "is_crosspostable",
# # "is_original_content",
# # "is_robot_indexable",
# # "locked",
# # "num_comments",
# # "num_crossposts",
# # "retrieved_on",
# # "selftext",
# # "title"]
# # bed_fields = [field for field in fields if field not in good_fields ]
# # bed_fields.remove('e')
# # bed_fields.remove('t')
# # bed_fields.remove('}],\n')
# #
#
client = pymongo.MongoClient('mongodb://132.72.66.126:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false')
db = client["reddit"]
#
# for collection_name in db.list_collection_names():
#     if collection_name == "mensrights":
#         continue
#     print(collection_name)
#     colc = db[collection_name]
#     collection_name_comments = "{}_comments".format(collection_name)
#     db.create_collection(collection_name_comments)
#     colc_comments = db[collection_name_comments]
#
#     curs = colc.find({}, {"reddit_api.comments": 1})
#     colc_comments.insert_many([y for x in tqdm(curs) for y in x["reddit_api"]["comments"]])
#     colc.update_many({}, {"$unset": {"reddit_api.comments": ""}})
#
colc = db["mensrights_comments"].with_options(write_concern=WriteConcern(w=0))
colc.index_information()
replies_lst = []
# curs = colc.find({"data.replies": {"$ne":""}}, {"data.replies.data.children":1})
# replies_lst = []
# for x in curs:
#     if "replies" in x["data"].keys():
#         replies_lst.append(x)
# while len(replies_lst) > 0:
#     children = [y for x in tqdm(replies_lst) for y in x["data"]["replies"]["data"]["children"]]
#     colc.insert_many(children)
#
#     curs = colc.find({"data.replies": {"$ne": ""}}, {"data.replies.data.children": 1})
#     replies_lst = []
#     for x in curs:
#         if "replies" in x["data"].keys():
#             replies_lst.append(x)