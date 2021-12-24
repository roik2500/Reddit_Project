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
import os
# import ijson
import ijson.backends.yajl2_c as ijson
import pandas as pd
import pymongo
from pymongo import WriteConcern
from pymongo.errors import BulkWriteError
from tqdm import tqdm
from pymongo import InsertOne


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
def counting_replies(replies_lst):
    cnt = 0
    for rep in replies_lst:
        cnt += 1
        if "replies" in rep["data"] and 'data' in rep["data"]["replies"]:
            cnt += counting_replies(rep["data"]["replies"]["data"]["children"])
    return cnt


def extract_top_level_comments(colc_curs, comments_collection):
    tmp_res = []
    for post in colc_curs:
        comments = [com for com in post["reddit_api"]["comments"]]
        tmp_res.extend(comments)
        if len(tmp_res) > 30000:
            print("writing")
            comments_collection.insert_many(tmp_res)
            tmp_res = []


def remove_fields(collection, filter_fields_path):
    fields = []
    with open(filter_fields_path) as f:
        line = f.readline()
        while line:
            fields.append(line.split('":')[0].strip(' "').strip("\n"))
            line = f.readline()
    collection.update_many({}, {"$unset": {f"data.{field}": "" for field in fields}})


def remove_dup(collection):
    cnt = 0
    curs = collection.find({}, {"data.id": 1}, no_cursor_timeout=True)
    index = set()
    for x in tqdm(curs):
        _id = x["data"]["id"]
        if _id in index:
            print("delete")
            collection.delete_one({"data.id": _id})
            cnt += 1
        else:
            index.add(_id)
    print(cnt)


def copy_docs(collection_from, collection_to):
    curs = collection_from.find({})
    tmp = []
    for post in tqdm(curs):
        tmp.append(post)
        if len(tmp) > 300000:
            collection_to.insert_many(tmp)
            print("writing")
            tmp = []
    if len(tmp) > 0:
        collection_to.insert_many(tmp)
        print("end writing")
        tmp = []


def comments_to_lists(comments, index=None):  # link_id = post_id
    results = []  # create list for results
    for comment in comments:  # iterate over comments
        if index is not None and comment["data"]["id"] in index:
            continue
        comment.pop('_id', None)
        item = [comment]
        if 'replies' in comment["data"] and len(comment['data']['replies']) > 0:
            replies = comment['data']['replies']['data']['children']
            item += comments_to_lists(replies)  # convert replies using the same function item["replies"] = k4lz6m

        results += item  # add converted item to results
    return results  # return all converted comments


def get_id_index(colc):
    index_cursor = colc.list_indexes()
    for index in index_cursor:
        print(index["name"])
        if index["name"] == "id_unique":
            print('fczcivp' in index.keys())
            print('fczcivp' in index)
            return index.keys()


def get_ijson_cursors_by_files_names(json_files_names):
    items_cursors = []
    for file_name in json_files_names:
        items = ijson.items(open(f"{file_name}.json", 'rb'), 'item')
        items_cursors.append(items)
    return items


def insert_json_to_mongo(json_files_names, target_colc):
    cursors_list = get_ijson_cursors_by_files_names(json_files_names)
    for curs in cursors_list:
        tmp = []
        for elm in curs:
            tmp.append(elm)
            if len(tmp) > 100000:
                target_colc.insert_many(tmp)
        if len(tmp) > 0:
            target_colc.insert_many(tmp)


def extract_nested_replies(collection_curs, index_flag=False):
    curs = collection_curs.find({"data.replies": {"$ne": ""}})
    replies_lst = []
    if index_flag:
        index = get_id_index(collection_curs)
    else:
        index = None
    counter = 0
    for x in tqdm(curs):
        if "replies" in x["data"].keys():
            comments = comments_to_lists(x["data"]["replies"]["data"]["children"], index)
            # print(counter, x["data"]["id"])
            replies_lst.extend(comments)
            counter += 1
        if counter >= 100000:
            print("writing!")
            collection_curs.insert_many(replies_lst)
            replies_lst = []
            counter = 0
    if counter > 0:
        print("last writing!")
        collection_curs.insert_many(replies_lst)


if __name__ == "__main__":
    client = pymongo.MongoClient(
        'mongodb://132.72.66.126:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl'
        '=false')
    db = client["reddit"]
    #   curs = collection.find({}, {"reddit_api.comments": 1})
    with open(r"G:\.shortcut-targets-by-id\1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d\final_project\data\MensRights_2020_.json", 'rb') as f:
        items = ijson.items(f, 'item')
        colc = db["MR_comments"]
        extract_top_level_comments(items, colc)
        extract_nested_replies(colc.with_options(write_concern=WriteConcern(w=0)), False)
    #    for collection_name in tqdm(db.list_collection_names()):
    #        if collection_name.__contains__("comments") and collection_name not in {"mensrights_comments","cryptocurrency_comments","wallstreetbets_comments"}:
    #            print(collection_name)
    #            colc = db[collection_name] #.with_options(write_concern=WriteConcern(w=0))
    #            remove_fields(colc, r""
    #                                r"filter_comments.txt")
    #    colc = db["WSB"]
    #    comments_colc = db["WSB_comments"]
    #    extract_top_level_comments(colc, comments_colc)

#   colc = db["WSB_comments"].with_options(write_concern=WriteConcern(w=0))
#   tmp_colc = db["tmp"]
#   tmp_colc.delete_many({})
# extract_nested_replies(colc, False)
# tmp_colc.delete_many({"data.id": "_"})
# copy_docs(tmp_colc, colc)
#   remove_dup(colc)
#   colc.create_index([("data.id", pymongo.DESCENDING)], unique=True)
#   curs = colc.find({})
#   copy_docs(colc, db["POL_comments"])

#
#   total_cnt = 0
#   total_res = []
#   for com in tqdm(curs):
#       total_cnt += 1
#       if "replies" in com["data"] and "data" in com["data"]["replies"]:
#           #total_cnt += counting_replies(com["data"]["replies"]["data"]["children"])
#           total_res += comments_to_lists(com["data"]["replies"]["data"]["children"])
#           if len(total_res) > 100000:
#                 tmp_colc.insert_many(total_res)
##                  pd.DataFrame(total_res).to_csv(f"wsb_comments_{total_cnt}.csv")
#                 print("writing")
#                 total_res = []
#   if len(total_res) > 0:
#      tmp_colc.insert_many(total_res)
##       pd.DataFrame(total_res).to_csv(f"wsb_comments_{total_cnt+1}.csv")
#      print("writing end")

# print(total_cnt)
# extract_nested_replies(colc)
# re        move_dup("cryptocurrency_comments")

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
