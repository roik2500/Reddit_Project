import datetime
import asyncio
import time
import logging
import pymongo
from api_utils.PushshiftApi import PushshiftApi
from api_utils.reddit_api import reddit_api
from tqdm import tqdm
import calendar
from db_utils.Con_DB import Con_DB


# async def extract_reddit_data_parallel(sub):
#     url = sub["permalink"]
#     pushift.convert_time_format(sub)
#     post_from_reddit = reddit.reddit.request('GET', url)
#     reddit.convert_time_format(post_from_reddit[0]['data']['children'][0]['data'])
#     final = {"reddit_api": post_from_reddit, "pushift_api": sub}
#     return final


async def write_to_mongo(sub, pbar_):
    start_time = time.time()
    try:
        pbar_.update(1)
        reddit.pushshift.convert_time_format(sub)
        reddit_post = {
            "post_id": sub["id"],
            "reddit_api": [], "pushift_api": sub
        }

        con_db.insert_to_db(reddit_post)
        end_time = time.time()
        elapsed_time_first_insert = end_time - start_time
        end_time = time.time()
    except pymongo.errors.DuplicateKeyError:
        print(reddit_post["pushift_api"]["id"] + " is already exist!")
        return
    reddit_post = await reddit.extract_reddit_data_parallel(sub)
    end_reddit_time = time.time()
    con_db.update_to_db(sub["id"], reddit_post)

    end_total_time = time.time()
    elapsed_reddit_time = end_reddit_time - end_time
    elapsed_second_insert_time = end_total_time - end_reddit_time
    logging.info(
        "id: {}, Extract from reddit time: {}. Insert to db first time: {}. Insert to db second time: {}".format(
            sub["id"], elapsed_reddit_time,
            elapsed_time_first_insert,
            elapsed_second_insert_time))


#
# async def write_to_mongo(sub, pbar_):
#     start_time = time.time()
#     try:
#         reddit_post = await reddit.extract_reddit_data_parallel(sub)
#         end_time = time.time()
#         elapsed_time_red_api = end_time - start_time
#         end_time = time.time()
#         con_db.insert_to_db(reddit_post)
#         pbar_.update(1)
#     except pymongo.errors.DuplicateKeyError:
#         print(reddit_post["pushift_api"]["id"] + " is already exist!")
#         return
#     end_total_time = time.time()
#     elapsed_mongo_time = end_total_time - end_time
#     elapsed_total_time = end_total_time - start_time
#     logging.info("Extract from reddit time: {}. Insert to db time: {}. Total time: {}".format(elapsed_time_red_api,
#                                                                                               elapsed_mongo_time,
#                                                                                               elapsed_total_time))
#


async def main(_submissions_list):
    with tqdm(total=len(_submissions_list)) as pbar:
        await asyncio.gather(*[write_to_mongo(submission, pbar) for submission in _submissions_list])


async def fix_reddit_empty_posts():
    counter = 0
    curs = mycol.find({'reddit_api': []})
    for post in tqdm(curs):
        if counter % 2 == 0:
            t1 = time.time()
            pid = post['post_id']
            push_post = post['pushift_api']
            red_post = await reddit.extract_reddit_data_parallel(push_post)
            red_t = time.time() - t1
            await con_db.update_to_db(Id=pid, reddit_post=red_post)
            upd_t = time.time() - red_t - t1
            logging.info(
                "id: {}, reddit time: {}. update time: {}".format(
                    pid, red_t, upd_t))
        counter += 1


if __name__ == '__main__':
    while True:
        logging.getLogger().setLevel(logging.INFO)
        logging.basicConfig(format='%(asctime)s %(message)s')
        # parameters
        con_db = Con_DB()
        year = 2020
        # for month in tqdm(range(12, 13, 1)):
        # for day in tqdm(calendar.monthrange(year, month)):
        # logging.info("month: {}".format(month))
        limit = 10000000
        start_time = int(datetime.datetime(year, 1, 1).timestamp())
        # if month == 12:
        #     end_time = int(datetime.datetime(year+1, 1, 1).timestamp())
        # else:
        end_time = int(datetime.datetime(year, 7, 1).timestamp())

        sub_reddit = 'politics'
        collection_name = sub_reddit
        last_index = 0
        ######
        mycol = con_db.get_cursor_from_mongodb(collection_name=collection_name)
        pushift = PushshiftApi()
        reddit = reddit_api()
        start_run_time = time.time()
        submissions_list = []
        try:
            asyncio.run(fix_reddit_empty_posts())
            break
        except pymongo.errors.CursorNotFound:
            continue

    # submissions_list = pushift.get_submission(Subreddit=sub_reddit, start_time=start_time, end_time=end_time, Limit=limit)
    # end_time = time.time()
    # elapsed_time = end_time - start_run_time
    # logging.info("Extract from pushift time: {}".format(elapsed_time))
    # submissions_list = submissions_list[last_index:]  # if you want to recover, change last index
    # asyncio.run(main(submissions_list))
