import datetime
import asyncio
import json
import pickle
import string
import sys
import time
import logging

import prawcore
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
global chunk_size
chunk_size = 850 * 1024
global total_post_len
total_post_len = 100000000
global data
data = {}
global counter
counter = 5
global tmp_id
infile = open('ids.pkl', 'rb')
tmp_id = pickle.load(infile)
infile.close()


async def insert_post(sub, pbar_):
    global data
    global counter
    global chunk_size
    global total_post_len
    logging.info(len(data))
    if sub['id'] in tmp_id:
        start_time = time.time()
        # try:
        reddit.pushshift.convert_time_format(sub)
        reddit_post = await reddit.extract_reddit_data_parallel(sub)
        post = {
            "post_id": sub["id"],
            "reddit_api": reddit_post,
            "pushift_api": sub
        }

        end_time = time.time()
        elapsed_time_first_insert = end_time - start_time
        end_time = time.time()
        # except pymongo.errors.DuplicateKeyError:
        #     print(reddit_post["pushift_api"]["id"] + " is already exist!")
        #     return
        end_reddit_time = time.time()
        # con_db.update_to_db(sub["id"], reddit_post)
        data[sub["id"]] = post
        pbar_.update(1)

        end_total_time = time.time()
        elapsed_reddit_time = end_reddit_time - end_time
        elapsed_second_insert_time = end_total_time - end_reddit_time
        logging.info("id: {}, Extract from reddit time: {}.".format(sub["id"], elapsed_time_first_insert))

        if sys.getsizeof(data) > chunk_size or len(data) >= total_post_len:
            start_time_dump = time.time()
            with open("json_wallstreetbets_2019_data/{}_{}_{}.json".format(collection_name, year, counter), 'w') as f:
                json.dump(data, f)
            data = {}
            counter += 1
            pbar_.reset()
            end_time_json = time.time()
            elapsed_time_json_write = end_time_json - start_time_dump
            logging.info("id: {}, 'write to json time: {}.".format(sub["id"], elapsed_time_json_write))


async def main(_submissions_list):
    with tqdm(total=len(_submissions_list)) as pbar:
        await asyncio.gather(*[insert_post(submission, pbar) for submission in _submissions_list])


async def fix_reddit_empty_posts():
    # counter = 0
    for character in range(4, 8):
        print(character)
        curs = mycol.find({'reddit_api': [], 'post_id': {'$regex': '^h{}'.format(character)}})
        print("here")
        pbar = tqdm(total=50000)
        await asyncio.gather(*[fix_one_post(post, pbar) for post in curs])
        # for post in tqdm(curs):
        #     await fix_one_post(post, pbar)


async def fix_one_post(post, pb):
    t1 = time.time()
    pid = post['post_id']
    push_post = post['pushift_api']
    try:
        red_post = await reddit.extract_reddit_data_parallel(push_post)
        red_t = time.time() - t1
        await con_db.update_to_db(Id=pid, reddit_post=red_post)
        upd_t = time.time() - red_t - t1
        logging.info(
            "id: {}, reddit time: {}. update time: {}".format(
                pid, red_t, upd_t))
        pb.update(1)
    except prawcore.exceptions.NotFound:
        logging.info('{} returned 404'.format(pid))
        return
    except prawcore.exceptions.ServerError:
        logging.info('{} returned 500'.format(pid))
        return
    except prawcore.exceptions.ResponseException:
        logging.info('{} returned 502'.format(pid))
        return


if __name__ == '__main__':
    data = {}
    counter = 0
    while True:
        logging.getLogger().setLevel(logging.INFO)
        logging.basicConfig(format='%(asctime)s %(message)s')
        # parameters
        con_db = Con_DB()
        year = 2019
        # for month in tqdm(range(12, 13, 1)):
        # for day in tqdm(calendar.monthrange(year, month)):
        # logging.info("month: {}".format(month))
        limit = total_post_len
        start_time = int(datetime.datetime(year, 1, 1).timestamp())
        # if month == 12:
        #     end_time = int(datetime.datetime(year+1, 1, 1).timestamp())
        # else:
        end_time = int(datetime.datetime(year, 1, 31).timestamp())

        sub_reddit = 'politics'
        collection_name = sub_reddit
        last_index = 0
        ######
        mycol = con_db.get_cursor_from_mongodb(collection_name=collection_name)
        pushift = PushshiftApi()
        reddit = reddit_api()
        start_run_time = time.time()
        submissions_list = []
        submissions_list = pushift.get_submission(Subreddit=sub_reddit, start_time=start_time, end_time=end_time,
                                                  Limit=limit)
        end_time = time.time()
        elapsed_time = end_time - start_run_time
        logging.info("Extract from pushift time: {}".format(elapsed_time))
        submissions_list = submissions_list[last_index:]  # if you want to recover, change last index
        try:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(main(submissions_list))
            # asyncio.run(main(submissions_list, data, counter))
            break
            if len(data) > 0:
                start_time_dump = time.time()
                with open("json_politics_2019_data/{}_{}_{}.json".format(collection_name, year, counter), 'w') as f:
                    json.dump(data, f)
                data = {}
                counter += 1
                pbar_.reset()
                end_time_json = time.time()
                elapsed_time_json_write = end_time_json - start_time_dump
                logging.info("id: {}, 'write to json time: {}.".format(sub["id"], elapsed_time_json_write))
        except pymongo.errors.CursorNotFound:
            continue

    # asyncio.run(main(submissions_list))