import numpy as np
import calendar
from data_layer import DataLayer
import datetime
import asyncio
import json
import pickle
import string
import sys
import time
import logging
import praw
import os
from dotenv import load_dotenv
from requests.exceptions import ChunkedEncodingError

load_dotenv(".env2")
from pmaw import PushshiftAPI
import prawcore
import pymongo
# from PushshiftApi import PushshiftApi
# from reddit_api import reddit_api
from tqdm import tqdm
import calendar
global original_chunk_size
original_chunk_size = 10000
global chunk_size
chunk_size = original_chunk_size
global data
data = {}
global tmp_data
tmp_data = {}
global counter
counter = 5
global times_praw
times_praw = []
global times
times = []
global file_name
file_name = ''


def convert_time_format(comment_or_post):
    comment_or_post['created_utc'] = datetime.datetime.fromtimestamp(
        comment_or_post['created_utc']).isoformat().split(
        "T")

async def insert_post(sub, kind, _post_or_comment, pbar_):
    global tmp_data
    global counter
    global chunk_size
    global original_chunk_size
    global times
    global times_praw
    s_time = time.time()
    # try:
    if '_reddit' in sub.keys():
        del sub["_reddit"]
    if 'subreddit' in sub.keys():
        del sub["subreddit"]
    if 'author' in sub.keys():
        del sub["author"]
    if 'poll_data' in sub.keys():
        sub['poll_data'] = str(sub['poll_data'])
    convert_time_format(sub)
    post_id = sub["id"]
    if kind == "reddit_api":
        tmp_data[post_id] = {}
        tmp_data[post_id]["post_id"] = post_id
        sub = dict(sorted(sub.items(), key=lambda item: item[0]))
        tmp_data[post_id][kind] = sub
        e_time = time.time()
        times_praw.append(e_time - s_time)
    else:
        if post_id in tmp_data:
            for k in sub.copy():
                if k in tmp_data[post_id]["reddit_api"]:
                    if sub[k] == tmp_data[post_id]["reddit_api"][k]:
                        del sub[k]
        else:
            tmp_data[post_id] = {}
            tmp_data[post_id]["post_id"] = post_id
        tmp_data[post_id][kind] = sub
        e_time = time.time()
        times.append(e_time - s_time)
        if len(tmp_data[post_id]) == 3:
            data[post_id] = tmp_data[post_id].copy()
            del tmp_data[post_id]
            chunk_size -= 1
    pbar_.update(1)

    if chunk_size <= 0:
        chunk_size = original_chunk_size
        start_time_dump = time.time()
        await dump_data()
        counter += 1
        pbar_.reset()
        logging.info("\nwrite to json time: {}.".format(time.time() - start_time_dump))


async def dump_data():
    global data
    global file_name
    mycol.insert_many(data.values())
    data = {}


async def main(_submissions_list, _submissions_list_praw, _post_or_comment):
    with tqdm(total=max(len(_submissions_list), len(_submissions_list_praw))) as pbar:
        await asyncio.gather(
            *[insert_post(submission, 'reddit_api', _post_or_comment, pbar) for submission in _submissions_list_praw])
        await asyncio.gather(
            *[insert_post(submission, 'pushift_api', _post_or_comment, pbar) for submission in _submissions_list])


if __name__ == '__main__':
    data = {}
    counter = 0
    logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(format='%(asctime)s %(message)s')
    # parameters
    data_layer = DataLayer(os.getenv("AUTH_DB"))
    year = 2020
    sub_reddit = 'politics'
    # for month in tqdm(range(12, 13, 1)):
    # for day in tqdm(calendar.monthrange(year, month)):
    # logging.info("month: {}".format(month))
    step = 1
    for post_or_comment in ["comment", "post"]:
        collection_name = f"{sub_reddit}_{post_or_comment}"
        mycol = data_layer.get_collection(year, sub_reddit, post_or_comment)
        file_name = "{}_{}_{}.json".format(collection_name, year, post_or_comment)
        for m in range(12, 13, step):
            last_day_of_month = calendar.monthrange(year, m)[1]
            logging.info(last_day_of_month)
            first_day = 1
            if m == 12:
              first_day = last_day_of_month
            for d in range(first_day, last_day_of_month+1, step):
                logging.info(f"date:{d}/{m}/{year}")
                start_time = int(datetime.datetime(year, m, d, 0,0).timestamp())
                # if month == 12:
                #     end_time = int(datetime.datetime(year+1, 1, 1).timestamp())
                # else:
                end_time = int(
                    datetime.datetime(year, m, d, 23, 59).timestamp())
                # logging.info(f"m={m}")
                ######
                pushift = PushshiftAPI(num_workers=12)
                print(os.getenv('CLIENT_ID'))
                reddit = praw.Reddit(
                    client_id=os.getenv('CLIENT_ID'),
                    client_secret=os.getenv('CLIENT_SECRET'),
                    user_agent=os.getenv('USER_AGENT'),
                    username=os.getenv('USER_NAME'),
                    password=os.getenv('PASSWORD'),
                    check_for_async=False
                )
                logging.info(post_or_comment)
                times_praw = []
                times = []
                submissions_list = []
                submissions_list_praw = []
                start_run_time = time.time()
                if post_or_comment == "post":
                    try:
                        submissions_list = pushift.search_submissions(subreddit=sub_reddit,
                                                                      after=start_time,
                                                                      before=end_time, safe_exit=True)
                    except ChunkedEncodingError as e:
                        logging.warn(f"Error at {d}/{m}/{year}")
                    end_run_time = time.time()
                    logging.info("Extract from pushift time: {}".format(end_run_time - start_run_time))
                    pushift.praw = reddit
                    try:
                        submissions_list_praw = pushift.search_submissions(subreddit=sub_reddit,
                                                                           after=start_time,
                                                                           before=end_time)
                    except ChunkedEncodingError as e:
                        logging.warn(f"Error at {d}/{m}/{year}")

                    logging.info("Extract from reddit time: {}".format(time.time() - end_run_time))
                else:
                    try:
                        submissions_list = pushift.search_comments(subreddit=sub_reddit, after=start_time,
                                                                   before=end_time, safe_exit=True)
                        if len(submissions_list) == 0:
                            submissions_list = pushift.search_comments(subreddit=sub_reddit, after=start_time,
                                                                       before=end_time)
                    except ChunkedEncodingError as e:
                        logging.warn(f"Error at {d}/{m}/{year}")
                    end_run_time = time.time()
                    logging.info("Extract from pushift time: {}".format(end_run_time - start_run_time))
                    pushift.praw = reddit
                    try:
                        submissions_list_praw = pushift.search_comments(subreddit=sub_reddit,
                                                                        after=start_time,
                                                                        before=end_time)
                    except ChunkedEncodingError as e:
                        logging.warn(f"Error at {d}/{m}/{year}")
                    logging.info("Extract from reddit time: {}".format(time.time() - end_run_time))
                loop = asyncio.get_event_loop()
                loop.run_until_complete(main(submissions_list, submissions_list_praw, post_or_comment))
                # asyncio.run(main(submissions_list, data, counter))
                if len(tmp_data) > 0:
                    data.update(tmp_data)
                    tmp_data = {}
                logging.info(f"Mean time to handle reddit {post_or_comment} is: {np.mean(times_praw)}")
                logging.info(f"Mean time to handle pushift {post_or_comment} is: {np.mean(times)}")
        if len(data) > 0:
            s_time_dump = time.time()
            loop.run_until_complete((dump_data()))
            counter += 1
            logging.info("Last write to json time: {}.".format(time.time() - s_time_dump))

