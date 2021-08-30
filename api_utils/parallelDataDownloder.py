import concurrent.futures
import datetime
import multiprocessing
import asyncio
import time
import logging

import pymongo

from PushshiftApi import PushshiftApi
from reddit_api import reddit_api
from tqdm import tqdm


async def extract_reddit_data_parallel(sub):
    url = sub["permalink"]
    pushift.convert_time_format(sub)
    post_from_reddit = reddit.reddit.request('GET', url)
    final = {"reddit_api": post_from_reddit, "pushift_api": sub}
    return final


async def write_to_mongo(sub, pbar_):
    start_time = time.time()
    try:
        reddit_post = await extract_reddit_data_parallel(sub)
        end_time = time.time()
        elapsed_time_red_api = end_time - start_time
        end_time = time.time()
        mycol.insert_one(reddit_post)

    except pymongo.errors.DuplicateKeyError:
        print(reddit_post["pushift_api"]["id"] + " is already exist!")
        return
    end_total_time = time.time()
    elapsed_mongo_time = end_total_time - end_time
    elapsed_total_time = end_total_time - start_time
    logging.info("Extract from reddit time: {}. Insert to db time: {}. Total time: {}".format(elapsed_time_red_api,
                                                                                              elapsed_mongo_time,
                                                                                              elapsed_total_time))
    pbar_.update(1)


async def main(_submissions_list):
    with tqdm(total=len(_submissions_list)) as pbar:
        await asyncio.gather(*[write_to_mongo(submission, pbar) for submission in _submissions_list])


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(format='%(asctime)s %(message)s')
    # parameters
    for month in range(1,12):
        for day in range(1,30,2):
            limit = 80
            start_time = int(datetime.datetime(2020, month, day).timestamp())
            end_time = int(datetime.datetime(2020, month, day+1).timestamp())
            sub_reddit = 'politics'
            collection_name = sub_reddit
            last_index = 0
            ######
            myclient = pymongo.MongoClient("mongodb+srv://shimon:1234@redditdata.aav2q.mongodb.net/")
            mydb = myclient["reddit"]
            mycol = mydb[collection_name]
            pushift = PushshiftApi()
            reddit = reddit_api()
            start_run_time = time.time()
            submissions_list = pushift.get_submission(Subreddit=sub_reddit, start_time=start_time, end_time=end_time,
                                                      # filter=['url', 'author', 'title', 'subreddit', 'selftext', 'id',
                                                      #         'link_id', 'created_utc', 'retrieved_on', 'can_gild'],
                                                      Limit=limit)
            end_time = time.time()
            elapsed_time = end_time - start_run_time
            logging.info("Extract from pushift time: {}".format(elapsed_time))
            submissions_list = submissions_list[last_index:]  # if you want to recover, change last index
            asyncio.run(main(submissions_list))
