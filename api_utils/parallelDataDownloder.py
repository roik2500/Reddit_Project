import datetime
import asyncio
import time
import logging
import pymongo
from api_utils.PushshiftApi import PushshiftApi
from api_utils.reddit_api import reddit_api
from tqdm import tqdm

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
        reddit_post = await reddit.extract_reddit_data_parallel(sub)
        end_time = time.time()
        elapsed_time_red_api = end_time - start_time
        end_time = time.time()
        con_db.insert_to_db(reddit_post)

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
    con_db = Con_DB()
    for month in tqdm(range(3, 7)):
        for day in tqdm(range(1, 28, 2)):
            logging.info("month: {}, day {}:".format(month, day))
            limit = 220
            start_time = int(datetime.datetime(2020, month, day).timestamp())
            end_time = int(datetime.datetime(2020, month, day + 1).timestamp())
            sub_reddit = 'wallstreetbets'
            collection_name = sub_reddit
            last_index = 0
            ######
            mycol = con_db.get_cursor_from_mongodb(collection_name=collection_name)
            pushift = PushshiftApi()
            reddit = reddit_api()
            start_run_time = time.time()
            submissions_list = pushift.get_submission(Subreddit=sub_reddit, start_time=start_time, end_time=end_time, Limit=limit)
            end_time = time.time()
            elapsed_time = end_time - start_run_time
            logging.info("Extract from pushift time: {}".format(elapsed_time))
            submissions_list = submissions_list[last_index:]  # if you want to recover, change last index
            asyncio.run(main(submissions_list))