import concurrent.futures
import datetime
import multiprocessing
import asyncio
import time
import logging

import pymongo

from PushshiftApi import PushshiftApi
from reddit_api import reddit_api
import pandas as pd
from tqdm import tqdm


async def extract_reddit_data_parallel(sub):
    url = sub["permalink"]
    # sub_id = sub["id"]
    pushift.convert_time_format(sub)
    # reddit.reddit.config.store_json_result = True
    post_from_reddit = reddit.reddit.request('GET', url)
    # post_from_reddit = reddit.reddit.submission(sub_id)

    # post_from_reddit = vars(post_from_reddit)
    final = {"reddit_api": post_from_reddit, "pushift_api": sub}
    # post_from_reddit = [post_from_reddit.permalink,
    #                     post_from_reddit.id,
    #                     post_from_reddit.is_crosspostable,
    #                     post_from_reddit.removed_by_category,
    #                     post_from_reddit.is_robot_indexable,
    #                     post_from_reddit.link_flair_richtext,
    #                     post_from_reddit.selftext,
    #                     sub["url"],
    #                     sub["author"],
    #                     sub["title"],
    #                     sub["subreddit"],
    #                     sub["selftext"],
    #                     sub["created_utc"],
    #                     sub["retrieved_on"],
    #                     post_from_reddit.comments]
    return final

    # final_lst.append(post_from_reddit)
    # cnt += 1
    # print(cnt)
    # if cnt % 200 == 0:
    #     # writer.writerows(lst)
    #     df = pd.DataFrame(data=final_lst, columns=["permalink",
    #                                          "id",
    #                                          "is_crosspostable",
    #                                          "removed_by_category",
    #                                          "is_robot_indexable",
    #                                          "link_flair_richtext",
    #                                          "selftext",
    #                                          'url',
    #                                          'author',
    #                                          'title',
    #                                          'subreddit',
    #                                                'selftext_pushift',
    #                                                'created_utc',
    #                                                'retrieved_on'])
    #     df.to_csv("data_{}.csv".format(cnt))
    #     final_lst = []


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
    mongo_tasks = []
    # for submission in tqdm(submissions_list):
    #     # res.append((executor.submit(extract_reddit_data_parallel, submission)))
    #     # res = executor.submit(extract_reddit_data_parallel, submission)
    #     # res.result()
    #     # write_to_mongo(submission)
    #     mongo_tasks.append(asyncio.create_task(write_to_mongo(submission)))
    # responses = [f for f in tqdm(asyncio.as_completed(mongo_tasks), total=len(mongo_tasks))]
    with tqdm(total=len(_submissions_list)) as pbar:
        await asyncio.gather(*[write_to_mongo(submission, pbar) for submission in _submissions_list])
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(mongo_tasks)
    # loop.close()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(format='%(asctime)s %(message)s')
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    limit = 10
    start_time = int(datetime.datetime(2020, 9, 1).timestamp())
    end_time = int(datetime.datetime(2020, 9, 15).timestamp())
    sub_reddit = 'politics'
    collection_name = sub_reddit
    last_index = 0
    myclient = pymongo.MongoClient("mongodb+srv://shimon:1234@redditdata.aav2q.mongodb.net/")
    mydb = myclient["reddit"]
    mycol = mydb[collection_name]
    pushift = PushshiftApi()
    reddit = reddit_api()
    start_run_time = time.time()
    # submissions_list = pushift.get_submission(Subreddit=sub_reddit, start_time=start_time, end_time=end_time,
    #                                           # filter=['url', 'author', 'title', 'subreddit', 'selftext', 'id',
    #                                           #         'link_id', 'created_utc', 'retrieved_on', 'can_gild'],
    #                                           Limit=limit)
    # end_time = time.time()
    # elapsed_time = end_time - start_run_time
    # logging.info("Extract from pushift time: {}".format(elapsed_time))
    # submissions_list = submissions_list[last_index:]  # if you want to recover, change last index
    asyncio.run(main(pushift.get_submission(Subreddit=sub_reddit, start_time=start_time, end_time=end_time,
                                              # filter=['url', 'author', 'title', 'subreddit', 'selftext', 'id',
                                              #         'link_id', 'created_utc', 'retrieved_on', 'can_gild'],
                                              Limit=limit)))
    # asyncio.run(main(submissions_list))
    # loop.run_until_complete(asyncio.wait(mongo_tasks))
    # if len(res) % 100 == 0:
    #     # writer.writerows(lst)
    #     final_lst = []
    #     for r in tqdm(res):
    #         final_lst.append(r.result())
    #     df = pd.DataFrame(data=final_lst)
    #     # , columns=["permalink",
    #     #                          "id",
    #     #                          "is_crosspostable",
    #     #                          "removed_by_category",
    #     #                          "is_robot_indexable",
    #     #                          "link_flair_richtext",
    #     #                          "selftext",
    #     #                          'url',
    #     #                          'author',
    #     #                          'title',
    #     #                          'subreddit',
    #     #                          'selftext_pushift',
    #     #                          'created_utc',
    #     #                          'retrieved_on',
    #     #                          'comments'])
    #     df.to_json("../data/data{}_{}_{}.json".format(files_counter, sub_reddit, start_time), orient="index")
    #     files_counter += 1
    #     res = []
