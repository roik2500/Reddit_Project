import concurrent.futures
import datetime
import multiprocessing

from PushshiftApi import PushshiftApi
from reddit_api import reddit_api
import pandas as pd
from tqdm import tqdm


def extract_reddit_data_parallel(sub):

    sub_id = sub["id"]
    pushift.convert_time_format(sub)
    post_from_reddit = reddit.reddit.submission(sub_id)
    post_from_reddit = [post_from_reddit.permalink,
                        post_from_reddit.id,
                        post_from_reddit.is_crosspostable,
                        post_from_reddit.removed_by_category,
                        post_from_reddit.is_robot_indexable,
                        post_from_reddit.link_flair_richtext,
                        post_from_reddit.selftext,
                        sub["url"],
                        sub["author"],
                        sub["title"],
                        sub["subreddit"],
                        sub["selftext"],
                        sub["created_utc"],
                        sub["retrieved_on"],
                        post_from_reddit.comments]
    return post_from_reddit

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


if __name__ == '__main__':

    with concurrent.futures.ThreadPoolExecutor() as executor:
        limit = 100000
        start_time = int(datetime.datetime(2020, 9, 1).timestamp())
        end_time = int(datetime.datetime(2020, 9, 15).timestamp())
        sub_reddit = 'politics'
        pushift = PushshiftApi()
        reddit = reddit_api()
        submissions_list = pushift.get_submission(Subreddit=sub_reddit, start_time=start_time,end_time=end_time,
                                                       filter=['url', 'author', 'title', 'subreddit', 'selftext', 'id',
                                                               'link_id', 'created_utc', 'retrieved_on', 'can_gild'],
                                                       Limit=limit, mod_removed_boolean=True,
                                                       user_removed_boolean=False)
        res = []
        files_counter = 1
        for submission in submissions_list:
            res.append((executor.submit(extract_reddit_data_parallel, submission)))
            if len(res) % limit == 5000:
                # writer.writerows(lst)
                final_lst = []
                for r in tqdm(res):
                    final_lst.append(r.result())
                df = pd.DataFrame(data=final_lst, columns=["permalink",
                                                           "id",
                                                           "is_crosspostable",
                                                           "removed_by_category",
                                                           "is_robot_indexable",
                                                           "link_flair_richtext",
                                                           "selftext",
                                                           'url',
                                                           'author',
                                                           'title',
                                                           'subreddit',
                                                           'selftext_pushift',
                                                           'created_utc',
                                                           'retrieved_on',
                                                           'comments'])
                df.to_json("../data/data{}_{}_{}.json".format(files_counter, sub_reddit, start_time), orient="index")
                files_counter += 1
                res = []