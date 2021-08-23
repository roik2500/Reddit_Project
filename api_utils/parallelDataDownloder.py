import concurrent.futures
import datetime
import multiprocessing

import PushshiftAPI
import reddit_api
import pandas as pd
from tqdm import tqdm


def extract_reddit_data_parallel(sub):

    sub_id = sub["id"]
    PushshiftAPI.convert_time_format(sub)
    post_from_reddit = reddit_api.reddit.submission(sub_id)
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
                        sub["retrieved_on"]]
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
        limit = 1000000
        start_time = int(datetime.datetime(2020, 1, 1).timestamp())
        submissions_list = PushshiftAPI.get_submission(Subreddit='politics', start_time=start_time,
                                                       filter=['url', 'author', 'title', 'subreddit', 'selftext', 'id',
                                                               'link_id', 'created_utc', 'retrieved_on', 'can_gild'],
                                                       Limit=limit, mod_removed_boolean=True,
                                                       user_removed_boolean=False)
        res = []
        for submission in submissions_list:
            res.append((executor.submit(extract_reddit_data_parallel, submission)))
            if len(res) % limit == 10000:
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
                                                           'retrieved_on'])
                df.to_csv("data_{}.csv".format(len(res)))
                res = []