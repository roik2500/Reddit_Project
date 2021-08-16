import csv
import datetime

import PushshiftAPI
import roi
import reddit_api
import pandas as pd

if __name__ == '__main__':
    start_time = int(datetime.datetime(2019, 1, 1).timestamp())
    submissions = PushshiftAPI.get_submission(Subreddit='mexico', start_time=start_time,
                                              filter=['url', 'author', 'title', 'subreddit', 'selftext', 'id',
                                                      'link_id'],
                                              Limit=100000, mod_removed_boolean=True, user_removed_boolean=False)
    # path = r'C:\Users\shimon\PycharmProjects\Reddit_Project\sampleJsonPosts.json'
    # submissions = pd.read_json(path)
    counter = 0
    removed = 0
    deleted = 0
    is_crosspostable_counter, is_robot_indexable_cnt, removed_by_cat_counter = 0, 0, 0
    df = pd.DataFrame()
    lst = []
    post_from_reddit = reddit_api.reddit.submission("bxhu2z")
    post_from_reddit = vars(post_from_reddit)
    # with open("data.csv",'w', encoding="UTF8",newline="") as f:

    fieldnames = post_from_reddit.keys()
    # writer = csv.DictWriter(f,fieldnames=fieldnames)
    # writer.writeheader()
    for submission in submissions:
        id = submission["id"]
        print(counter)
        post_from_reddit = reddit_api.reddit.submission(id)
        post_from_reddit = [post_from_reddit.permalink, post_from_reddit.id, post_from_reddit.is_crosspostable,
                            post_from_reddit.removed_by_category, post_from_reddit.is_robot_indexable,
                            post_from_reddit.link_flair_richtext, post_from_reddit.selftext]
        lst.append(post_from_reddit)
        # writer.fieldnames = list(post_from_reddit.__dict__.keys())
        # if not post_from_reddit.is_crosspostable and not post_from_reddit.is_robot_indexable:
        #     removed += 1
        # if post_from_reddit.removed_by_category is None and post_from_reddit.selftext == '[deleted]':
        #     deleted += 1
        #
        # # if submission.link_flair_indexable_cnt == "False":
        # #     is_crosspostable_counter += 1
        counter += 1
        if counter % 5000 == 0:
            # writer.writerows(lst)
            df = pd.DataFrame(data=lst, columns=["permalink", "id", "is_crosspostable", "removed_by_category",
                                                 "is_robot_indexable", "link_flair_richtext", "selftext"])
            df.to_csv("data_{}.csv".format(counter))
    # with open("data.csv",'w', encoding="UTF8",newline="") as f:
    #
    #     fieldnames = df.columns
    #     writer = csv.DictWriter(f,fieldnames=fieldnames)
    #     writer.writeheader()
    #     writer.writerows(lst)
    # with open("data.csv",'w') as f:
    #     w = csv.writer(f)
    #     w.writerows(submissions)
    # print(removed)
    # print(deleted)
    # print(is_robot_indexable_cnt)
    # print(removed_by_cat_counter)
