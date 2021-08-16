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
                                              Limit=200, mod_removed_boolean=True, user_removed_boolean=False)
    # path = r'C:\Users\shimon\PycharmProjects\Reddit_Project\sampleJsonPosts.json'
    # submissions = pd.read_json(path)
    counter = 0
    removed = 0
    deleted = 0
    is_crosspostable_counter, is_robot_indexable_cnt, removed_by_cat_counter = 0, 0, 0
    for submission in submissions:
        id = submission["id"]
        print(counter)
        post_from_reddit = reddit_api.reddit.submission(id)
        if not post_from_reddit.is_crosspostable and post_from_reddit.selftext == '[removed]' and not post_from_reddit.is_robot_indexable:
            removed += 1
        if post_from_reddit.removed_by_category is None and post_from_reddit.selftext == '[deleted]':
            deleted += 1

        # if submission.link_flair_indexable_cnt == "False":
        #     is_crosspostable_counter += 1
        counter += 1

    print(removed)
    print(deleted)
    # print(is_robot_indexable_cnt)
    # print(removed_by_cat_counter)
