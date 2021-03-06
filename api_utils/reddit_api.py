import requests
import praw
import os
from dotenv import load_dotenv
import datetime
from api_utils.PushshiftApi import PushshiftApi

load_dotenv()

class reddit_api:
    def __init__(self):
        self.reddit = praw.Reddit(
            client_id=os.getenv('CLIENT_ID'),
            client_secret=os.getenv('CLIENT_SECRET'),
            user_agent=os.getenv('USER_AGENT'),
            username=os.getenv('USER_NAME'),
            password=os.getenv('PASSWORD'),
            check_for_async=False
        )
        self.pushshift = PushshiftApi()

    def convert_time_format(self, comment_or_post):
        comment_or_post['created_utc'] = datetime.datetime.fromtimestamp(
            int(comment_or_post['created_utc'])).isoformat().split(
            "T")

    async def extract_reddit_data_parallel(self, sub):
        url = sub["permalink"]
        post_from_reddit = self.reddit.request('GET', url)
        self.convert_time_format(post_from_reddit[0]['data']['children'][0]['data'])

        relevent_data_post_from_reddit = \
            {"post": post_from_reddit[0]['data']['children'][0]['data'],
             "comments": post_from_reddit[1]['data']['children']
             }
        return relevent_data_post_from_reddit

    def get_user_commnets(self, reddit_user_name):
        return self.reddit.redditor(reddit_user_name).comments.new(limit=1000)

    def get_removed_comment_from_reddit(self, user_comments, removed_comment_id):
        try:
            for comment in user_comments:
                if comment.id == removed_comment_id:
                    return comment.body
        except:
            print('user has deleted')
    # async def extract_reddit_data_parallel(self, sub):
    #     url = sub["permalink"]
    #     self.pushshift.convert_time_format(sub)
    #     post_from_reddit = self.reddit.request('GET', url)
    #
    #     self.convert_time_format(post_from_reddit[0]['data']['children'][0]['data'])
    #
    #     relevent_data_post_from_reddit = \
    #         {"post": post_from_reddit[0]['data']['children'][0]['data'],
    #          "comments": post_from_reddit[1]['data']['children']
    #          }
    #
    #     final = {
    #         "post_id": post_from_reddit[0]['data']['children'][0]['data']['id'],
    #         "reddit_api": relevent_data_post_from_reddit, "pushift_api": sub
    #     }
    #     return final


if __name__ == '__main__':
    reddit = reddit_api()
    comment_reddit = reddit.get_user_commnets(reddit_user_name="Remarkable_Fish_1127")
    submission_reddit = reddit.get_removed_comment_from_reddit(comment_reddit,removed_comment_id="hj9ty0p")
    print(submission_reddit)

# path = r'C:\Users\shimon\Visual Studio Code Projects\Reddit_Project'
# df = pd.read_json(path)
# is_crosspostable_counter = 0
# removed_by_cat_counter = 0
# is_robot_indexable_cnt = 0
# link_flair_indexable_cnt = 0
#
# for submission in df:
#     submission_reddit = reddit.submission(id=submission.id)
#     if submission.is_crosspostable == "false":
#         is_crosspostable_counter += 1
#     if submission.removed_by_category == "moderator":
#         removed_by_cat_counter += 1
#     if submission.is_robot_indexable_cnt == "false":
#         is_crosspostable_counter += 1
#     if submission.link_flair_indexable_cnt == "false":
#         is_crosspostable_counter += 1
#
# for top_level_comment in submission.comments:
#     print("id: ", top_level_comment, ", body: ", top_level_comment.body)
