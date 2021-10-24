import requests
import praw
import os
from dotenv import load_dotenv
import datetime
from api_utils.PushshiftApi import PushshiftApi
load_dotenv("../.env")
#
# # note that CLIENT_ID refers to 'personal use script' and SECRET_TOKEN to 'token'
# auth = requests.auth.HTTPBasicAuth(os.getenv('CLIENT_ID'), os.getenv('CLIENT_SECRET'))
#
# # here we pass our login method (password), username, and password
# data = {'grant_type': 'password',
#         "username": os.getenv('USER_NAME'),
#         "password": os.getenv('PASSWORD')}
#
# # setup our header info, which gives reddit a brief description of our app
# headers = {'User-Agent': 'MyBot/0.0.1'}
#
# # send our request for an OAuth token
# res = requests.post('https://www.reddit.com/api/v1/access_token',
#                     auth=auth, data=data, headers=headers)
#
# # convert response to JSON and pull access_token value
# TOKEN = res.json()['access_token']
#
# # add authorization to our headers dictionary
# headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}
#
# # while the token is valid (~2 hours) we just add headers=headers to our requests
# requests.get('https://oauth.reddit.com/api/v1/me', headers=headers)
#
# res = requests.get("https://oauth.reddit.com/r/python/hot",
#                    headers=headers)
#
# print(res.json())  # let's see what we get




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
    submission_reddit = reddit.reddit.submission(id= "hgp8pd")
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
