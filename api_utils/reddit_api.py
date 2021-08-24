# import requests
#
# # note that CLIENT_ID refers to 'personal use script' and SECRET_TOKEN to 'token'
# auth = requests.auth.HTTPBasicAuth('FVJ_dCHfJWeGtkI4ekc9ow', 'lSslynhzT_oHLnKvxjaerTZ3jvbCrQ')
#
# # here we pass our login method (password), username, and password
# data = {'grant_type': 'password',
#         'username': 'ObjectiveExisting282 ',
#         'password': 'sH231294'}
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
import praw
import pandas as pd


class reddit_api:
    def __init__(self, reddit):
        self.reddit = praw.Reddit(
            client_id="FVJ_dCHfJWeGtkI4ekc9ow",
            client_secret="lSslynhzT_oHLnKvxjaerTZ3jvbCrQ",
            user_agent="MyBot/0.0.1",
            username='ObjectiveExisting282 ',
            password='sH231294',
        )

if __name__ == '__main__':
    reddit = reddit_api()
    submission_reddit = reddit.reddit.submission(id=["hgpif7", "hgpamk", "hgp8pd"])
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
