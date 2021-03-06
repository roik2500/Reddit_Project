from pprint import pprint

from pmaw import PushshiftAPI as PushshiftApiPmaw
import datetime
import requests
import json
import pandas as pd
import os
import csv
import praw
from psaw import PushshiftAPI
from tqdm import tqdm


class PushshiftApi(object):
    def __init__(self):
        self.api_psaw = PushshiftAPI()
        self.api_pmaw = PushshiftApiPmaw()
        self.comments_columns_to_remove = ["all_awardings", "approved_at_utc", "associated_award",
                                           "author_flair_background_color",
                                           "author_flair_css_class", "author_flair_richtext",
                                           "author_flair_template_id",
                                           "author_flair_text",
                                           "author_flair_text_color", "author_flair_type", "author_patreon_flair",
                                           "awarders",
                                           "can_mod_post", "collapsed", "collapsed_because_crowd_control",
                                           "collapsed_reason",
                                           "comment_type",
                                           "distinguished", "edited", "gildings", "is_submitter", "locked", "no_follow",
                                           "retrieved_on", "score",
                                           "send_replies", "stickied", "top_awarded_type", "total_awards_received",
                                           "treatment_tags"]
        self.posts_columns_to_remove = ["all_awardings", "author_flair_css_class", "author_flair_richtext",
                                        "author_flair_type",
                                        "author_patreon_flair", "awarders", "can_mod_post", "score", "gildings",
                                        "locked",
                                        "no_follow",
                                        "retrieved_on", "send_replies", "stickied", "total_awards_received",
                                        "treatment_tags",
                                        "allow_live_comments",
                                        "author_flair_text", "contest_mode", "domain", "is_meta", "is_crosspostable",
                                        'is_original_content',
                                        'is_reddit_media_domain', 'is_robot_indexable', 'is_self', 'is_video',
                                        'link_flair_background_color',
                                        'link_flair_richtext', 'link_flair_text_color', 'link_flair_type', 'media_only',
                                        'over_18',
                                        'parent_whitelist_status', 'pinned', 'pwls', 'spoiler', 'subreddit_subscribers',
                                        'subreddit_type',
                                        'thumbnail', 'upvote_ratio', 'whitelist_status', 'wls']

    def get_submission(self, Subreddit, start_time, end_time,  Limit):

        posts = self.api_pmaw.search_submissions(subreddit=Subreddit, limit=Limit, after=start_time, before=end_time)

        submissions = [post for post in posts]

        return submissions

    def search_submissions_and_comments(self, Subreddit, filter, Limit, mod_removed_boolean,
                                        user_removed_boolean):
        # The `search_comments` and `search_submissions` methods return generator objects
        posts = self.api_pmaw.search_submissions(subreddit=Subreddit, limit=Limit, filter=filter,
                                                 mod_removed=mod_removed_boolean,
                                                 user_removed=user_removed_boolean,
                                                 can_gild=True)
        for post in posts:
            date = datetime.datetime.fromtimestamp(int(post['created_utc'])).isoformat()

        submissions = [post for post in posts]
        submission_ids_list = []

        for submission in tqdm(submissions):
            submission_ids_list.append(submission['id'])
            self.convert_time_format(submission)
            # initial comments section in every post
            submission['comments'] = {}

        # The `search_comments` and `search_submissions` methods return generator objects
        # comment_ids = api_pmaw.search_submission_comment_ids(ids=submission_ids_list)
        # comment_id_list = [c_id for c_id in comment_ids]
        #
        # # The `search_comments` and `search_submissions` methods return generator objects
        # comments = api_pmaw.search_comments(ids=comment_id_list)
        # comment_list = []
        # for comment in comments:
        #     comment_list.append(comment)
        #     convert_time_format(comment)
        #
        # comments_df = pd.DataFrame(comment_list, index=comment_id_list)
        # comments_df.drop(comments_columns_to_remove, axis=1, inplace=True)
        #
        # for index, row_comment in comments_df.iterrows():
        #     match_comment_to_post(row_comment, submission_ids_list, submissions)

        posts_df = pd.DataFrame(submissions, index=submission_ids_list)
        # posts_df.drop(self.posts_columns_to_remove, axis=1, inplace=True)
        # print(posts_df)
        # convert pandas Data frame to Json

    def get_comments_by_comments_ids(self, ids_list):
        return self.api_pmaw.search_comments(ids=ids_list)  # return generator

    def convert_time_format(self, comment_or_post):
        comment_or_post['created_utc'] = datetime.datetime.fromtimestamp(
            comment_or_post['created_utc']).isoformat().split(
            "T")
        comment_or_post['retrieved_on'] = datetime.datetime.fromtimestamp(
            comment_or_post['retrieved_on']).isoformat().split(
            "T")

    def match_comment_to_post(self, comment, submission_ids_list, submissions):
        id = comment['link_id'].replace('t3_', '')
        index_of_post_in_list = submission_ids_list.index(id)

        # find if there is a nested comment
        if submissions[index_of_post_in_list]['comments'].get(comment['id']) is None:
            submissions[index_of_post_in_list]['comments'].update({comment['id']: comment})
        else:
            submissions[index_of_post_in_list]['comments'][comment[id]].update({comment['id']: comment})

    def search_submissions_with_query(self, subreddit, start_time, end_time, query, limit, mod_removed_boolean,
                                      user_removed_boolean):
        # The `search_comments` and `search_submissions` methods return generator objects
        submissions = self.api_psaw.api_pmaw.search_submissions(subreddit=subreddit,
                                                                q=query,
                                                                after=start_time,
                                                                # before=end_time,
                                                                limit=limit)
        for submission in submissions:
            print(submission)

    def search_comments_with_query(self, subreddit, start_time, end_time, query, limit, mod_removed_boolean,
                                   user_removed_boolean):
        # The `search_comments` and `search_submissions` methods return generator objects
        comments = self.api_psaw.api_pmaw.search_comments(subreddit=subreddit,
                                                          q=query,
                                                          after=start_time,
                                                          before=end_time,
                                                          limit=limit)
        for comment in comments:
            print(comment)

    def write_to_csv(self, headers, data):
        # change the path to the path in your computer
        path = 'C:/Users/roik2/PycharmProjects/pythonProject1/data.csv'
        try:
            with open(path, 'w', encoding='UTF8', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(headers)
                writer.writerows(data)
        except:
            print("something wrong")

    def read_from_csv(self, path):
        df = pd.read_csv(path)
        return df

# # Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # df = pd.read_json(r'C:\Users\roik2\PycharmProjects\Reddit_Project\sampleJsonPosts.json')

    # subreddits_df = read_from_csv(r"C:\Users\shimon\Downloads\subreddits_basic.csv")
    p = PushshiftApi()
    comments = p.api_pmaw.search_comments(ids=['hflpkl1'])
    start_time = int(datetime.datetime(2019, 1, 1).timestamp())
    end_time = int(datetime.datetime(2019, 1, 2).timestamp())
        # subreddits_col_name = subreddits_df.columns[3]
    #subreddits_name = subreddits_df.loc[:, str(subreddits_col_name)]
    a = PushshiftApi()
    a.search_submissions_and_comments(Subreddit='wallstreetbets',
                                filter=['url', 'author', 'title', 'subreddit', 'selftext', 'id', 'link_id',
                                        'created_utc', 'retrieved_on', 'can_gild'],
                                Limit=3, mod_removed_boolean=True, user_removed_boolean=True)
#
    # for subreddit_name in subreddits_name:
    #     search_submissions_and_comments(Subreddit=subreddit_name, start_time=start_time,
    #                                     filter=['url', 'author', 'title', 'subreddit', 'selftext', 'id', 'link_id'],
    #                                     Limit=10, mod_removed_boolean=False, user_removed_boolean=True)
    #
    # search_submissions_with_query(subreddit='wallstreetbets', start_time=start_time, end_time=[],
    #                               query='AMC', limit=10, mod_removed_boolean=False, user_removed_boolean=True)
    #
    # search_comments_with_query(subreddit='wallstreetbets', start_time=start_time, end_time=[],
    #                            query='AMC', limit=10, mod_removed_boolean=False, user_removed_boolean=True)

if __name__ == '__main__':
    api = PushshiftApi()
    comment_ids = ['fq7rqzq']
    comments = api.get_comments_by_comments_ids(ids_list=comment_ids)
    comment_list = [comment for comment in comments]
    pprint(comment_list)