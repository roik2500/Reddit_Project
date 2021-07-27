from psaw import PushshiftAPI
import datetime
import requests
import json
import pandas as pd
import os
import csv

api = PushshiftAPI()


def get_comments_data_from_pushshift(parent_id):
    url = "https://api.pushshift.io/reddit/submission/comment_ids/" + str(parent_id)
    req = requests.get(url)
    data = json.loads(req.text, strict=False)
    return data['data']


def search_submissions_and_comments(Subreddit, start_time, filter, Limit, mod_removed_boolean, user_removed_boolean):
    # The `search_comments` and `search_submissions` methods return generator objects
    sub_reader = api.search_submissions(subreddit=Subreddit, limit=Limit,
                                        mod_removed=mod_removed_boolean,
                                        user_removed=user_removed_boolean,
                                        after=start_time)

    submissions = [sub.d_ for sub in sub_reader]

    for submission in submissions:
        link_id = f"t3_{submission['id']}"
        com_reader = api.search_comments(link_id=link_id)
        comments = [comment.d_ for comment in com_reader]
        submitted_time = datetime.datetime.fromtimestamp(submission['created_utc']).isoformat().split("T")
        print("link_id: ", link_id, " comments: ", comments)
        print("subreddit: ", Subreddit)
        print("created: ", submitted_time)
        print("id: ", submission['id'])
        print("author: ", submission['author'])
        print("created_utc: ", start_time)
        print("title: ", submission['title'])
        print("body: ", submission['selftext'])
        print("url: ", submission['url'])
        print("------------------------------")


def search_submissions_with_query(subreddit, start_time, end_time, query, limit, mod_removed_boolean, user_removed_boolean):
    # The `search_comments` and `search_submissions` methods return generator objects
    submissions = api.search_submissions(subreddit=subreddit,
                                         q=query,
                                         after=start_time,
                                         # before=end_time,
                                         limit=limit)
    for submission in submissions:
        print(submission)


def search_comments_with_query(subreddit, start_time, end_time, query, limit, mod_removed_boolean, user_removed_boolean):
    # The `search_comments` and `search_submissions` methods return generator objects
    comments = api.search_comments(subreddit=subreddit,
                                   q=query,
                                   after=start_time,
                                   before=end_time,
                                   limit=limit)
    for comment in comments:
        print(comment)


def get_submissions_data(subreddit, limit, mod_removed_boolean, user_removed_boolean):
    # The `search_comments` and `search_submissions` methods return generator objects
    start_time = int(datetime.datetime(2021, 7, 21).timestamp())
    submissions = api.search_submissions(subreddit=subreddit,
                                         limit=limit)

    # submissions = api.search_comments(subreddit="wallstreetbets" , limit=50)
    '''mod_removed=mod_removed_boolean,
                                             user_removed=user_removed_boolean,
                                             after=start_time,'''
    data = []
    for submission in submissions:
        submitted_time = datetime.datetime.fromtimestamp(submission.created_utc).isoformat().split("T")
        print(submission)
        temp = []
        [temp.append(c) for c in submission]
        data.append(temp)
        # print(temp)
    headers = ["all_awardings", "approved_at_utc", "associated_award", "author", "author_flair_background_color",
               "author_flair_css_class", "author_flair_richtext", "author_flair_template_id", "author_flair_text",
               "author_flair_text_color", "author_flair_type", "author_fullname", "author_is_blocked",
               "author_patreon_flair", "author_premium", "awarders", "banned_at_utc", "body", "can_mod_post",
               "collapsed", "collapsed_because_crowd_control", "collapsed_reason", "collapsed_reason_code",
               "comment_type", "created_utc", "distinguished", "edited", "gildings", "id", "is_submitter", "link_id",
               "locked", "no_follow", "parent_id", "permalink", "retrieved_on", "score", "send_replies", "stickied",
               "subreddit", "subreddit_id", "top_awarded_type", "total_awards_received", "treatment_tags", "created",
               "d_"]
    print(data)
    return data
    # write_to_csv(headers, data)


def write_to_csv(headers,data):
    #change the path to the path in your computer
    path = 'C:/Users/roik2/PycharmProjects/pythonProject1/data.csv'
    try:
        with open(path,'w',encoding='UTF8',newline='') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(data)
    except:
        print("something wrong")


def read_from_csv(path):
    df = pd.read_csv(path)
    return df


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # print('precessors: ', os.cpu_count())
    subreddits_df = read_from_csv("C:/Users/User/Documents/Fourth Year/Project/subreddits_basic.csv")

    start_time = int(datetime.datetime(2021, 1, 21).timestamp())
    subreddits_col_name = subreddits_df.columns[3]
    subreddits_name = subreddits_df.loc[:, str(subreddits_col_name)]

    search_submissions_and_comments(Subreddit=subreddits_col_name, start_time=start_time,
                                    filter=['url', 'author', 'title', 'subreddit', 'selftext', 'id', 'link_id'],
                                    Limit=10, mod_removed_boolean=False, user_removed_boolean=True)
    for subreddit_name in subreddits_name:
        search_submissions_and_comments(Subreddit=subreddit_name, start_time=start_time,
                                        filter=['url', 'author', 'title', 'subreddit', 'selftext', 'id', 'link_id'],
                                        Limit=10, mod_removed_boolean=False, user_removed_boolean=True)

    search_submissions_with_query(subreddit='wallstreetbets', start_time=start_time, end_time=[],
                                  query='AMC', limit=10, mod_removed_boolean=False, user_removed_boolean=True)


    # start_time = int(datetime.datetime(2021, 7, 21).timestamp())
    # submissions_ids = search_submissions(subreddit='wallstreetbets', start_time=start_time,
    #                      filter=['url', 'author', 'title', 'subreddit', 'selftext', 'id', 'link_id'],
    #                      limit=10, mod_removed_boolean=False, user_removed_boolean=True)
    # for submission_id in submissions_ids:
    #     print("submission_id: ", get_comments_data_from_pushshift(str(submission_id)))
    # submissions_ids = get_submissions_data(subreddit='wallstreetbets',limit=50, mod_removed_boolean=False, user_removed_boolean=True)
    # for submission_id in submissions_ids:
    #     print("submission_id: ", get_comments_data_from_pushshift(str(submission_id)))
    # print(get_comments_data_from_pushshift("oq38bl"))
