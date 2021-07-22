from psaw import PushshiftAPI
import datetime
api = PushshiftAPI()
import pandas as pd
import os
import csv

def search_submissions(subreddit,filter, limit, mod_removed_boolean, user_removed_boolean):
    # The `search_comments` and `search_submissions` methods return generator objects
    start_time = int(datetime.datetime(2021, 7, 21).timestamp())
    submissions = api.search_submissions(mod_removed=mod_removed_boolean,
                                         user_removed=user_removed_boolean,
                                        after=start_time,
                                        subreddit=subreddit,
                                        filter=filter,
                                        limit=limit)
    for submission in submissions:
        print(submission)
        print("subreddit: ", subreddit)
        print("created: ", submission.created)
        print("author: ", submission.author)
        print("created_utc: ", submission.submitted_time)
        print("title: ", submission.title)
        print("url: ", submission.url)
        print("------------------------------")

def get_submissions_data(subreddit,limit, mod_removed_boolean, user_removed_boolean):
    # The `search_comments` and `search_submissions` methods return generator objects
    start_time= int(datetime.datetime(2021, 7, 21).timestamp())
    submissions = api.search_submissions(mod_removed=mod_removed_boolean,
                                         user_removed=user_removed_boolean,
                                        after=start_time,
                                        subreddit=subreddit,
                                        limit=limit)

    submissions = api.search_comments(limit=50)
    data = []
    for submission in submissions:
        submitted_time = datetime.datetime.fromtimestamp(submission.created_utc).isoformat().split("T")
        print(submission)
        temp = []
        [temp.append(c) for c in submission]
        data.append(temp)
        #print(temp)
    headers = ["all_awardings","approved_at_utc","associated_award","author","author_flair_background_color","author_flair_css_class","author_flair_richtext","author_flair_template_id","author_flair_text","author_flair_text_color","author_flair_type","author_fullname","author_is_blocked","author_patreon_flair","author_premium","awarders","banned_at_utc","body","can_mod_post","collapsed","collapsed_because_crowd_control","collapsed_reason","collapsed_reason_code","comment_type","created_utc","distinguished","edited","gildings","id","is_submitter","link_id","locked","no_follow","parent_id","permalink","retrieved_on","score","send_replies","stickied","subreddit","subreddit_id","top_awarded_type","total_awards_received","treatment_tags","created","d_"]
    print(data)
    write_to_csv(headers,data)



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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    #search_submissions(subreddit='wallstreetbets',limit=10, mod_removed_boolean=False, user_removed_boolean=True)
    get_submissions_data(subreddit='wallstreetbets',limit=50, mod_removed_boolean=False, user_removed_boolean=True)