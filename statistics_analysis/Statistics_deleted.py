from db_utils.Con_DB import Con_DB
from tqdm import tqdm
import pandas as pd
import os
from pprint import pprint
from datetime import datetime


class Statistic_Deleted:
    def __init__(self):
        # create a new object of connection to DB
        con = Con_DB()
        self.posts = con.get_cursor_from_mongodb(db_name='reddit',
                                                     collection_name=os.getenv("COLLECTION_NAME")).find({})
        self.df_size = 0
        self.user_deleted = 0
        self.removed = 0
        self.blanked = 0
        self.blanked_and_selftext = 0
        self.removed_and_selftext = 0
        self.removedP_and_selftextR = 0

    def percentage(self, percent):
        return (percent/self.df_size) * 100

    def deleted(self):

        for post in tqdm(self.posts):
            if post['reddit_api']['post']['selftext'] == '[removed]':
                self.removed+=1
            elif post['reddit_api']['post']['selftext'] == '[deleted]':
                self.user_deleted+=1

            if post['reddit_api']['post']['selftext'] == '':
                self.blanked += 1

            if post['reddit_api']['post']['selftext'] == '' and post['pushift_api']['selftext'] != '' and post['pushift_api']['selftext'] != '[deleted]' and post['pushift_api']['selftext'] != '[removed]':
                self.blanked_and_selftext += 1

            pushift_keys = post['pushift_api'].keys()
            if 'selftext' in pushift_keys and (post['reddit_api']['post']['selftext'] == '[removed]' ) and post['pushift_api']['selftext'] != '' and post['pushift_api']['selftext'] != '[deleted]' and post['pushift_api']['selftext'] != '[removed]':
                self.removed_and_selftext += 1

            if 'selftext' in pushift_keys and (post['pushift_api']['selftext'] == '[removed]') and \
                    post['reddit_api']['post']['selftext'] != '' and post['reddit_api']['post']['selftext'] != '[deleted]'\
                    and post['reddit_api']['post']['selftext'] != '[removed]':
                self.removedP_and_selftextR += 1
            self.df_size += 1

        print("deleted: ", self.percentage(self.user_deleted),
              "removed: ", self.percentage(self.removed),
              "% blanked: ", self.percentage(self.blanked),
              "% blanked_and_selftext: ", self.percentage(self.blanked_and_selftext),
              "% removed_and_selftext: ", self.percentage(self.removed_and_selftext),
              "% removedPushshift_and_selftextReddit: ", self.percentage(self.removedP_and_selftextR))


    def number_of_comments(self):
        n = 0
        dict_post_id_n_comments = {} # key post id, val number of comments that belong to the posts
        removed_posts = {} #key
        removed_posts_that_have_more_than_1_comment = {}
        number_of_removed_comments = 0
        number_of_deleted_comments = 0
        for object in tqdm(self.posts):
            n_temp = len(object['reddit_api']['comments'])
            n += n_temp
            if n_temp > 0:
                dict_post_id_n_comments[object['post_id']] = n_temp
            if object['reddit_api']['post']['selftext'] == '[removed]' and len(object['reddit_api']['post']['selftext']) == 9: # 9 is the number of the letter in [removed]
                removed_posts[object['post_id']] = n_temp
            if object['reddit_api']['post']['selftext'] == '[removed]' and n_temp > 0:
                removed_posts_that_have_more_than_1_comment[object['post_id']] = n_temp
            for comment in object['reddit_api']['comments']:
                if 'body' in comment['data'] and comment['data']['body'] == '[removed]':
                    number_of_removed_comments += 1
                elif 'body' in comment['data'] and comment['data']['body'] == '[deleted]':
                    number_of_deleted_comments += 1
        print("number of comments in all data - {}".format(n))
        print()
        print("post that have comments {} dict in all data :".format(len(dict_post_id_n_comments.keys())))
        # pprint(dict_post_id_n_comments)
        print()
        print("removed posts len {}".format(len(removed_posts.keys())))
        # pprint(removed_posts)
        print("removed_posts_that_have_more_than_1_comment len {}".format(len(removed_posts_that_have_more_than_1_comment.keys())))
        print("number_of_removed_comments {}".format(number_of_removed_comments))
        print("number_of_deleted_comments {}".format(number_of_deleted_comments))
        return number_of_removed_comments


class Statistic:

    def __init__(self):
        self.con_db = Con_DB()
        self.objects = self.con_db.get_cursor_from_mongodb(db_name='reddit',
                                                 collection_name=os.getenv("COLLECTION_NAME")).find({})

        self.post_quantity_per_month = {}
        self.post_quantity_per_day = {}

        self.comment_quantity_per_month = {}
        self.comment_quantity_per_day = {}


    def get_quantity(self, post_or_comment_str, dict_month, dict_day):

        for object in tqdm(self.objects):
            post_or_comments = self.con_db.get_text_from_post_OR_comment(object=object, post_or_comment=post_or_comment_str)
            for post_or_comment in post_or_comments:
                date_object, date_key = self.get_date_keys(post_or_comment)
                if date_key not in dict_month.keys():
                    dict_month[date_key] = 1
                else:
                    dict_month[date_key] += 1

                if date_object not in dict_day.keys():
                    dict_day[date_object] = 1
                else:
                    dict_day[date_object] += 1

        print("dict_month")
        pprint(dict_month)

        print("dict_day")
        pprint(dict_day)

    def get_removed_quantity(self, post_or_comment_str, dict_month, dict_day):

        for object in tqdm(self.objects):
            post_or_comments = self.con_db.get_text_from_post_OR_comment(object=object, post_or_comment=post_or_comment_str)
            for post_or_comment in post_or_comments:
                date_object, date_key = self.get_date_keys(post_or_comment)
                if post_or_comment[-1]:
                    if date_key in dict_month.keys():
                        dict_month[date_key] += 1
                    else:
                        dict_month[date_key] = 1

                    if date_object in dict_day.keys():
                        dict_day[date_object] += 1
                    else:
                        dict_day[date_object] = 1

        # print("dict_month")
        # pprint(dict_month)

        # print("dict_day")
        # pprint(dict_day)


    def get_date_keys(self, post_or_comment):
        date_object = post_or_comment[1]
        year = int(datetime.strptime(date_object, "%Y-%m-%d").date().year)
        month = int(datetime.strptime(date_object, "%Y-%m-%d").date().month)
        date_key = str(year) + "/" + str(month)
        return date_object, date_key


if __name__ == '__main__':
    s = Statistic_Deleted()
    # # print("deleted")
    # # s.deleted()
    # print("number_of_comments")
    # total_removed = s.number_of_comments()
    #
    # print("total_removed {}".format(total_removed))

    s = Statistic()
    # print("post")
    # s.get_quantity("post", s.post_quantity_per_month, s.post_quantity_per_day)
    # print("comment")
    # s.get_quantity("comment", s.comment_quantity_per_month, s.comment_quantity_per_day)

    # print('posts deleted quantity')
    # s.get_deleted_quantity("post", s.post_quantity_per_month, s.post_quantity_per_day)
    print('comments deleted quantity')
    s.get_removed_quantity("comment", s.post_quantity_per_month, s.post_quantity_per_day)