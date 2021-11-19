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
        self.posts = con.get_cursor_from_mongodb_for_real(db_name='reddit',
                                                     collection_name=os.getenv("COLLECTION_NAME")).find({})
        # self.posts = con.get_cursor_from_mongodb(db_name='reddit',
        #                                              collection_name=os.getenv("COLLECTION_NAME")).find({})
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
        # self.objects = self.con_db.get_cursor_from_mongodb(db_name='local',
        #                                          collection_name=os.getenv("COLLECTION_NAME")) #.find({})
        self.objects = self.con_db.get_cursor_from_mongodb(db_name='reddit',
                                                          collection_name=os.getenv("COLLECTION_NAME")).find({})

        self.post_quantity_per_month = {}
        self.post_quantity_per_day = {}
        self.automod_filterd = 0

        self.comment_quantity_per_month = {}
        self.comment_quantity_per_day = {}


    def get_quantity(self, post_or_comment_str, dict_month, dict_day):

        for object in tqdm(self.objects):
            post_or_comments = self.con_db.get_text_from_post_OR_comment(_object=object, post_or_comment=post_or_comment_str)
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
        return dict_month, dict_day

    def get_removed_quantity(self, post_or_comment_str, dict_month, dict_day, automod_filterd):

        for obj in self.objects:
            if post_or_comment_str == 'post':
                if ('removed_by_category' in obj['reddit_api']['post'].keys()) and (
                        obj['reddit_api']['post']['removed_by_category'] == 'automod_filtered'):
                    automod_filterd += 1
            post_or_comments = self.con_db.get_text_from_post_OR_comment(_object=obj, post_or_comment=post_or_comment_str)
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

        print("dict_month")
        pprint(dict_month)

        print("dict_day")
        pprint(dict_day)

        print('automod_filterd')
        pprint(automod_filterd)
        return dict_month #, dict_day


    def get_date_keys(self, post_or_comment):
        date_object = post_or_comment[1]
        year = int(datetime.strptime(date_object, "%Y-%m-%d").date().year)
        month = int(datetime.strptime(date_object, "%Y-%m-%d").date().month)
        date_key = str(year) + "/" + str(month)
        return date_object, date_key


if __name__ == '__main__':
    # s = Statistic_Deleted()
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
    # s.get_removed_quantity("post", s.post_quantity_per_month, s.post_quantity_per_day, s.automod_filterd)
    print('comments deleted quantity')
    s.get_removed_quantity("comment", s.post_quantity_per_month, s.post_quantity_per_day, s.automod_filterd)


    '''
    
    {'2020/1': 6675,
     '2020/10': 9935,
     '2020/11': 19122,
     '2020/12': 27426,
     '2020/2': 19061,
     '2020/3': 32600,
     '2020/4': 16465,
     '2020/5': 11949,
     '2020/6': 15996,
     '2020/7': 13111,
     '2020/8': 14069,
     '2020/9': 14693}
    
    
    post
    239365it [03:28, 1147.02it/s]
    dict_month
    {'2020/1': 8348,
     '2020/10': 12255,
     '2020/11': 26398,
     '2020/12': 31826,
     '2020/2': 21278,
     '2020/3': 36589,
     '2020/4': 20123,
     '2020/5': 15709,
     '2020/6': 18109,
     '2020/7': 15170,
     '2020/8': 16407,
     '2020/9': 17153}
    dict_day
    {'2020-01-01': 104,
     '2020-01-02': 206,
     '2020-01-03': 308,
     '2020-01-04': 146,
     '2020-01-05': 119,
     '2020-01-06': 163,
     '2020-01-07': 212,
     '2020-01-08': 395,
     '2020-01-09': 311,
     '2020-01-10': 247,
     '2020-01-11': 144,
     '2020-01-12': 92,
     '2020-01-13': 260,
     '2020-01-14': 338,
     '2020-01-15': 282,
     '2020-01-16': 286,
     '2020-01-17': 320,
     '2020-01-18': 202,
     '2020-01-19': 133,
     '2020-01-20': 194,
     '2020-01-21': 323,
     '2020-01-22': 334,
     '2020-01-23': 338,
     '2020-01-24': 405,
     '2020-01-25': 225,
     '2020-01-26': 176,
     '2020-01-27': 367,
     '2020-01-28': 327,
     '2020-01-29': 412,
     '2020-01-30': 541,
     '2020-01-31': 438,
     '2020-02-01': 267,
     '2020-02-02': 246,
     '2020-02-03': 484,
     '2020-02-04': 1038,
     '2020-02-05': 942,
     '2020-02-06': 715,
     '2020-02-07': 681,
     '2020-02-08': 376,
     '2020-02-09': 286,
     '2020-02-10': 511,
     '2020-02-11': 793,
     '2020-02-12': 790,
     '2020-02-13': 721,
     '2020-02-14': 680,
     '2020-02-15': 315,
     '2020-02-16': 254,
     '2020-02-17': 345,
     '2020-02-18': 692,
     '2020-02-19': 896,
     '2020-02-20': 1197,
     '2020-02-21': 1061,
     '2020-02-22': 450,
     '2020-02-23': 312,
     '2020-02-24': 952,
     '2020-02-25': 977,
     '2020-02-26': 1003,
     '2020-02-27': 1620,
     '2020-02-28': 1728,
     '2020-02-29': 946,
     '2020-03-01': 713,
     '2020-03-02': 1235,
     '2020-03-03': 1272,
     '2020-03-04': 826,
     '2020-03-05': 898,
     '2020-03-06': 1003,
     '2020-03-07': 647,
     '2020-03-08': 479,
     '2020-03-09': 1678,
     '2020-03-10': 1153,
     '2020-03-11': 1058,
     '2020-03-12': 2296,
     '2020-03-13': 2120,
     '2020-03-14': 1100,
     '2020-03-15': 909,
     '2020-03-16': 2610,
     '2020-03-17': 1566,
     '2020-03-18': 1846,
     '2020-03-19': 1464,
     '2020-03-20': 1464,
     '2020-03-21': 727,
     '2020-03-22': 624,
     '2020-03-23': 1298,
     '2020-03-24': 1210,
     '2020-03-25': 1330,
     '2020-03-26': 1320,
     '2020-03-27': 1069,
     '2020-03-28': 619,
     '2020-03-29': 513,
     '2020-03-30': 701,
     '2020-03-31': 841,
     '2020-04-01': 749,
     '2020-04-02': 695,
     '2020-04-03': 928,
     '2020-04-04': 359,
     '2020-04-05': 364,
     '2020-04-06': 698,
     '2020-04-07': 779,
     '2020-04-08': 672,
     '2020-04-09': 1026,
     '2020-04-10': 753,
     '2020-04-11': 510,
     '2020-04-12': 502,
     '2020-04-13': 592,
     '2020-04-14': 794,
     '2020-04-15': 810,
     '2020-04-16': 682,
     '2020-04-17': 927,
     '2020-04-18': 474,
     '2020-04-19': 456,
     '2020-04-20': 838,
     '2020-04-21': 1061,
     '2020-04-22': 681,
     '2020-04-23': 689,
     '2020-04-24': 673,
     '2020-04-25': 408,
     '2020-04-26': 373,
     '2020-04-27': 517,
     '2020-04-28': 604,
     '2020-04-29': 789,
     '2020-04-30': 720,
     '2020-05-01': 810,
     '2020-05-02': 1441,
     '2020-05-03': 326,
     '2020-05-04': 415,
     '2020-05-05': 541,
     '2020-05-06': 589,
     '2020-05-07': 653,
     '2020-05-08': 643,
     '2020-05-09': 361,
     '2020-05-10': 254,
     '2020-05-11': 432,
     '2020-05-12': 559,
     '2020-05-13': 567,
     '2020-05-14': 557,
     '2020-05-15': 529,
     '2020-05-16': 313,
     '2020-05-17': 266,
     '2020-05-18': 622,
     '2020-05-19': 614,
     '2020-05-20': 585,
     '2020-05-21': 550,
     '2020-05-22': 481,
     '2020-05-23': 308,
     '2020-05-24': 215,
     '2020-05-25': 252,
     '2020-05-26': 483,
     '2020-05-27': 573,
     '2020-05-28': 609,
     '2020-05-29': 557,
     '2020-05-30': 328,
     '2020-05-31': 276,
     '2020-06-01': 496,
     '2020-06-02': 514,
     '2020-06-03': 514,
     '2020-06-04': 462,
     '2020-06-05': 714,
     '2020-06-06': 349,
     '2020-06-07': 231,
     '2020-06-08': 762,
     '2020-06-09': 1002,
     '2020-06-10': 959,
     '2020-06-11': 1308,
     '2020-06-12': 1113,
     '2020-06-13': 562,
     '2020-06-14': 457,
     '2020-06-15': 730,
     '2020-06-16': 785,
     '2020-06-17': 717,
     '2020-06-18': 708,
     '2020-06-19': 694,
     '2020-06-20': 468,
     '2020-06-21': 274,
     '2020-06-22': 492,
     '2020-06-23': 572,
     '2020-06-24': 563,
     '2020-06-25': 515,
     '2020-06-26': 566,
     '2020-06-27': 379,
     '2020-06-28': 289,
     '2020-06-29': 439,
     '2020-06-30': 475,
     '2020-07-01': 518,
     '2020-07-02': 598,
     '2020-07-03': 346,
     '2020-07-04': 232,
     '2020-07-05': 214,
     '2020-07-06': 496,
     '2020-07-07': 539,
     '2020-07-08': 551,
     '2020-07-09': 625,
     '2020-07-10': 660,
     '2020-07-11': 441,
     '2020-07-12': 279,
     '2020-07-13': 661,
     '2020-07-14': 589,
     '2020-07-15': 600,
     '2020-07-16': 620,
     '2020-07-17': 516,
     '2020-07-18': 342,
     '2020-07-19': 251,
     '2020-07-20': 451,
     '2020-07-21': 552,
     '2020-07-22': 532,
     '2020-07-23': 563,
     '2020-07-24': 582,
     '2020-07-25': 338,
     '2020-07-26': 237,
     '2020-07-27': 426,
     '2020-07-28': 490,
     '2020-07-29': 539,
     '2020-07-30': 699,
     '2020-07-31': 683,
     '2020-08-01': 339,
     '2020-08-02': 281,
     '2020-08-03': 492,
     '2020-08-04': 646,
     '2020-08-05': 678,
     '2020-08-06': 648,
     '2020-08-07': 697,
     '2020-08-08': 353,
     '2020-08-09': 294,
     '2020-08-10': 468,
     '2020-08-11': 593,
     '2020-08-12': 709,
     '2020-08-13': 633,
     '2020-08-14': 628,
     '2020-08-15': 322,
     '2020-08-16': 281,
     '2020-08-17': 416,
     '2020-08-18': 556,
     '2020-08-19': 556,
     '2020-08-20': 596,
     '2020-08-21': 661,
     '2020-08-22': 436,
     '2020-08-23': 315,
     '2020-08-24': 564,
     '2020-08-25': 531,
     '2020-08-26': 682,
     '2020-08-27': 805,
     '2020-08-28': 637,
     '2020-08-29': 473,
     '2020-08-30': 349,
     '2020-08-31': 768,
     '2020-09-01': 933,
     '2020-09-02': 982,
     '2020-09-03': 1188,
     '2020-09-04': 1284,
     '2020-09-05': 523,
     '2020-09-06': 343,
     '2020-09-07': 310,
     '2020-09-08': 756,
     '2020-09-09': 829,
     '2020-09-10': 701,
     '2020-09-11': 620,
     '2020-09-12': 373,
     '2020-09-13': 285,
     '2020-09-14': 572,
     '2020-09-15': 652,
     '2020-09-16': 661,
     '2020-09-17': 635,
     '2020-09-18': 443,
     '2020-09-19': 375,
     '2020-09-20': 272,
     '2020-09-21': 645,
     '2020-09-22': 425,
     '2020-09-23': 842,
     '2020-09-24': 617,
     '2020-09-25': 262,
     '2020-09-26': 256,
     '2020-09-27': 160,
     '2020-09-28': 362,
     '2020-09-29': 414,
     '2020-09-30': 433,
     '2020-10-01': 389,
     '2020-10-02': 680,
     '2020-10-03': 288,
     '2020-10-04': 185,
     '2020-10-05': 378,
     '2020-10-06': 570,
     '2020-10-07': 528,
     '2020-10-08': 526,
     '2020-10-09': 491,
     '2020-10-10': 269,
     '2020-10-11': 238,
     '2020-10-12': 418,
     '2020-10-13': 448,
     '2020-10-14': 512,
     '2020-10-15': 582,
     '2020-10-16': 535,
     '2020-10-17': 298,
     '2020-10-18': 203,
     '2020-10-19': 201,
     '2020-10-20': 259,
     '2020-10-21': 472,
     '2020-10-22': 448,
     '2020-10-23': 419,
     '2020-10-24': 222,
     '2020-10-25': 196,
     '2020-10-26': 370,
     '2020-10-27': 404,
     '2020-10-28': 501,
     '2020-10-29': 482,
     '2020-10-30': 479,
     '2020-10-31': 264,
     '2020-11-01': 175,
     '2020-11-02': 301,
     '2020-11-03': 381,
     '2020-11-04': 356,
     '2020-11-05': 68,
     '2020-11-06': 114,
     '2020-11-07': 255,
     '2020-11-08': 224,
     '2020-11-09': 935,
     '2020-11-10': 758,
     '2020-11-11': 546,
     '2020-11-12': 729,
     '2020-11-13': 925,
     '2020-11-14': 444,
     '2020-11-15': 360,
     '2020-11-16': 691,
     '2020-11-17': 1083,
     '2020-11-18': 1028,
     '2020-11-19': 873,
     '2020-11-20': 1014,
     '2020-11-21': 478,
     '2020-11-22': 412,
     '2020-11-23': 1088,
     '2020-11-24': 1731,
     '2020-11-25': 2402,
     '2020-11-26': 1684,
     '2020-11-27': 3004,
     '2020-11-28': 1280,
     '2020-11-29': 925,
     '2020-11-30': 2134,
     '2020-12-01': 2006,
     '2020-12-02': 1676,
     '2020-12-03': 939,
     '2020-12-04': 902,
     '2020-12-05': 707,
     '2020-12-06': 517,
     '2020-12-07': 1159,
     '2020-12-08': 1461,
     '2020-12-09': 1428,
     '2020-12-10': 1165,
     '2020-12-11': 1071,
     '2020-12-12': 606,
     '2020-12-13': 427,
     '2020-12-14': 905,
     '2020-12-15': 1066,
     '2020-12-16': 1214,
     '2020-12-17': 1310,
     '2020-12-18': 1362,
     '2020-12-19': 713,
     '2020-12-20': 596,
     '2020-12-21': 1162,
     '2020-12-22': 1528,
     '2020-12-23': 1330,
     '2020-12-24': 918,
     '2020-12-25': 629,
     '2020-12-26': 562,
     '2020-12-27': 563,
     '2020-12-28': 929,
     '2020-12-29': 1013,
     '2020-12-30': 984,
     '2020-12-31': 978}
    
    Process finished with exit code 0
    '''