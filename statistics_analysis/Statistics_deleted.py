from db_utils.Con_DB import Con_DB
from tqdm import tqdm
import pandas as pd
import pandasql as sqldf
from pprint import pprint

class Statistic:
    def __init__(self):
        # create a new object of connection to DB
        con = Con_DB()
        post_collection = con.get_cursor_from_mongodb(collection_name="wallstreetbets")
        self.posts = [post for post in post_collection.find({})]
        # self.df=pd.DataFrame(p)
        # pushshift_collection = con.get_posts_from_mongodb(collection_name="pushift_api")
        # self.pushshift_posts = [post for post in pushshift_collection.find({})]

        self.df_size = len(self.posts)

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
            # keys = post['re'].keys()
            if post['reddit_api']['post']['selftext'] == '[removed]':
                self.removed+=1
            elif post['reddit_api']['post']['selftext'] == '[deleted]':
                self.user_deleted+=1

            if post['reddit_api']['post']['selftext'] == '':
                self.blanked += 1

            if post['reddit_api']['post']['selftext'] == '' and post['pushift_api']['selftext'] != '' and post['pushift_api']['selftext'] != '[deleted]' and post['pushift_api']['selftext'] != '[removed]':
                self.blanked_and_selftext += 1

            #or post['reddit_api'][0]['data']['children'][0]['data']['selftext'] == '[deleted]'
            pushift_keys = post['pushift_api'].keys()
            if 'selftext' in pushift_keys and (post['reddit_api']['post']['selftext'] == '[removed]' ) and post['pushift_api']['selftext'] != '' and post['pushift_api']['selftext'] != '[deleted]' and post['pushift_api']['selftext'] != '[removed]':
                self.removed_and_selftext += 1

            if 'selftext' in pushift_keys and (post['pushift_api']['selftext'] == '[removed]') and \
                    post['reddit_api']['post']['selftext'] != '' and post['reddit_api']['post']['selftext'] != '[deleted]'\
                    and post['reddit_api']['post']['selftext'] != '[removed]':
                self.removedP_and_selftextR += 1
                print(post['post_id'])
                print()

        print("deleted: ", self.percentage(self.user_deleted),
              "removed: ", self.percentage(self.removed),
              "% blanked: ", self.percentage(self.blanked),
              "% blanked_and_selftext: ", self.percentage(self.blanked_and_selftext),
              "% removed_and_selftext: ", self.percentage(self.removed_and_selftext),
              "% removedPushshift_and_selftextReddit: ", self.percentage(self.removedP_and_selftextR))


    def number_of_comments(self):
        n = 0
        dict_post_id_n_comments = {}
        removed_dict = {}
        removed_selftext_dict = {}
        for object in self.posts:
            n_temp = len(object['reddit_api']['comments'])
            n += n_temp
            if n_temp > 0:
                dict_post_id_n_comments[object['post_id']] = n_temp
            if object['reddit_api']['post']['selftext'] == '[removed]' and len(object['reddit_api']['post']['selftext']) == 9:
                removed_dict[object['post_id']] = n_temp
            if object['reddit_api']['post']['selftext'] == '[removed]' and n_temp > 0:
                removed_selftext_dict[object['post_id']] = n_temp
        print("number of comments in all data - {}".format(n))
        print()
        print("dict post and comments number {} dict in all data :".format(len(dict_post_id_n_comments.keys())))
        # pprint(dict_post_id_n_comments)
        print()
        print("removed posts len {}".format(len(removed_dict.keys())))
        # pprint(removed_dict)
        print("removed_comment_dict len {}".format(len(removed_selftext_dict.keys())))
if __name__ == '__main__':
    s = Statistic()
    # s.deleted()
    s.number_of_comments()