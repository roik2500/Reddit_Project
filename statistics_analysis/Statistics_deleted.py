from db_utils.Con_DB import Con_DB
from tqdm import tqdm
import pandas as pd
import pandasql as sqldf

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

        self.deleted_2_false = 0
        self.blanked = 0
        self.blanked_and_selftext = 0
        self.removed_and_selftext = 0
    def percentage(self, percent):
        return (percent/self.df_size) * 100

    def deleted(self):

        for post in tqdm(self.posts):
            # keys = post['re'].keys()
            if post['reddit_api'][0]['data']['children'][0]['data']['selftext'] == '[removed]':
                self.deleted_2_false+=1
            elif post['reddit_api'][0]['data']['children'][0]['data']['selftext'] == '[deleted]':
                self.deleted_2_false+=1

            if post['reddit_api'][0]['data']['children'][0]['data']['selftext'] == '':
                self.blanked += 1

            if post['reddit_api'][0]['data']['children'][0]['data']['selftext'] == '' and post['pushift_api']['selftext'] != '' and post['pushift_api']['selftext'] != '[deleted]' and post['pushift_api']['selftext'] != '[removed]':
                self.blanked_and_selftext += 1

            #or post['reddit_api'][0]['data']['children'][0]['data']['selftext'] == '[deleted]'
            pushift_keys = post['pushift_api'].keys()
            if 'selftext' in pushift_keys and (post['reddit_api'][0]['data']['children'][0]['data']['selftext'] == '[removed]' ) and post['pushift_api']['selftext'] != '' and post['pushift_api']['selftext'] != '[deleted]' and post['pushift_api']['selftext'] != '[removed]':
                self.removed_and_selftext += 1

        print("self.deleted_2_false: ", self.percentage(self.deleted_2_false),
              "% self.blanked: ", self.percentage(self.blanked),
              "% self.blanked_and_selftext: ", self.percentage(self.blanked_and_selftext),
              "% removed_and_selftext: ", self.percentage(self.removed_and_selftext))


    def precentage_media(self):


        # # Taking the posts that contains only variable "is_video"
        # all_posts = con.get_posts_text(posts, "is_video")
        numOfVideo, numOfImg, numOfPosts = 0, 0, 0

        for post in tqdm(self.posts.find({})):
            numOfPosts += 1  # counting the number of posts
            pushift_keys = post['pushift_api'].keys()

            if 'preview' in pushift_keys:
                # checking if this post contains a image
                if post['pushift_api']["preview"] is not None and post['pushift_api']["preview"]['enabled'] == True:
                    numOfImg += 1

            if 'is_video' in pushift_keys:
                # checking if this post contains a video
                if post['pushift_api']["is_video"]:
                    numOfVideo += 1


        self.video = "Video Precentage: " +str(round((numOfVideo / numOfPosts) * 100, 3)) + "%"
        self.img = "Image Precentage: "+str(round((numOfImg / numOfPosts) * 100, 3)) + "%"
        print(self.video)
        print(self.img)


if __name__ == '__main__':
    s = Statistic()
    s.deleted()