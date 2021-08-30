from db_utils.Con_DB import Con_DB
from tqdm import tqdm

class Statistic:
    def __init__(self):
        self.img = 0
        self.video = 0

    def precentage_media(self):
        # create a new object of connection to DB
        con = Con_DB()
        posts = con.get_posts_from_mongodb(collection_name="deletedData")

        # # Taking the posts that contains only variable "is_video"
        # all_posts = con.get_posts_text(posts, "is_video")
        numOfVideo, numOfImg, numOfPosts = 0, 0, 0

        for post in tqdm(posts.find({})):
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
    s.precentage_media()
