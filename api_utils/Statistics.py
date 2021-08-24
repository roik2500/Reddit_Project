from Con_DB import Con_DB
class Statistic:
    def __init__(self):
        self.img = 0
        self.video = 0

    def precentage_media(self):
        # create a new object of connection to DB
        con = Con_DB()
        posts = con.get_posts_from_mongodb("deletedData")

        # Taking the posts that contains only variable "is_video"
        all_posts = con.get_posts_text(posts, "is_video")
        numOfVideo, numOfImg, numOfPosts = 0, 0, 0

        for post in all_posts:
            numOfPosts += 1  # counting the number of posts

            # checking if this post contains a video
            if post["is_video"]:
                numOfVideo += 1

            # checking if this post contains a image
            if post["preview"] is not None and post["preview"]['enabled'] == True:
                numOfImg += 1

        self.video = str(round((numOfVideo / numOfPosts) * 100, 3)) + "%"
        self.img = str(round((numOfImg / numOfPosts) * 100, 3)) + "%"


if __name__ == '__main__':
    s = Statistic()
    s.precentage_media()
