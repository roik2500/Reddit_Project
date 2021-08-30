

class Statistic:
    def __init__(self):
        self.num_of_img = 0
        self.num_of_video = 0
        self.numOfPosts = 0

    '''
    This function is called from the main loop in Main_test file
    and the function updating the self variables.
    the function is checking if a specific post contains an image or video
    :argument con: The connection to DB --- was defined in the Main_test file
    :argument post: specific post from db 
    '''
    def precentage_media(self,con,post):
        # create a new object of connection to DB
        #con = Con_DB()
        #posts = con.get_cursor_from_mongodb(collection_name="politics")

        # # Taking the posts that contains only variable "is_video"
        # all_posts = con.get_posts_text(posts, "is_video")
       # numOfVideo, numOfImg, numOfPosts = 0, 0, 0

        #for post in tqdm(posts.find({})):
            self.numOfPosts += 1  # counting the number of posts
            pushift_keys = post['pushift_api'].keys()

            if 'preview' in pushift_keys:
                # checking if this post contains a image
                if post['pushift_api']["preview"] is not None and post['pushift_api']["preview"]['enabled'] == True:
                    self.num_of_img += 1

            if 'is_video' in pushift_keys:
                # checking if this post contains a video
                if post['pushift_api']["is_video"]:
                    self.num_of_video += 1

    '''
    This function is display the data, its mean printing the self variables in %
    '''
    def get_percent(self):
        print("Statistic:\n")
        print("Num of posts: {}".format(self.numOfPosts))
        print("Num of images: {}".format(self.num_of_img))
        print("Num of videos: {}".format(self.num_of_video))

        print("Video Precentage: " + str(round((self.num_of_video / self.numOfPosts) * 100, 4)) + "%")
        print("Image Precentage: " + str(round((self.num_of_img / self.numOfPosts) * 100, 4)) + "%")
        print("sub-reddit: politics\n Total Posts: 5000\n Year: 2020")
        return {'num_of_video':self.num_of_video,'num_of_img':self.num_of_img,'numOfPosts':self.numOfPosts}

