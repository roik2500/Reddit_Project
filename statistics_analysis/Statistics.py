
import matplotlib.pyplot as plt

class Statistic:
    def __init__(self,subreddit,type):
        self.num_of_img = 0
        self.num_of_video = 0
        self.numOfPosts = 0
        self.subreddit = subreddit
        self.type_of_post = type

    '''
    This function is called from the main loop in Main_test file
    and the function updating the self variables.
    the function is checking if a specific post contains an image or video
    :argument con: The connection to DB --- was defined in the Main_test file
    :argument post: specific post from db 
    '''
    def precentage_media(self,con,post):
        pushift_keys={}
        if self.type_of_post == "Removed":
            if post['reddit_api']['post']['selftext'] == "[removed]":
                self.numOfPosts += 1  # counting the number of posts
                pushift_keys = post['pushift_api'].keys()
            else:
                return

        elif self.type_of_post == "NotRemoved":
            if post['reddit_api']['post']['selftext'] != "[removed]":
                self.numOfPosts += 1  # counting the number of posts
                pushift_keys = post['pushift_api'].keys()
            else:
                return

        elif self.type_of_post == "All":
            self.numOfPosts += 1  # counting the number of posts
            pushift_keys = post['pushift_api'].keys()
        else:
            return

        if 'preview' in pushift_keys:
            # checking if this post contains a image
            if post['pushift_api']["preview"] is not None and post['pushift_api']["preview"]['enabled'] == True:
                self.num_of_img += 1

        if 'is_video' in pushift_keys:
            # checking if this post contains a video
            if post['pushift_api']["is_video"]:
                self.num_of_video += 1


    '''
    This function is creating a graph that includes 3 bars per month
    bar  red - positive 
    bar  blue - neutral 
    bar  green - negative 
    '''
    def draw_statistic_bars(self, sentimentObject,path=""):
        x = list(sentimentObject.text_per_month.keys())
        k = [val['positive'] for val in sentimentObject.sentiment_per_month.values()]
        y = [val['neutral'] for val in sentimentObject.sentiment_per_month.values()]
        z = [val['negative'] for val in sentimentObject.sentiment_per_month.values()]

        x1 = [i+0.27 for i in list(sentimentObject.text_per_month.keys())]
        x2 = [i+(0.27*2) for i in list(sentimentObject.text_per_month.keys())]

        fig = plt.figure(figsize=(15, 5))
        ax = fig.add_subplot(111)

        rects1 = ax.bar(x, z, width=0.27, color='g', align='center')
        rects2 = ax.bar(x1 , y, width=0.27, color='b', align='center')
        rects3 = ax.bar(x2 , k, width=0.27, color='r', align='center')
        ax.legend((rects1[0], rects2[0], rects3[0]), ('negative', 'neutral', 'positive'))

        self.autolabel(rects1,ax)
        self.autolabel(rects2,ax)
        self.autolabel(rects3,ax)


        # if path !="":
        #     plt.savefig('{}/{} {}.png'.format(path, self.subreddit, self.type_of_post))

        return plt.show()
    '''
    Helper function for draw_statistic_bars()
    This function are indicate the amount of each bar on the graph
    '''
    def autolabel(self,rects,ax):
        for rect in rects:
            h = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2., 1.05 * h, '%d' % int(h),
                    ha='center', va='bottom')

    '''
    This function is display the data, its mean printing the self variables in %
    '''
    def get_percent(self):
        print("Statistic for {}:".format(self.type_of_post))
        print("Num of posts: {}".format(self.numOfPosts))
        print("Num of images: {}".format(self.num_of_img))
        print("Num of videos: {}".format(self.num_of_video))
        print("Video Precentage: " + str(round((self.num_of_video / self.numOfPosts) * 100, 4)) + "%")
        print("Image Precentage: " + str(round((self.num_of_img / self.numOfPosts) * 100, 4)) + "%\n")
        #print("sub-reddit: politics\n Total Posts: 5000\n Year: 2020")
        return {'num_of_video':self.num_of_video,'num_of_img':self.num_of_img,'numOfPosts':self.numOfPosts}

