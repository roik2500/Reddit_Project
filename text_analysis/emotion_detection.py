import os
from statistics import mean
import text2emotion as te
from db_utils.Con_DB import Con_DB
from dotenv import load_dotenv
from datetime import datetime
from datetime import date
import json
import re
import matplotlib.pyplot as plt
from pprint import pprint
from decimal import Decimal
from tqdm import tqdm
from db_utils.FileReader import FileReader
from pysentimiento import EmotionAnalyzer
# emotion_analyzer = EmotionAnalyzer(lang="en")
#
# print(emotion_analyzer.predict('With market value of approximately $3 billion and with $1.5 billion cash on balance '
#                                'sheet , $wish is trading at 0.5 times the sales. With recent downgrades pointing to '
#                                'same regurgitated talking paints by the CEO during the last quarterly earnings call . '
#                                'With each downgrade the stock price is dropping 10-15% and it is so funny that some '
#                                'market participants still believe that markets are efficient. If this slide continues '
#                                'the stock price can drop below the cash value and I would not be not surprised. Its '
#                                'a best example of greed and irrationality of stock market and is difficult to fathom. '
#                                'As I have experienced myself, Wish is trying to make itself better with respect to '
#                                'shipping and quality of its products this company is here to stay. As they say the '
#                                'night is darkest before dawn. Hang in their fellow 🦍 as tide will change and gotta '
#                                'have some patience.'))


load_dotenv()


class EmotionDetection:
    def __init__(self):
        self.emotion_dict = {}
        # self.emotion_posts_avg_of_subreddit = {"Angry": {}, "Fear": {},
        #                                        "Happy": {}, "Sad": {}, "Surprise": {}}
        self.emotion_posts_avg_of_subreddit = {"Disgust": {}, "Others": {},
                                               "Anger": {}, "Fear": {}, "Surprise": {}, "Sadness": {}, "Joy": {}}
        self.emotion_analyzer = EmotionAnalyzer(lang="en")

    def extract_posts_emotion_rate(self, posts, con_DB):  # posts format: [title + selftext, date, id]
        # self.emotion_dict = {}
        for post in tqdm(posts):
            items = con_DB.get_text_from_post_OR_comment(object=post, post_or_comment='post')
            for item in items:
                if item[-1]:
                    break
                year = int(datetime.strptime(item[1]
                                             , "%Y-%m-%d").date().year)
                month = int(datetime.strptime(item[1]
                                              , "%Y-%m-%d").date().month)
                date_key = str(year) + "/" + str(month)

                self.insert_rates_to_emotion_dict(date_key, item[2], item[0])

    def insert_rates_to_emotion_dict(self, date_key, reddit_id, tokens):
        if date_key in self.emotion_dict.keys():
            self.emotion_dict[date_key].append([reddit_id, self.get_post_emotion_rate(tokens)])
        else:
            self.emotion_dict[date_key] = [[reddit_id, self.get_post_emotion_rate(tokens)]]

    def get_post_emotion_rate(self, text):
        return self.emotion_analyzer.predict(text)

    # date_format  = '%Y/%m' or  "%Y-%m-%d"
    def emotion_plot_for_posts_in_subreddit(self, date_format, subreddit_name, NER, path_to_save_plt, category):

        x1 = sorted([*self.emotion_posts_avg_of_subreddit["Disgust"]], key=lambda t: datetime.strptime(t, date_format))
        x2 = sorted([*self.emotion_posts_avg_of_subreddit["Others"]], key=lambda t: datetime.strptime(t, date_format))
        x3 = sorted([*self.emotion_posts_avg_of_subreddit["Anger"]], key=lambda t: datetime.strptime(t, date_format))
        x4 = sorted([*self.emotion_posts_avg_of_subreddit["Fear"]], key=lambda t: datetime.strptime(t, date_format))
        x5 = sorted([*self.emotion_posts_avg_of_subreddit["Surprise"]], key=lambda t: datetime.strptime(t, date_format))
        x6 = sorted([*self.emotion_posts_avg_of_subreddit["Sadness"]], key=lambda t: datetime.strptime(t, date_format))
        x7 = sorted([*self.emotion_posts_avg_of_subreddit["Joy"]], key=lambda t: datetime.strptime(t, date_format))

        y1 = [*self.emotion_posts_avg_of_subreddit["Disgust"].values()]
        y2 = [*self.emotion_posts_avg_of_subreddit["Others"].values()]
        y3 = [*self.emotion_posts_avg_of_subreddit["Anger"].values()]
        y4 = [*self.emotion_posts_avg_of_subreddit["Fear"].values()]
        y5 = [*self.emotion_posts_avg_of_subreddit["Surprise"].values()]
        y6 = [*self.emotion_posts_avg_of_subreddit["Sadness"].values()]
        y7 = [*self.emotion_posts_avg_of_subreddit["Joy"].values()]

        # plot lines
        plt.xticks(rotation=90)
        plt.plot(x1, y1, label="Disgust", linestyle="-")
        plt.plot(x2, y2, label="Others", linestyle="--")
        plt.plot(x3, y3, label="Anger", linestyle="-.")
        plt.plot(x4, y4, label="Fear", linestyle=":")
        plt.plot(x5, y5, label="Surprise", linestyle="-")
        plt.plot(x6, y6, label="Sadness", linestyle="-")
        plt.plot(x7, y7, label="Joy", linestyle="-")

        plt.title('Emotion Detection to ' + subreddit_name + ' subreddit ' + NER + "-" + category)
        plt.ylabel("Emotion rate")
        plt.xlabel("Time (month)")
        plt.legend()

        if re.match("^[A-Za-z0-9_-]*$", NER):
            plt.savefig(path_to_save_plt + '_' + NER + '_' + datetime.now().strftime("%H-%M-%S") + ".jpg",
                        transparent=True)
        plt.show()

    # date_format  = '%Y/%m' or  "%Y-%m-%d"
    def emotion_plot_for_all_posts_in_subreddit(self, date_format, subreddit_name, path_to_save_plt, category,
                                                path_to_read_data):

        x1 = sorted([*self.emotion_posts_avg_of_subreddit["Disgust"]], key=lambda t: datetime.strptime(t, date_format))
        x2 = sorted([*self.emotion_posts_avg_of_subreddit["Others"]], key=lambda t: datetime.strptime(t, date_format))
        x3 = sorted([*self.emotion_posts_avg_of_subreddit["Anger"]], key=lambda t: datetime.strptime(t, date_format))
        x4 = sorted([*self.emotion_posts_avg_of_subreddit["Fear"]], key=lambda t: datetime.strptime(t, date_format))
        x5 = sorted([*self.emotion_posts_avg_of_subreddit["Surprise"]], key=lambda t: datetime.strptime(t, date_format))
        x6 = sorted([*self.emotion_posts_avg_of_subreddit["Sadness"]], key=lambda t: datetime.strptime(t, date_format))
        x7 = sorted([*self.emotion_posts_avg_of_subreddit["Joy"]], key=lambda t: datetime.strptime(t, date_format))

        y1 = [*self.emotion_posts_avg_of_subreddit["Disgust"].values()]
        y2 = [*self.emotion_posts_avg_of_subreddit["Others"].values()]
        y3 = [*self.emotion_posts_avg_of_subreddit["Anger"].values()]
        y4 = [*self.emotion_posts_avg_of_subreddit["Fear"].values()]
        y5 = [*self.emotion_posts_avg_of_subreddit["Surprise"].values()]
        y6 = [*self.emotion_posts_avg_of_subreddit["Sadness"].values()]
        y7 = [*self.emotion_posts_avg_of_subreddit["Joy"].values()]

        # plot lines
        plt.xticks(rotation=90)
        plt.plot(x1, y1, label="Disgust", linestyle="-")
        plt.plot(x2, y2, label="Others", linestyle="--")
        plt.plot(x3, y3, label="Anger", linestyle="-.")
        plt.plot(x4, y4, label="Fear", linestyle=":")
        plt.plot(x5, y5, label="Surprise", linestyle="-")
        plt.plot(x6, y6, label="Sadness", linestyle="-")
        plt.plot(x7, y7, label="Joy", linestyle="-")

        plt.title('Emotion Detection to ' + subreddit_name + ' subreddit ' + "-" + category)
        plt.ylabel("Emotion rate")
        plt.xlabel("Time (month)")
        plt.legend()
        plt.savefig(path_to_save_plt + '_' + subreddit_name + '_' + category +
                    '_' + datetime.now().strftime("%H-%M-%S") + ".jpg", transparent=True)
        plt.show()

    '''This method plot graph per emotion(like Fear) for one NER'''

    # date_format  = '%Y/%m' or  "%Y-%m-%d"
    def emotion_plot_per_NER(self, date_format, subreddit_name, NER, path_to_save_plt,
                             removed_df, not_removed_df, emotions_list_category):

        for emotion_category in emotions_list_category:
            removed_sorted = sorted(removed_df[emotion_category].iloc[0].replace("'", '')[2:-2].split('), ('),
                                    key=lambda t: datetime.strptime(t.split(', ')[0], date_format))
            not_removed_sorted = sorted(not_removed_df[emotion_category].iloc[0].replace("'", '')[2:-2].split('), ('),
                                        key=lambda t: datetime.strptime(t.split(', ')[0], date_format))

            # all_sorted=sorted(all_df[emotion_category].iloc[0].replace("'", '')[2:-2].split('), ('),all_df
            #                      key=lambda t: datetime.strptime(t.split(', ')[0], date_format))
            x1_removed = [datetime.strptime(key_date.split(', ')[0], date_format) for key_date in removed_sorted]
            x2_not_removed = [datetime.strptime(key_date.split(', ')[0], date_format) for key_date in
                              not_removed_sorted]
            # x3_all = [datetime.strptime(key_date .split(', ')[0], date_format) for key_date  in all_sorted]

            y1_removed = [round(Decimal(key_date.split(', ')[1]), 2) for key_date in removed_sorted]
            y2_not_removed = [round(Decimal(key_date.split(', ')[1]), 2) for key_date in not_removed_sorted]

            x3_interseting_point, y3_interseting_point = self.get_interseting_point(removed=removed_sorted,
                                                                                    not_removed=not_removed_sorted,
                                                                                    date_format=date_format)
            # y3_all = [round(Decimal(key_date .split(', ')[1]), 2) for key_date  in all_sorted]

            # plot lines
            fig, ax = plt.subplots()

            # ax.plot_date(x, y, markerfacecolor='CornflowerBlue', markeredgecolor='white')
            ax.set_ylim(0.0, max(*y1_removed, *y2_not_removed) + Decimal(0.04))
            fig.autofmt_xdate()
            ax.set_xlim([date(2020, 1, 1), date(2020, 12, 31)])
            # plt.show()

            # plt.ylim(0.0, max(*y1_removed, *y2_not_removed)+Decimal(0.04))
            # plt.xlim([datetime.date(2020, 1, 1), datetime.date(2020, 12, 31)])
            # plt.xticks(rotation=45)

            ax.plot_date(x1_removed, y1_removed, label="Removed", linestyle="-", marker='o')
            ax.plot_date(x2_not_removed, y2_not_removed, label="Not Removed", linestyle="--", marker='o')
            plt.scatter(x3_interseting_point, y3_interseting_point, label="Interesting point", marker='v', color="red")
            # plt.plot(x3_all, y3_all, label="All", linestyle="-.", marker='o')

            plt.title('Emotion Detection to ' + subreddit_name + ' subreddit ' + NER + "-" + emotion_category)
            plt.ylabel("Emotion rate")
            plt.xlabel("Time (month)")
            plt.legend()
            plt.savefig(
                path_to_save_plt + '_' + NER + '_' + emotion_category + datetime.now().strftime("%H-%M-%S") + ".jpg",
                transparent=True)
            plt.show()
            plt.clf()

    def get_interseting_point(self, removed, not_removed, date_format):
        removed = {datetime.strptime(key_date.split(', ')[0], date_format): key_date.split(', ')[1] for key_date in
                   removed}
        not_removed = {datetime.strptime(key_date.split(', ')[0], date_format): key_date.split(', ')[1] for key_date in
                       not_removed}
        longest_series = len(removed) > len(not_removed)
        interseting_point_dict = {}
        if longest_series:
            data = removed
        else:
            data = not_removed

        for rec in data.items():
            if rec[0] in removed and rec[0] in not_removed:
                if removed[rec[0]] > not_removed[rec[0]]:
                    interseting_point_dict[rec[0]] = "{}".format(float(removed[rec[0]]) + 0.02)

        return interseting_point_dict.keys(), interseting_point_dict.values()


    def calculate_post_emotion_rate_mean(self):
        # self.emotion_posts_avg_of_subreddit = {"Disgust": {}, "Others": {},
        #                                        "Anger": {}, "Fear": {}, "Surprise": {}, "Sadness": {}, "Joy": {}}
        for emotion in self.emotion_posts_avg_of_subreddit.keys():
            for key, month_posts_emotions in self.emotion_dict.items():
                self.emotion_posts_avg_of_subreddit[emotion][key] = \
                    mean([emotion_rate[1].probas[emotion.lower()] for emotion_rate in month_posts_emotions])

    def get_plot_and_emotion_rate_from_all_posts_in_category(self, data_cursor, Con_DB,
                                                             path_to_read_data, path_to_save_plt, category,
                                                             subreddit_name):
        print("extract emotion rate")
        self.extract_posts_emotion_rate(posts=data_cursor, con_DB=Con_DB)
        # has_removed=False -> get data that the selftext of post are removed
        #
        print("MEAN")
        pprint(self.emotion_dict)
        self.calculate_post_emotion_rate_mean()
        print("write to disk")
        pprint(self.emotion_posts_avg_of_subreddit)
        file_name = 'emotion_rate_{}_{}'.format(subreddit_name, category)
        file_reader.write_dict_to_json(path=path_to_read_data,
                                       file_name=file_name,
                                       dict_to_write=emotion_detection.emotion_posts_avg_of_subreddit)
        print("plot")
        # self.emotion_posts_avg_of_subreddit = {}
        # self.emotion_dict = {}
        self.emotion_plot_for_all_posts_in_subreddit(date_format='%Y/%m', subreddit_name=subreddit_name,
                                                                  path_to_read_data=path_to_read_data,
                                                                  path_to_save_plt=path_to_save_plt,
                                                                  category=category)


if __name__ == '__main__':
    emotion_detection = EmotionDetection()
    file_reader = FileReader()
    Con_DB = Con_DB()
    data_cursor = Con_DB.get_cursor_from_mongodb(db_name='reddit',
             collection_name='wallstreetbets').find({}).limit(2000)

    emotion_avg_in_month = ["Disgust", "Others", "Anger", "Fear", "Surprise", "Sadness", "Joy"]

    PATH_DRIVE = os.getenv("OUTPUTS_DIR") + 'emotion_detection/'
    resource_path = PATH_DRIVE + 'resources/'
    plot_folder_path = PATH_DRIVE + 'plots/'

    emotion_detection.get_plot_and_emotion_rate_from_all_posts_in_category(data_cursor=data_cursor,
                       Con_DB=Con_DB,
                       path_to_read_data=resource_path,
                       path_to_save_plt=plot_folder_path,
                       category="Removed",
                       subreddit_name=os.getenv("COLLECTION_NAME"))
    #
    # emotions = ["Angry", "Fear", "Happy", "Sad", "Surprise"]
    # data_category_df = pd.read_csv(folder_path + "Removed_NER_emotion_rate_mean_wallstreetbets.csv")
    # not_removed_df = pd.read_csv(folder_path + "NotRemoved_NER_emotion_rate_mean_wallstreetbets.csv")
    # folder_path = 'C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\emotion_plots\\'
    # # all_df = pd.read_csv(folder_path + "All_NER_emotion_rate_mean_wallstreetbets.csv")
    #
    # r_entities_set = set([entity[1] for index, entity in data_category_df.iterrows()])
    # nr_entities_set = set([entity[1] for index, entity in not_removed_df.iterrows()])
    # entities_set = r_entities_set.intersection(nr_entities_set)
    #
    # for entity in entities_set:
    #     emotion_detection.emotion_plot_per_NER(date_format='%Y/%m', subreddit_name=os.getenv("COLLECTION_NAME"),
    #                                            NER=entity,
    #                                            path_to_save_plt=folder_path,
    #                                            removed_df=data_category_df.loc[data_category_df['entity'] == entity],
    #                                            not_removed_df=not_removed_df.loc[not_removed_df['entity'] == entity],
    #                                            # all_df=all_df.loc[all_df['entity'] == entity],
    #                                            emotions_list_category=emotions)
