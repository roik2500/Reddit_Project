import os
from statistics import mean
from db_utils.Con_DB import Con_DB
from dotenv import load_dotenv
from datetime import datetime
from datetime import date
import json
import re
import matplotlib.pyplot as plt

from decimal import Decimal
from tqdm import tqdm

# from transformers import BertTokenizer
# from transformers import pipeline
from pprint import pprint
# model_name = "monologg/bert-base-cased-goemotions-ekman"
# classifier = pipeline("text-classification",model=model_name, return_all_scores=True)
# prediction = classifier("I am happy", )
# pprint(prediction)

from transformers import BertTokenizer
from GoEmotions_pytorch.model import BertForMultiLabelClassification
from GoEmotions_pytorch.multilabel_pipeline import MultiLabelPipeline
import torch.nn as nn
from transformers import BertPreTrainedModel, BertModel
from pprint import pprint
import numpy as np

from db_utils.FileReader import FileReader

import torch

# from pysentimiento import EmotionAnalyzer

'''
@inproceedings{demszky2020goemotions,
 author = {Demszky, Dorottya and Movshovitz-Attias, Dana and Ko, Jeongwoo and Cowen, Alan and Nemade, Gaurav and Ravi, Sujith},
 booktitle = {58th Annual Meeting of the Association for Computational Linguistics (ACL)},
 title = {{GoEmotions: A Dataset of Fine-Grained Emotions}},
 year = {2020}
}
'''

load_dotenv()
torch.cuda.is_available()
'''
[[{'label': 'admiration', 'score': 0.00017094481154344976},
  {'label': 'amusement', 'score': 2.571632830949966e-05},
  {'label': 'anger', 'score': 1.4369928976520896e-05},
  {'label': 'annoyance', 'score': 1.1100154551968444e-05},
  {'label': 'approval', 'score': 3.581155033316463e-05},
  {'label': 'caring', 'score': 3.9640905015403405e-05},
  {'label': 'confusion', 'score': 1.3769921679340769e-05},
  {'label': 'curiosity', 'score': 7.811759132891893e-06},
  {'label': 'desire', 'score': 6.056168786017224e-06},
  {'label': 'disappointment', 'score': 6.6568572947289795e-06},
  {'label': 'disapproval', 'score': 1.2706344023172278e-05},
  {'label': 'disgust', 'score': 7.178270152508048e-06},
  {'label': 'embarrassment', 'score': 2.580308546384913e-06},
  {'label': 'excitement', 'score': 7.431762787746266e-05},
  {'label': 'fear', 'score': 3.6119192827754887e-06},
  {'label': 'gratitude', 'score': 1.776665885699913e-05},
  {'label': 'grief', 'score': 4.1310436245112214e-06},
  {'label': 'joy', 'score': 0.9993305206298828},
  {'label': 'love', 'score': 6.10205352131743e-05},
  {'label': 'nervousness', 'score': 2.639161948536639e-06},
  {'label': 'optimism', 'score': 1.6253150533884764e-05},
  {'label': 'pride', 'score': 5.021602646593237e-06},
  {'label': 'realization', 'score': 1.616420559003018e-05},
  {'label': 'relief', 'score': 3.253559043514542e-05},
  {'label': 'remorse', 'score': 1.3855061524736811e-06},
  {'label': 'sadness', 'score': 3.7760148643428693e-06},
  {'label': 'surprise', 'score': 3.310600732220337e-06},
  {'label': 'neutral', 'score': 7.317437120946124e-05}]]

'''


class BertForMultiLabelClassification(BertPreTrainedModel):
    def __init__(self, config):
        super().__init__(config)
        self.num_labels = config.num_labels

        self.bert = BertModel(config)
        self.dropout = nn.Dropout(config.hidden_dropout_prob)
        self.classifier = nn.Linear(config.hidden_size, self.config.num_labels)
        self.loss_fct = nn.BCEWithLogitsLoss()

        self.init_weights()

    def forward(
            self,
            input_ids=None,
            attention_mask=None,
            token_type_ids=None,
            position_ids=None,
            head_mask=None,
            inputs_embeds=None,
            labels=None,
    ):
        outputs = self.bert(
            input_ids,
            attention_mask=attention_mask,
            token_type_ids=token_type_ids,
            position_ids=position_ids,
            head_mask=head_mask,
            inputs_embeds=inputs_embeds,
        )
        pooled_output = outputs[1]

        pooled_output = self.dropout(pooled_output)
        logits = self.classifier(pooled_output)

        outputs = (logits,) + outputs[2:]  # add hidden states and attention if they are here

        if labels is not None:
            loss = self.loss_fct(logits, labels)
            outputs = (loss,) + outputs

        return outputs  # (loss), logits, (hidden_states), (attentions)


class EmotionDetection:
    def __init__(self):
        self.emotion_dict = {}
        # self.emotion_posts_avg_of_subreddit = {'admiration': {}, 'amusement': {}, 'Anger': {}, 'annoyance': {},
        #                                        'approval': {}, 'caring': {}, 'confusion': {}, 'curiosity': {},
        #                                        'desire': {}, 'disappointment': {}, 'disapproval': {}, 'Disgust': {},
        #                                        'embarrassment': {}, 'excitement': {}, 'Fear': {}, 'gratitude': {},
        #                                        'love': {}, 'nervousness': {}, 'optimism': {}, 'pride': {}, 'Joy': {},
        #                                        'realization': {}, 'relief': {}, 'remorse': {}, 'Sadness': {},
        #                                        'grief': {}, 'Surprise': {}, 'Neutral': {}}
        self.emotion_posts_avg_of_subreddit = {'Anger': {}, 'Disgust': {}, 'Fear': {}, 'Joy': {},
                                               'Sadness': {}, 'Surprise': {}, 'Neutral': {}}

        model_name = "monologg/bert-base-cased-goemotions-ekman"
        tokenizer = BertTokenizer.from_pretrained(model_name)
        model = BertForMultiLabelClassification.from_pretrained(model_name)

        # goemotions = MultiLabelPipeline(
        #     model=model,
        #     tokenizer=tokenizer,
        #     threshold=0.3
        # )
        self.emotion_analyzer = MultiLabelPipeline(
            model=model,
            tokenizer=tokenizer,
            threshold=0.1
        )
        # model_name = "monologg/bert-base-cased-goemotions-ekman"
        # self.emotion_analyzer = pipeline("text-classification", model=model_name, return_all_scores=True)

    def extract_posts_emotion_rate(self, posts, con_DB, post_need_to_extract, category,
                                   post_or_comment_arg):  # posts format: [title + selftext, date, id]
        self.emotion_dict = {}
        for post in tqdm(posts):
            if post_need_to_extract:
                items = con_DB.get_text_from_post_OR_comment(_object=post, post_or_comment=post_or_comment_arg)
            else:
                items = post
            for item in items:
                if category == "Removed":
                    if not item[-1]:
                        break
                elif category == "NotRemoved":
                    if item[-1]:
                        break
                year = int(datetime.strptime(item[1]
                                             , "%Y-%m-%d").date().year)
                month = int(datetime.strptime(item[1]
                                              , "%Y-%m-%d").date().month)
                # day = int(datetime.strptime(item[1]
                #                             , "%Y-%m-%d").date().day)
                date_key = str(year) + "/" + str(month)  # + "/" + str(day)

                self.insert_rates_to_emotion_dict(date_key, item[2], item[0])

    def insert_rates_to_emotion_dict(self, date_key, reddit_id, tokens):
        try:
            if date_key in self.emotion_dict.keys():
                self.emotion_dict[date_key].append([reddit_id, self.get_post_emotion_rate(tokens)])
            else:
                self.emotion_dict[date_key] = [[reddit_id, self.get_post_emotion_rate(tokens)]]
        except:
            print("Something went wrong - post  id  {}". format(reddit_id))

    def mean_emotion_analyzer(self, old_emotion, new_emotion):
        if old_emotion == 0:
            return new_emotion
        else:
            for index in range(len(new_emotion)):
                label, rate = new_emotion[index].values()
                if label == old_emotion[index]['label']:
                    old_emotion[index]['score'] = mean([old_emotion[index]['score'], rate])
            return old_emotion

    def add_new_emotion_rate_to_dict(self, old_emotion_dict, new_emotion_dict):
        for new_emotion_type in new_emotion_dict:
            if new_emotion_type['label'] in old_emotion_dict.keys():
                old_emotion_dict[new_emotion_type['label']].append(new_emotion_type['score'])
        return old_emotion_dict

    # def get_post_emotion_rate(self, text):
    #     res = 0
    #     n = len(text)
    #     temp_dict_rate = {'anger': [], 'disgust': [], 'fear': [], 'joy': [],
    #                       'sadness': [], 'surprise': [], 'neutral': []}
    #     if n > 50:
    #         i=0
    #         for i in range(50, n, 50):
    #             result = self.emotion_analyzer(text[i-50:i])[0]
    #             temp_dict_rate = self.add_new_emotion_rate_to_dict(temp_dict_rate, result)
    #             # res = self.mean_emotion_analyzer(res, result)
    #         if i < n:
    #             result = self.emotion_analyzer(text[i:n-1])[0]
    #             temp_dict_rate = self.add_new_emotion_rate_to_dict(temp_dict_rate, result)
    #             # res = self.mean_emotion_analyzer(res, result)
    #         for obj in result:
    #             emotion_type = obj['label']
    #
    #             obj['score'] = mean(temp_dict_rate[emotion_type])
    #
    #             # temp_dict_rate[key] = mean(val)
    #     else:
    #         result = self.emotion_analyzer(text)
    #     return result

    def get_post_emotion_rate(self, text):
        res = 0
        text = text.replace('\n', ' ')
        text = text.replace('\'', '')
        text = text.replace('\"', '')
        n = len(text)
        # if n == 1345:
        #     return
        # temp_dict_rate = {'anger': [], 'disgust': [], 'fear': [], 'joy': [],
        #                   'sadness': [], 'surprise': [], 'neutral': []}
        temp_dict_rate = {}
        texts = []
        if n > 1250:
            i = 0
            for i in range(1250, n, 1250):
                texts.append(text[i - 1250:i])
                # result = self.emotion_analyzer(text[i-50:i])[0]
                # temp_dict_rate = self.add_new_emotion_rate_to_dict(temp_dict_rate, result)
                # res = self.mean_emotion_analyzer(res, result)
            if i < n:
                texts.append(text[i:n - 1])
                # result = self.emotion_analyzer(text[i:n-1])[0]
                # temp_dict_rate = self.add_new_emotion_rate_to_dict(temp_dict_rate, result)
                # res = self.mean_emotion_analyzer(res, result)
            # print('text ->', text)
            # print(n)
            # print('texts', texts)
            # try:
            result = self.emotion_analyzer(texts)
            # except:
            #     print("An exception occurred")
        else:
            result = self.emotion_analyzer(text)

        for obj in result:
            labels_socres_dict = dict(zip(obj['labels'], obj['scores']))
            # [temp_dict_rate[label].append(score) for label, score in labels_socres_dict.items() if score != []]
            for label, score in labels_socres_dict.items():
                # if score != []:
                if label in temp_dict_rate.keys():
                    temp_dict_rate[label].append(score)
                else:
                    temp_dict_rate[label] = [score]
            # temp_dict_rate.update(dict(zip(obj['labels'], [obj['scores']])))
        #     for index_label in range(len(obj['labels'])):
        #         emotion_type = obj['labels'][index_label]
        #         emotion_rate = obj['scores'][index_label]
        #         temp_dict_rate[emotion_type].append(emotion_rate)
        # obj['score'] = mean(temp_dict_rate[emotion_type])
        # temp_dict_rate[key] = mean(val)
        for key, val in temp_dict_rate.items():
            if val != []:
                temp_dict_rate[key] = mean(val)

        return temp_dict_rate

    def normaliztion(self, *argv):
        lst = []
        for arg in argv:
            lst.append(arg)
        period = len(lst[0])
        norm_numbers = [[], [], [], [], [], []]
        # month_lst = []
        for month in range(period):
            month_lst = [lst[i][month] for i in range(len(lst))]
            # for i in range(len(lst)):
            #     month_lst.append(lst[i][month])
            for index in range(len(lst)):
                norm_numbers[index].append(month_lst[index] / sum(month_lst))
        return norm_numbers

    # date_format  = '%Y/%m' or  "%Y-%m-%d"
    def emotion_plot_for_posts_in_subreddit(self, date_format, subreddit_name, NER, path_to_save_plt, category):

        x1 = sorted([*self.emotion_posts_avg_of_subreddit["Disgust"]], key=lambda t: datetime.strptime(t, date_format))
        x2 = sorted([*self.emotion_posts_avg_of_subreddit["Neutral"]], key=lambda t: datetime.strptime(t, date_format))
        x3 = sorted([*self.emotion_posts_avg_of_subreddit["Anger"]], key=lambda t: datetime.strptime(t, date_format))
        x4 = sorted([*self.emotion_posts_avg_of_subreddit["Fear"]], key=lambda t: datetime.strptime(t, date_format))
        x5 = sorted([*self.emotion_posts_avg_of_subreddit["Surprise"]], key=lambda t: datetime.strptime(t, date_format))
        x6 = sorted([*self.emotion_posts_avg_of_subreddit["Sadness"]], key=lambda t: datetime.strptime(t, date_format))
        x7 = sorted([*self.emotion_posts_avg_of_subreddit["Joy"]], key=lambda t: datetime.strptime(t, date_format))

        y1 = [*self.emotion_posts_avg_of_subreddit["Disgust"].values()]
        y2 = [*self.emotion_posts_avg_of_subreddit["Neutral"].values()]
        y3 = [*self.emotion_posts_avg_of_subreddit["Anger"].values()]
        y4 = [*self.emotion_posts_avg_of_subreddit["Fear"].values()]
        y5 = [*self.emotion_posts_avg_of_subreddit["Surprise"].values()]
        y6 = [*self.emotion_posts_avg_of_subreddit["Sadness"].values()]
        y7 = [*self.emotion_posts_avg_of_subreddit["Joy"].values()]

        # y1, y3, y4, y5, y6, y7 = self.normaliztion(y1, y3, y4, y5, y6, y7)

        # plot lines
        plt.ylim(0, 1)
        plt.xticks(rotation=90)
        plt.plot(x1, y1, label="Disgust", linestyle="-")
        plt.plot(x2, y2, label="Neutral", linestyle="--")
        plt.plot(x3, y3, label="Anger", linestyle="-.")
        plt.plot(x4, y4, label="Fear", linestyle=":")
        plt.plot(x5, y5, label="Surprise", linestyle="-")
        plt.plot(x6, y6, label="Sadness", linestyle="-")
        plt.plot(x7, y7, label="Joy", linestyle="-")

        plt.title('Emotion Detection to ' + subreddit_name + "-" + NER + "-" + category)
        plt.ylabel("Emotion rate")
        plt.xlabel("Time (month)")
        plt.legend()

        # if re.match("^[A-Za-z0-9_-]*$", NER):
        plt.savefig(path_to_save_plt + '_' + NER + '_' + datetime.now().strftime("%H-%M-%S") + ".jpg",
                    transparent=True)
        plt.show()
        plt.clf()

    # date_format  = '%Y/%m' or  "%Y-%m-%d"
    def emotion_plot_for_all_posts_in_subreddit(self, date_format, subreddit_name, path_to_save_plt, category):

        x1 = sorted([*self.emotion_posts_avg_of_subreddit["Disgust"]], key=lambda t: datetime.strptime(t, date_format))
        x2 = sorted([*self.emotion_posts_avg_of_subreddit["Neutral"]], key=lambda t: datetime.strptime(t, date_format))
        x3 = sorted([*self.emotion_posts_avg_of_subreddit["Anger"]], key=lambda t: datetime.strptime(t, date_format))
        x4 = sorted([*self.emotion_posts_avg_of_subreddit["Fear"]], key=lambda t: datetime.strptime(t, date_format))
        x5 = sorted([*self.emotion_posts_avg_of_subreddit["Surprise"]], key=lambda t: datetime.strptime(t, date_format))
        x6 = sorted([*self.emotion_posts_avg_of_subreddit["Sadness"]], key=lambda t: datetime.strptime(t, date_format))
        x7 = sorted([*self.emotion_posts_avg_of_subreddit["Joy"]], key=lambda t: datetime.strptime(t, date_format))

        y1 = [*self.emotion_posts_avg_of_subreddit["Disgust"].values()]
        y2 = [*self.emotion_posts_avg_of_subreddit["Neutral"].values()]
        y3 = [*self.emotion_posts_avg_of_subreddit["Anger"].values()]
        y4 = [*self.emotion_posts_avg_of_subreddit["Fear"].values()]
        y5 = [*self.emotion_posts_avg_of_subreddit["Surprise"].values()]
        y6 = [*self.emotion_posts_avg_of_subreddit["Sadness"].values()]
        y7 = [*self.emotion_posts_avg_of_subreddit["Joy"].values()]

        # y1, y3, y4, y5, y6, y7 = self.normaliztion(y1, y3, y4, y5, y6, y7)

        # plot lines
        plt.ylim(0, 1)
        plt.xticks(rotation=90)
        plt.plot(x1, y1, label="Disgust", linestyle="-")
        plt.plot(x2, y2, label="Neutral", linestyle="--")
        plt.plot(x3, y3, label="Anger", linestyle="-.")
        plt.plot(x4, y4, label="Fear", linestyle=":")
        plt.plot(x5, y5, label="Surprise", linestyle="-")
        plt.plot(x6, y6, label="Sadness", linestyle="-")
        plt.plot(x7, y7, label="Joy", linestyle="-")

        plt.title('Emotion Detection on posts to ' + subreddit_name + ' subreddit ' + "-" + category)
        plt.ylabel("Emotion rate")
        plt.xlabel("Time (month)")
        plt.legend()
        plt.savefig(path_to_save_plt + '_' + subreddit_name + '_' + category +
                    '_' + datetime.now().strftime("%H-%M-%S") + ".jpg", transparent=True)
        # plt.show()

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

            y1_removed, y2_not_removed, y3_interseting_point = self.normaliztion(y1_removed, y2_not_removed,
                                                                                 y3_interseting_point)
            # y3_all = [round(Decimal(key_date .split(', ')[1]), 2) for key_date  in all_sorted]

            # plot lines
            fig, ax = plt.subplots()

            # ax.plot_date(x, y, markerfacecolor='CornflowerBlue', markeredgecolor='white')
            ax.set_ylim([0.0, max(*y1_removed, *y2_not_removed) + Decimal(0.04)])
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
        # emotion_index = {'anger': 1, 'disgust': 2, 'fear': 3, 'joy': 4,
        #                   'sadness': 5, 'surprise': 6, 'neutral': 7}
        for emotion in self.emotion_posts_avg_of_subreddit.keys():
            scores = []
            for key, month_posts_emotions in self.emotion_dict.items():
                for id, emotion_rate in month_posts_emotions:
                    # if emotion_rate != 0:
                    #     if len(emotion_rate) > 1:
                    #         emotion_rate = [emotion_rate]
                    if emotion_rate is not None:
                        for label, score in emotion_rate.items():
                            # label, score = label_score.values()
                            if label.lower() == emotion.lower():
                                scores.append(score)
                                break
                if len(scores) > 0:
                    self.emotion_posts_avg_of_subreddit[emotion][key] = mean(scores)
                # self.emotion_posts_avg_of_subreddit[emotion][key] = \
                #     mean([emotion_rate[1].probas[emotion.lower()] for emotion_rate in month_posts_emotions])

    def get_plot_and_emotion_rate_from_all_posts_in_category(self, data_cursor, Con_DB, post_need_to_extract,
                                                             path_to_save_plt, file_reader, resources, category,
                                                             subreddit_name, post_or_comment_arg):
        print("extract emotion rate")
        self.extract_posts_emotion_rate(posts=data_cursor, con_DB=Con_DB, post_need_to_extract=post_need_to_extract,
                                        category=category, post_or_comment_arg=post_or_comment_arg)
        # has_removed=False -> get data that the selftext of post are removed
        #
        print("MEAN")
        self.calculate_post_emotion_rate_mean()


        print("plot")
        self.emotion_plot_for_all_posts_in_subreddit(date_format='%Y/%m', subreddit_name=subreddit_name,
                                                     path_to_save_plt=path_to_save_plt,
                                                     category=category)
        print("write to disk")
        file_name = 'every_{}_{}s_emotion_rate_{}'.format(category, post_or_comment_arg, subreddit_name)
        file_reader.write_dict_to_json(path=resources,
                                       file_name=file_name,
                                       dict_to_write=self.emotion_posts_avg_of_subreddit)
        self.emotion_dict = {}
        # self.emotion_posts_avg_of_subreddit = {'admiration': {}, 'amusement': {}, 'Anger': {}, 'annoyance': {},
        #                                        'approval': {}, 'caring': {}, 'confusion': {}, 'curiosity': {},
        #                                        'desire': {}, 'disappointment': {}, 'disapproval': {}, 'Disgust': {},
        #                                        'embarrassment': {}, 'excitement': {}, 'Fear': {}, 'gratitude': {},
        #                                        'love': {}, 'nervousness': {}, 'optimism': {}, 'pride': {}, 'Joy': {},
        #                                        'realization': {}, 'relief': {}, 'remorse': {}, 'Sadness': {},
        #                                        'grief': {}, 'Surprise': {}, 'Neutral': {}}

        self.emotion_posts_avg_of_subreddit = {'Anger': {}, 'Disgust': {}, 'Fear': {}, 'Joy': {},
                                               'Sadness': {}, 'Surprise': {}, 'Neutral': {}}


if __name__ == '__main__':
    emotion_detection_removed = EmotionDetection()
    con_db = Con_DB()
    file_reader = FileReader()
    COLLECTION_NAME = os.getenv("COLLECTION_NAME")
    data_cursor = con_db.get_cursor_from_mongodb(db_name='reddit', collection_name=COLLECTION_NAME).find({})
    removed_plots_folder_path = "C:\\Users\\User\\Documents\\FourthYear\\Project"
    emotion_detection_removed.get_plot_and_emotion_rate_from_all_posts_in_category(data_cursor=data_cursor,
                                                                                   Con_DB=con_db,
                                                                                   path_to_save_plt=removed_plots_folder_path,
                                                                                   category="All",
                                                                                   resources='',
                                                                                   subreddit_name=COLLECTION_NAME,
                                                                                   file_reader=file_reader,
                                                                                   post_need_to_extract=True,
                                                                                   post_or_comment_arg='post')
