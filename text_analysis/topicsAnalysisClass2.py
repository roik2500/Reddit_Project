import ijson
import logging
import os
import re
import time
from wordcloud import WordCloud
import pandas as pd
import spacy
from gensim.models import CoherenceModel, LdaModel, LdaMulticore, TfidfModel
from matplotlib import pyplot as plt
from textblob import TextBlob
from tqdm import tqdm
from db_utils.Con_DB import Con_DB
from dotenv import load_dotenv
import pathlib
from utills import *

load_dotenv()
regex_lda = re.compile('(model*.*gensim$)')
spacy.load('en_core_web_sm')
nltk.download('wordnet', quiet=True)
nltk.download('stopwords', quiet=True)
logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)s %(message)s')


class TopicsAnalysis:
    def __init__(self, src_name, rmovd_flag, prep_data, prep_dic_cor,  post_comment, start, limit, step):
        self.limit = limit
        self.start = start
        self.step = step
        self.removed_flag = rmovd_flag
        self.subreddit = src_name
        self.post_comment = post_comment
        self.dir = "{}/{}/{}".format(os.getenv('OUTPUTS_DIR'), src_name, self.post_comment)
        self.src_name = src_name
        if self.removed_flag:
            self.dir += "/all"
        else:
            self.dir += "/removed"
        print(self.dir)
        pathlib.Path(self.dir).mkdir(parents=True, exist_ok=True)
        self.perplexity_values = []
        self.coherence_values = []
        self.con_db = Con_DB()
        # bring data from source
        if prep_data:
            self.prepare_data()
        self.corpus = 0
        self.dictionary = 0
        self.id_list = self.load_pickle("id_lst")
        self.text_data = self.load_pickle("text_data")
        self.id_list_month = convert_tuples_to_dict(self.id_list)
        if prep_dic_cor:
            for k in self.text_data:
                directory = "{}/{}".format(self.dir, k)
                curr_text_data = self.text_data[k]
                dump_prepared_data_files(directory, curr_text_data)
            directory = "{}/general".format(self.dir)
            curr_text_data = [txt for txt_data in list(self.text_data.values()) for txt in txt_data]
            dump_prepared_data_files(directory, curr_text_data)

    def load_dic_cor(self, k):
        self.dictionary = corpora.Dictionary.load(self.dir + "/{}/dictionary.gensim".format(k))
        infile = open(self.dir + "/{}/corpus_tfidf.pkl".format(k), 'rb')
        self.corpus = pickle.load(infile)
        infile.close()

    def load_pickle(self, file_name):
        infile = open(self.dir + "/{}.pkl".format(file_name), 'rb')
        file = pickle.load(infile)
        infile.close()
        return file

    def prepare_data(self):
        id_lst, text_data_dict, curr_text_data = [], {}, []
        counter = 0
        items = self.con_db.get_data_cursor("wallstreetbets_2020_full_", 'json')
        with tqdm(total=500000) as pbar:
            for x in items:
                if counter == 200:
                    break
                pbar.update(1)
                data_list = self.con_db.get_text_from_post_OR_comment(x, self.post_comment)
                for d in data_list:
                    text, date, Id, is_removed = d
                    if self.removed_flag or is_removed:
                        month = int(date.split('-')[1])
                        id_lst.append((Id, month))
                        tokens = prepare_text_for_lda(text)
                        text_data_dict.setdefault(month, []).append(tokens)
                        counter += 1
        pickle.dump(id_lst, open(self.dir + '/id_lst.pkl', 'wb'))
        pickle.dump(text_data_dict, open(self.dir + '/text_data.pkl', 'wb'))
        return text_data_dict
        # for k in text_data_dict:
        #     directory = "{}/{}".format(self.dir, k)
        #     curr_text_data = text_data_dict[k]
        #     dump_prepared_data_files(directory, curr_text_data)
        # directory = "{}/general".format(self.dir)
        # curr_text_data = [txt for txt_data in list(text_data_dict.values()) for txt in txt_data]
        # dump_prepared_data_files(directory, curr_text_data)

    def topics_exp(self, text_data, month_name):
        self.perplexity_values = []
        self.coherence_values = []
        lda_models = []
        for num_of_topic in tqdm(range(self.start, self.limit, self.step)):
            logging.info("{} topics model build: {}".format(month_name, num_of_topic))
            coherence_lda, ldamodel, perplexity_value = self.create_topics_model(num_of_topic, text_data, month_name)
            self.perplexity_values.append(perplexity_value)
            self.coherence_values.append(coherence_lda)
            lda_models.append((ldamodel, perplexity_value, num_of_topic))
        lda_models.sort(key=lambda x: x[1])
        for k, m in enumerate(lda_models):
            directory = self.dir + "/{}/{}".format(month_name, k)
            pathlib.Path(directory).mkdir(parents=True, exist_ok=True)
            m[0].save(
                '{}/model_{}topic_{}.gensim'.format(directory, m[2], month_name))
        self.extract_coh_prex_plots(month_name)
        return lda_models

    def create_topics_model(self, num_of_topic, text_data, month):
        corp = self.corpus
        dic = self.dictionary
        ldamodel = LdaMulticore(corpus=corp, num_topics=num_of_topic, id2word=dic, passes=15)
        perplexity_value = ldamodel.log_perplexity(corp)
        # Compute Coherence Score
        coherence_model_lda = CoherenceModel(model=ldamodel, texts=text_data, dictionary=dic, coherence='c_v')
        coherence_lda = coherence_model_lda.get_coherence()
        return coherence_lda, ldamodel, perplexity_value

    def dominant_topics(self, ldamodel, month, ids=None):
        sent_topics_df = pd.DataFrame()
        month_name_model, best_second = month.split('-')
        corp = self.corpus
        for i, row in enumerate(ldamodel[corp]):
            row = sorted(row, key=lambda x: (x[1]), reverse=True)
            for j, (topic_num, prop_topic) in enumerate(row):
                if j == 0:  # => dominant topic
                    wp = ldamodel.show_topic(topic_num)
                    topic_keywords = ", ".join([word for word, prop in wp])
                    sent_topics_df = sent_topics_df.append(
                        pd.Series([int(topic_num), round(prop_topic, 4), topic_keywords]), ignore_index=True)
                else:
                    break
        sent_topics_df.columns = ['Dominant_Topic', 'Perc_Contribution', 'Topic_Keywords']
        contents = pd.DataFrame(ids)
        sent_topics_df = pd.concat([sent_topics_df, contents], axis=1)
        sent_topics_df.columns = [
            'Dominant_Topic', 'Topic_Perc_Contrib', 'Keywords', 'post_id', 'month'
        ] if len(sent_topics_df.columns) == 5 else ['Dominant_Topic', 'Topic_Perc_Contrib', 'Keywords', 'post_id']

        sent_topics_df = sent_topics_df.reset_index()
        sent_topics_df.to_csv(
            self.dir + "/{}/{}/document_topic_table_{}.csv".format(month_name_model, best_second, month))
        print(sent_topics_df.head(15))

    def extract_coh_prex_plots(self, month_name):
        x = range(self.start, self.limit, self.step)
        # Using built-in trigonometric function we can directly plot
        # the given cosine wave for the given angles
        Y1 = self.coherence_values
        Y2 = self.perplexity_values
        # Initialise the subplot function using number of rows and columns
        figure, axis = plt.subplots(2, 1)
        axis[0].plot(x, Y1)
        axis[0].set_ylabel("Coherence score")
        axis[1].plot(x, Y2)
        axis[1].set_ylabel("Perplexity score")
        plt.xlabel("Num Topics")
        plt.savefig(self.dir + "/{}/coherence_perplxity".format(month_name))

    def extract_model_outputs(self, model, ids, mod_name):
        month_name_model, best_second = mod_name.split('-')
        logging.info("month_name_model {}, best_second {}".format(month_name_model, best_second))
        self.dominant_topics(ldamodel=model, month=mod_name, ids=ids)
        # Visualize the topics
        # visualisation = pyLDAvis.gensim_models.prepare(model, corp, dic, sort_topics=False)
        # try:
        #   pyLDAvis.save_html(visualisation, self.dir+'/{}/{}/LDA_Visualization_{}.html'.format(month_name_model, best_second, mod_name))
        # except TypeError:
        #    print(mod_name)
        sentiment_topics = []
        plt.clf()
        for topic_id in tqdm(range(model.num_topics)):
            topk = model.show_topic(topic_id, 10)
            topk_words = [w for w, _ in topk]
            topic = '{}'.format(' '.join(topk_words))
            blob = TextBlob(topic)
            sentim = blob.sentiment.polarity
            print(topic, ": ", sentim)
            # Create and generate a word cloud image:
            joined_topk_words = " ".join(topk_words)
            wordcloud = WordCloud().generate(joined_topk_words)
            sentiment_topics.append([topic_id, topic, sentim])
            # Display the generated image:
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.axis("off")
            plt.title("topic #{} from {} with {} topics".format(topic_id, mod_name, model.num_topics))
            # plt.show()
            w_c_dir = self.dir + "/{}/{}/worldcloud".format(month_name_model, best_second)
            pathlib.Path(w_c_dir).mkdir(parents=True, exist_ok=True)
            # if not os.path.exists(w_c_dir):
            #     os.mkdir(w_c_dir)
            plt.savefig(w_c_dir + "/{}_worldcloud_{}".format(mod_name, topic_id))
        df = pd.DataFrame(data=sentiment_topics, columns=['topic_id', 'topic', 'sentim'])
        df.to_csv(self.dir + "/{}/{}/topic_sentim_{}.csv".format(month_name_model, best_second, mod_name))

    def load_models(self, k):
        models = []
        for root, dirs, files in os.walk(self.dir + "/{}/".format(k)):
            for file in files:
                if regex_lda.match(file):
                    models.append(LdaModel.load('{}/{}'.format(root, file)))
        return models

    def use_models(self, key, id_lst, models):
        logging.info("{} topics model using".format(key))
        for k, m in enumerate(models):
            self.use_models(m, id_lst, str(key) + '-' + str(k))

    def create_or_load_model(self, key, prepare_models, txt_data):
        self.load_dic_cor(str(key))
        if prepare_models:
            models = self.topics_exp(txt_data, key)
        else:
            models = self.load_models(key)
        # logging.info("{} topics model using".format(key))
        return models
