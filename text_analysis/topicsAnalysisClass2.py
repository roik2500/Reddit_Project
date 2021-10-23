import ijson
import logging
import os
import re
import time
from wordcloud import WordCloud
import pandas as pd
import spacy
from gensim import corpora
import gensim
from gensim.models import CoherenceModel, LdaModel, LdaMulticore, TfidfModel
from matplotlib import pyplot as plt
from spacy.lang.en import English
import nltk
from nltk.corpus import wordnet as wn
from nltk.stem.wordnet import WordNetLemmatizer
import pickle
import pyLDAvis.gensim_models
import pyLDAvis
from textblob import TextBlob
from tqdm import tqdm
from db_utils.Con_DB import Con_DB
from dotenv import load_dotenv
import pathlib

load_dotenv()

spacy.load('en_core_web_sm')
nltk.download('wordnet', quiet=True)
nltk.download('stopwords', quiet=True)
# logging.getLogger().setLevel(logging.CRITICAL)
logging.basicConfig(format='%(asctime)s %(message)s')


def get_lemma(word):
    lemma = wn.morphy(word)
    if lemma is None:
        return word
    else:
        return lemma


def get_lemma2(word):
    return WordNetLemmatizer().lemmatize(word)


def tokenize(text):
    parser = English()
    lda_tokens = []
    tokens = parser(text)
    for token in tokens:
        if token.orth_.isspace():
            continue
        elif token.like_url:
            lda_tokens.append('URL')
        elif token.orth_.startswith('@'):
            lda_tokens.append('SCREEN_NAME')
        else:
            lda_tokens.append(token.lower_)
    return lda_tokens


def prepare_text_for_lda(text):
    en_stop = set(nltk.corpus.stopwords.words('english'))
    tokens = tokenize(text)
    tokens = [token for token in tokens if len(token) > 2]
    tokens = [token for token in tokens if token not in en_stop]
    tokens = [get_lemma(token) for token in tokens]
    return tokens


def convert_tuples_to_dict(tup):
    dic = {}
    for x, y in tup:
        dic.setdefault(y, []).append(x)
    return dic


class TopicsAnalysis:
    def __init__(self, src_name, rmovd_flag, prep_data, post_comment):
        self.limit = 30
        self.start = 20
        self.step = 5
        self.removed_flag = rmovd_flag
        self.subreddit = src_name
        self.post_comment = post_comment
        self.dir = "{}/{}/{}".format(os.getenv('OUTPUTS_DIR'), src_name, self.post_comment)
        self.src_name = src_name
        if self.removed_flag:
            self.dir += "/all"
        else:
            self.dir += "/removed"
        pathlib.Path(self.dir).mkdir(parents=True, exist_ok=True)
        self.perplexity_values = []
        self.coherence_values = []
        self.con_db = Con_DB()
        # bring data from source
        if prep_data:
            # self.data_cursor = self.con_db.get_cursor_from_mongodb(collection_name=src_name).find({})
            # self.data_cursor = self.con_db.get_cursor_from_json("wallstreetbets_2020_full_")
            self.prepare_data()
        self.corpus = 0
        self.dictionary = 0
        self.id_list = self.load_pickle("id_lst")
        self.text_data = self.load_pickle("text_data")
        self.id_list_month = convert_tuples_to_dict(self.id_list)

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
        id_lst, text_data = [], {}
        counter = 0
        # for k in range(1, 2):
        with open("G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/data/wallstreetbets_2020_full_.json", 'rb') as fh:
            with tqdm(total=500000) as pbar:
                # line = fh.readline()
                # self.con_db.setAUTH_DB(k)
                # self.data_cursor = self.con_db.get_cursor_from_mongodb(collection_name=self.src_name).find({})
                # for x in tqdm(self.data_cursor):
                start_pos = 0
                # parser = ijson.parse(fh)
                items = ijson.items(fh, 'item')
                for x in items:
                    # if self.removed_flag or self.con_db.is_removed(x, self.post_comment, "Removed"):
                    # try:
                    pbar.update(1)
                    # x = json.loads(line)
                    if counter == 200:
                        break
                    data_list = self.con_db.get_text_from_post_OR_comment(x, self.post_comment)
                    for d in data_list:
                        text, date, Id, is_removed = d
                        if self.removed_flag or is_removed:
                            month = int(date.split('-')[1])
                            id_lst.append((Id, month))
                            tokens = prepare_text_for_lda(text)
                            text_data.setdefault(month, []).append(tokens)
                            counter += 1
                    # line = fh.readline()


        pickle.dump(id_lst, open(self.dir+'/id_lst.pkl', 'wb'))
        pickle.dump(text_data, open(self.dir+'/text_data.pkl', 'wb'))
        for k in text_data:
            directory = self.dir+"/{}".format(k)
            pathlib.Path(directory).mkdir(parents=True, exist_ok=True)
            # if not os.path.exists(directory):
            #     os.mkdir(directory)
            dic = corpora.Dictionary(text_data[k])
            dic.save('{}/dictionary.gensim'.format(directory))
            corpus = [dic.doc2bow(text) for text in text_data[k]]
            tfidf = TfidfModel(corpus)
            corpus_tfidf = tfidf[corpus]
            pickle.dump(corpus, open('{}/corpus.pkl'.format(directory), 'wb'))
            pickle.dump(corpus_tfidf, open('{}/corpus_tfidf.pkl'.format(directory), 'wb'))
        directory = self.dir + "/general"
        pathlib.Path(directory).mkdir(parents=True, exist_ok=True)
        # if not os.path.exists(directory):
        #     os.mkdir(directory)
        dic = corpora.Dictionary(list(text_data.values())[0])
        corpus = [dic.doc2bow(text) for txt_data in list(text_data.values()) for text in txt_data]
        tfidf = TfidfModel(corpus)
        corpus_tfidf = tfidf[corpus]
        pickle.dump(corpus, open(directory+'/corpus.pkl', 'wb'))
        pickle.dump(corpus_tfidf, open('{}/corpus_tfidf.pkl'.format(directory), 'wb'))
        dic.save(directory+'/dictionary.gensim')

    def topics_exp(self, text_data, month_name):
        self.perplexity_values = []
        self.coherence_values = []
        best_num_of_topics, bst_coh, best_model, second_best_num_of_topics, second_best_model, second_best_coh = 0, 0, 0, 0, 0, 0
        lda_models = []
        for num_of_topic in tqdm(range(self.start, self.limit, self.step)):
            logging.info("{} topics model build: {}".format(month_name, num_of_topic))
            coherence_lda, ldamodel, perplexity_value = self.create_topics_model(num_of_topic, text_data, month_name)
            self.perplexity_values.append(perplexity_value)
            self.coherence_values.append(coherence_lda)
            lda_models.append((ldamodel, perplexity_value, num_of_topic))
            # if coherence_lda > bst_coh:
            #     second_best_coh = bst_coh
            #     second_best_num_of_topics = best_num_of_topics
            #     second_best_model = best_model
            #     bst_coh = coherence_lda
            #     best_num_of_topics = num_of_topic
            #     best_model = ldamodel
            # elif coherence_lda >= second_best_coh:
            #     second_best_num_of_topics = num_of_topic
            #     second_best_model = ldamodel
            #     second_best_coh = coherence_lda
        lda_models.sort(key=lambda x: x[1])
        for k, m in enumerate(lda_models):
            directory = self.dir+"/{}/{}".format(month_name, k)
            pathlib.Path(directory).mkdir(parents=True, exist_ok=True)
            m[0].save(
                '{}/model_{}topic_{}.gensim'.format(directory, m[2], month_name))

        # best_directory = self.dir+"/{}/best".format(month_name)
        # pathlib.Path(best_directory).mkdir(parents=True, exist_ok=True)
        # if not os.path.exists(best_directory):
        #     os.mkdir(best_directory)
        # second_directory = self.dir+"/{}/second_best".format(month_name)
        # pathlib.Path(second_directory).mkdir(parents=True, exist_ok=True)
        # if not os.path.exists(second_directory):
        #     os.mkdir(second_directory)
        self.extract_plots(month_name)
        # if type(second_best_model) == int:
        #     print(month_name, self.coherence_values)
        # else:
        # second_best_model.save(
        #     '{}/model_{}topc_second_{}.gensim'.format(second_directory, second_best_num_of_topics, month_name))
        # if type(best_model) == int:
        #     print(month_name, self.coherence_values)
        # else:
        # best_model.save('{}/model_{}topc_best_{}.gensim'.format(best_directory, best_num_of_topics, month_name))
        return lda_models

    def create_topics_model(self, num_of_topic, text_data, month):
        # print(num_of_topic)
        corp = 0
        corp = self.corpus
        dic = self.dictionary
        # if month == "general":
        #     corp = self.corpus
        #     dic = self.dictionary
        # else:
        #     corp = self.corpus_dict[month]
        #     dic = self.dictionary_dict[month]
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
        # if month_name_model == "general":
        #     corp = self.corpus
        # else:
        #     month_name_model = int(month.split('_')[0])
        #     corp = self.corpus_dict[month_name_model]
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
        sent_topics_df.to_csv(self.dir+"/{}/{}/document_topic_table_{}.csv".format(month_name_model, best_second, month))
        print(sent_topics_df.head(15))

    def extract_plots(self, month_name):
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
        plt.savefig(self.dir+"/{}/coherence_perplxity".format(month_name))

    def use_model(self, model, ids, mod_name):
        month_name_model, best_second = mod_name.split('-')
        corp = self.corpus
        dic = self.dictionary
        # if month_name_model == "general":
        #     corp = self.corpus
        #     dic = self.dictionary
        # else:
        #     month_name_model = int(mod_name.split('_')[0])
        #     corp = self.corpus_dict[month_name_model]
        #     dic = self.dictionary_dict[month_name_model]
        self.dominant_topics(ldamodel=model, month=mod_name, ids=ids)
        # Visualize the topics
        visualisation = pyLDAvis.gensim_models.prepare(model, corp, dic, sort_topics=False)
        try:
            pyLDAvis.save_html(visualisation, self.dir+'/{}/{}/LDA_Visualization_{}.html'.format(month_name_model, best_second, mod_name))
        except TypeError:
            print(mod_name)
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
            w_c_dir = self.dir+"/{}/{}/worldcloud".format(month_name_model, best_second)
            pathlib.Path(w_c_dir).mkdir(parents=True, exist_ok=True)
            # if not os.path.exists(w_c_dir):
            #     os.mkdir(w_c_dir)
            plt.savefig(w_c_dir+"/{}_worldcloud_{}".format(mod_name, topic_id))
        df = pd.DataFrame(data=sentiment_topics, columns=['topic_id', 'topic', 'sentim'])
        df.to_csv(self.dir+"/{}/{}/topic_sentim_{}.csv".format(month_name_model, best_second, mod_name))

    def load_models(self, k):
        modls = []
        for root, dirs, files in os.walk(self.dir+"/{}/".format(k)):
            for file in files:
                if regex_lda.match(file):
                    modls.append(LdaModel.load('{}/{}'.format(root, file)))
        return modls


def run_model(key, id_lst, txt_data):
    topics.load_dic_cor(str(key))
    if prepare_models:
        models = topics.topics_exp(txt_data, key)
    else:
        models = topics.load_models(key)
    logging.info("{} topics model using".format(key))
    for k, m in enumerate(models):
        topics.use_model(m, id_lst, str(key) + '-' + str(k))
    # topics.use_model(models[0], id_lst, str(key) + '-best')
    # topics.use_model(models[1], id_lst, str(key) + '-second_best')
# topics.id_list_month


if __name__ == "__main__":
    regex_lda = re.compile('(model*.*gensim$)')
    source_name, source_type = "wallstreetbets", "mongo"
    prepare_data = False  # if true load data from mongo and prapre it. else load dic and corp from disk
    prepare_models = False  # if true create models. else load models from disk
    post_comment_flag = "post"
    for i in range(1, 2):
        removed_flag = i  # if True its all data, if False its only the removed
        topics = TopicsAnalysis(source_name, removed_flag, prepare_data, post_comment_flag)
        rng = list(topics.id_list_month.keys())
        rng.append("general")
        for month_key in tqdm(rng):
            logging.info("{} topics model build".format(month_key))
            if month_key == 'general':
                ids_lst = topics.id_list
                txt_dt = list(topics.text_data.values())[0]
            else:
                ids_lst = topics.id_list_month[month_key]
                txt_dt = topics.text_data[month_key]
            run_model(month_key, ids_lst, txt_dt)
