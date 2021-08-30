import pandas as pd
import pymongo
import spacy
from gensim import corpora
import gensim
from gensim.models import CoherenceModel, LdaModel
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

con_db = Con_DB()
spacy.load('en_core_web_sm')
parser = English()
nltk.download('wordnet')
nltk.download('stopwords')
en_stop = set(nltk.corpus.stopwords.words('english'))


def tokenize(text):
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


def get_lemma(word):
    lemma = wn.morphy(word)
    if lemma is None:
        return word
    else:
        return lemma


def get_lemma2(word):
    return WordNetLemmatizer().lemmatize(word)


def prepare_text_for_lda(text):
    tokens = tokenize(text)
    tokens = [token for token in tokens if len(token) > 4]
    tokens = [token for token in tokens if token not in en_stop]
    tokens = [get_lemma(token) for token in tokens]
    return tokens


def topic_analysis(src_name, src_type):
    text_data = []
    limit = 90
    start = 288322
    step = 6
    coherence_values = []
    perplexity_values = []
    best_num_of_topics = 0
    bst_coh = 0
    best_model = 0
    data = data_access(src_name, src_type)
    id_list = []
    for x in tqdm(data):
        x = x["pushift_api"]
        id_list.append(x["pushift_api"]["id"])
        tokens = prepare_text_for_lda(x["title"])
        if "selftext" in x and not x["selftext"].__contains__("[removed]") and x[
            "selftext"] != "[deleted]":
            tokens.extend(prepare_text_for_lda(x["selftext"]))
        text_data.append(tokens)
    dictionary = corpora.Dictionary(text_data)
    corpus = [dictionary.doc2bow(text) for text in text_data]
    pickle.dump(corpus, open('../outputs/corpus.pkl', 'wb'))
    dictionary.save('../outputs/dictionary.gensim')
    for num_of_topic in tqdm(range(start, limit, step)):
        # print(num_of_topic)
        ldamodel = gensim.models.ldamodel.LdaModel(corpus, num_topics=num_of_topic, id2word=dictionary, passes=15)
        perplexity_value = ldamodel.log_perplexity(corpus)
        # print('\nPerplexity: ', perplexity_value)  # a measure of how good the model is. lower the better.
        perplexity_values.append(perplexity_value)
        # Compute Coherence Score
        coherence_model_lda = CoherenceModel(model=ldamodel, texts=text_data, dictionary=dictionary, coherence='c_v')
        coherence_lda = coherence_model_lda.get_coherence()
        if coherence_lda > bst_coh:
            bst_coh = coherence_lda
            best_num_of_topics = num_of_topic
            best_model = ldamodel
        coherence_values.append(coherence_lda)
        # print('\nCoherence Score: ', coherence_lda)
    best_model.save('../outputs/model{}.gensim'.format(best_num_of_topics))
    extract_plots(coherence_values, limit, perplexity_values, start, step)
    dominant_topics(ldamodel=best_model, corpus=corpus, ids=id_list)
    return best_model, corpus, dictionary


def data_access(source_name_, source_type_):
    if source_type_ == "mongo":
        # mycol = con_db.get_posts_from_mongodb(collection_name=source_name_)
        myclient = pymongo.MongoClient("mongodb+srv://shimon:1234@redditdata.aav2q.mongodb.net/")
        mydb = myclient["reddit"]
        mycol = mydb[source_name_]
        data_ = mycol.find()
        # data_ = mycol.find({}, {"title": 1, "selftext": 1, "subreddit": 1})
    elif source_type_ == "csv":
        data_ = pd.read_csv("../data/{}.csv".format(source_name_))
        data_ = data_[data_["is_crosspostable"] == False]
    return data_


def dominant_topics(ldamodel, corpus, ids):
    sent_topics_df = pd.DataFrame()
    for i, row in enumerate(ldamodel[corpus]):
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
    contents = pd.Series(ids)
    sent_topics_df = pd.concat([sent_topics_df, contents], axis=1)
    sent_topics_df.columns = [
        'Dominant_Topic', 'Topic_Perc_Contrib', 'Keywords', 'post_id'
    ]
    sent_topics_df = sent_topics_df.reset_index()
    sent_topics_df.to_csv("../outputs/document_topic_table.csv")
    print(sent_topics_df.head(15))


def extract_plots(coherence_values, limit, perplexity_values, start, step):
    x = range(start, limit, step)
    fig, axs = plt.subplots(2)
    axs[0].plot(x, coherence_values)
    plt.xlabel("Num Topics")
    plt.ylabel("Coherence score")
    plt.legend(("coherence_values"), loc='best')
    axs[1].plot(x, perplexity_values)
    plt.xlabel("Num Topics")
    plt.ylabel("Perplexity score")
    plt.legend(("Perplexity_values"), loc='best')
    plt.savefig("../outputs/coherence_perplxity")


if __name__ == "__main__":
    flag = True  # true if the model already built
    source_name, source_type = "wallstreetbets", "mongo"
    best_model_path = "../outputs/model80.gensim"
    if flag:
        id_list = []
        data = data_access(source_name, source_type)
        for x in data:
            id_list.append(x["pushift_api"]["id"])
        lda_model = LdaModel.load(best_model_path)
        infile = open("../outputs/corpus.pkl", 'rb')
        corpus = pickle.load(infile)
        infile.close()
        dictionary = corpora.Dictionary.load("../outputs/dictionary.gensim")
        topics = lda_model.print_topics(num_words=4)
        dominant_topics(ldamodel=lda_model, corpus=corpus, ids=id_list)
    else:
        lda_model, corpus, dictionary = topic_analysis(source_name, source_type)

    # Visualize the topics
    visualisation = pyLDAvis.gensim_models.prepare(lda_model, corpus, dictionary)
    pyLDAvis.save_html(visualisation, '../outputs/LDA_Visualization.html')

    # for topic_id in range(loaded_lda_model.num_topics):
    #     topk = loaded_lda_model.show_topic(topic_id, 10)
    #     topk_words = [w for w, _ in topk]
    #     topic = '{}'.format(' '.join(topk_words))
    #     blob = TextBlob(topic)
    #     print(topic,": ",blob.sentiment.polarity)
