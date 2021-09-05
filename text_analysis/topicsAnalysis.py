from time import sleep

from wordcloud import WordCloud
import pandas as pd
import pymongo
import spacy
from gensim import corpora
import gensim
from gensim.models import CoherenceModel, LdaModel
from matplotlib import pyplot as plt, gridspec
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

# con_db = Con_DB()
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
    limit = 13
    start = 2
    step = 6
    coherence_values = []
    perplexity_values = []
    best_num_of_topics = 0
    bst_coh = 0
    best_model = 0
    second_best_num_of_topics = 0
    second_bst_coh = 0
    second_best_model = 0
    data = con_db.get_cursor_from_mongodb(collection_name=source_name).find({})
    id_list = []

    for x in tqdm(data):
        x_reddit = x["reddit_api"]["post"]
        x_pushift = x["pushift_api"]
        # if "selftext" in x_reddit and x_reddit["selftext"].__contains__("[removed]"):
        id_list.append(x["post_id"])
        tokens = prepare_text_for_lda(x_pushift["title"])
        month = int(x_reddit["created_utc"][0].split('-')[1])
        if "selftext" in x_pushift and not x_pushift["selftext"].__contains__("[removed]") and x_pushift[
            "selftext"] != "[deleted]":
            tokens.extend(prepare_text_for_lda(x_pushift["selftext"]))
        text_data.append((tokens, month))
    dic = corpora.Dictionary(text_data)
    corpuss = [dic.doc2bow(text[0]) for text in text_data]
    pickle.dump(corpuss, open('../outputs/corpus.pkl', 'wb'))
    dic.save('../outputs/dictionary.gensim')
    for num_of_topic in tqdm(range(start, limit, step)):
        # print(num_of_topic)
        ldamodel = gensim.models.ldamodel.LdaModel(corpuss, num_topics=num_of_topic, id2word=dic, passes=15)
        perplexity_value = ldamodel.log_perplexity(corpuss)
        # print('\nPerplexity: ', perplexity_value)  # a measure of how good the model is. lower the better.
        perplexity_values.append(perplexity_value)
        # Compute Coherence Score
        coherence_model_lda = CoherenceModel(model=ldamodel, texts=text_data, dictionary=dic, coherence='c_v')
        coherence_lda = coherence_model_lda.get_coherence()
        if coherence_lda > bst_coh:
            second_bst_coh = bst_coh
            second_best_num_of_topics = best_num_of_topics
            second_best_model = best_model
            bst_coh = coherence_lda
            best_num_of_topics = num_of_topic
            best_model = ldamodel
        coherence_values.append(coherence_lda)
        # print('\nCoherence Score: ', coherence_lda)
    best_model.save('../outputs/model{}_best.gensim'.format(best_num_of_topics))
    extract_plots(coherence_values, limit, perplexity_values, start, step)
    dominant_topics(ldamodel=best_model, corpus=corpuss, ids=id_list)
    second_best_model.save('../outputs/model{}_second.gensim'.format(second_best_num_of_topics))
    dominant_topics(ldamodel=second_best_model, corpus=corpuss, ids=id_list)
    return best_model, corpuss, dic

#
# def data_access(source_name_, source_type_):
#     if source_type_ == "mongo":
#         # mycol = con_db.get_posts_from_mongodb(collection_name=source_name_)
#         myclient = pymongo.MongoClient("mongodb+srv://shimon:d6*E5GixFv!8SLX@cluster0.vfpai.mongodb.net/test")
#         mydb = myclient["reddit"]
#         mycol = mydb[source_name_]
#         data_ = mycol.find()
#         data_ = mycol.find({})
#     elif source_type_ == "csv":
#         data_ = pd.read_csv("../data/{}.csv".format(source_name_))
#         data_ = data_[data_["is_crosspostable"] == False]
#     return data_


def dominant_topics(ldamodel, corpus, ids=None):
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

    # Using built-in trigonometric function we can directly plot
    # the given cosine wave for the given angles
    Y1 = coherence_values
    Y2 = perplexity_values

    # Initialise the subplot function using number of rows and columns
    figure, axis = plt.subplots(2, 1)

    axis[0].plot(x, Y1)
    axis[0].set_ylabel("Coherence score")

    axis[1].plot(x, Y2)
    axis[1].set_ylabel("Perplexity score")
    plt.xlabel("Num Topics")


    # Combine all the operations and display
    # fig = plt.figure()
    # gs = gridspec.GridSpec(2, 1)
    # ax1 = plt.subplot(gs[0, 0])
    # # Adds subplot 'ax' in grid 'gs' at position [x,y]
    # ax1.set_ylabel('Coherence score')  # Add y-axis label 'Foo' to graph 'ax' (xlabel for x-axis)
    # fig.add_subplot(ax1)  # add 'ax' to figure
    # ax1 = fig.add_axes(coherence_values)
    # ax1.set_ylabel("Coherence score")
    # ax1.plot(x)
    # ax2 = fig.add_axes(perplexity_values)
    # ax2.set_ylabel("Perplexity score")
    # ax1.set_xlabel("Num Topics")
    # ax2.plot(x)
    #
    plt.savefig("../outputs/coherence_perplxity")

    # x = range(start, limit, step)
    # fig, axs = plt.subplots(2)
    # axs[0].plot(x, coherence_values)
    # plt.xlabel("Num Topics", axes=axs[0])
    # plt.ylabel("Coherence score", axes=axs[0])
    # # plt.legend(("coherence_values"), loc='best', axes=axs[0])
    # axs[1].plot(x, perplexity_values)
    # plt.xlabel("Num Topics", axes=axs[1])
    # plt.ylabel("Perplexity score", axes=axs[1])
    # # plt.legend(("Perplexity_values"), loc='best', axes=axs[1])
    # plt.savefig("../outputs/coherence_perplxity")


if __name__ == "__main__":
    flag = False  # true if the model already built
    source_name, source_type = "wallstreetbets", "mongo"
    best_model_path = "../outputs/model80.gensim"
    con_db = Con_DB()
    if flag:
        id_list = []
        data = con_db.get_cursor_from_mongodb(collection_name=source_name)
        # for x in tqdm(data):
        #     id_list.append(x["pushift_api"]["id"])
        lda_model = LdaModel.load(best_model_path)
        infile = open("../outputs/corpus.pkl", 'rb')
        corpus = pickle.load(infile)
        infile.close()
        dictionary = corpora.Dictionary.load("../outputs/dictionary.gensim")
        # topics = lda_model.print_topics(num_words=4)
        # dominant_topics(ldamodel=lda_model, corpus=corpus, ids=id_list)

    else:
        lda_model, corpus, dictionary = topic_analysis(source_name, source_type)

    # Visualize the topics
    visualisation = pyLDAvis.gensim_models.prepare(lda_model, corpus, dictionary, sort_topics=False)
    pyLDAvis.save_html(visualisation, '../outputs/LDA_Visualization.html')
    sentiment_topics = []
    plt.clf()
    for topic_id in tqdm(range(lda_model.num_topics)):
        topk = lda_model.show_topic(topic_id, 10)
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
        # plt.show()
        plt.savefig("../outputs/worldcloud_{}".format(topic_id))
    df = pd.DataFrame(data=sentiment_topics, columns=['topic_id', 'topic', 'sentim'])
    df.to_csv("../outputs/topic_sentim.csv")
