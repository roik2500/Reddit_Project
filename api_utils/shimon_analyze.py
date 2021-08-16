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


if __name__ == "__main__":
    loaded_lda_model = LdaModel.load("model32.gensim")
    infile = open("corpus.pkl", 'rb')
    loaded_corpus = pickle.load(infile)
    infile.close()
    loaded_dict = corpora.Dictionary.load("dictionary.gensim")
    topics = loaded_lda_model.print_topics(num_words=4)
    for topic_id in range(loaded_lda_model.num_topics):
        topk = loaded_lda_model.show_topic(topic_id, 10)
        topk_words = [w for w, _ in topk]
        topic = '{}'.format(' '.join(topk_words))
        blob = TextBlob(topic)
        print(topic,": ",blob.sentiment.polarity)

    # Visualize the topics
    # visualisation = pyLDAvis.gensim_models.prepare(loaded_lda_model, loaded_corpus, loaded_dict)
    # pyLDAvis.save_html(visualisation, 'LDA_Visualization.html')

    # text_data = []
    # myclient = pymongo.MongoClient("mongodb+srv://shimon:1234@redditdata.aav2q.mongodb.net/")
    # mydb = myclient["reddit"]
    # mycol = mydb["deletedData"]
    #
    # for x in mycol.find({}, {"title": 1, "selftext": 1, "subreddit": 1}):
    #     if x["subreddit"] == "The_Donald":
    #         tokens = prepare_text_for_lda(x["title"])
    #         tokens.extend(prepare_text_for_lda(x["selftext"]))
    #         print(tokens)
    #         text_data.append(tokens)
    #
    # dictionary = corpora.Dictionary(text_data)
    # corpus = [dictionary.doc2bow(text) for text in text_data]
    # pickle.dump(corpus, open('corpus.pkl', 'wb'))
    # dictionary.save('dictionary.gensim')
    #
    # # NUM_TOPICS = 5
    #
    # limit = 36
    # start = 2
    # step = 6
    # coherence_values = []
    # perplexity_values = []
    # model_list = []
    # best_num_of_topics = 0
    # bst_coh = 0
    # for num_of_topic in range(start, limit, step):
    #     print(num_of_topic)
    #     ldamodel = gensim.models.ldamodel.LdaModel(corpus, num_topics=num_of_topic, id2word=dictionary, passes=15)
    #     ldamodel.save('model{}.gensim'.format(num_of_topic))
    #     topics = ldamodel.print_topics(num_words=4)
    #     for topic in topics:
    #         print(topic)
    #     # Compute Perplexity
    #     perplexity_value = ldamodel.log_perplexity(corpus)
    #     print('\nPerplexity: ', perplexity_value)  # a measure of how good the model is. lower the better.
    #     perplexity_values.append(perplexity_value)
    #
    #     # Compute Coherence Score
    #     coherence_model_lda = CoherenceModel(model=ldamodel, texts=text_data, dictionary=dictionary, coherence='c_v')
    #     coherence_lda = coherence_model_lda.get_coherence()
    #     if coherence_lda > bst_coh:
    #         bst_coh = coherence_lda
    #         best_num_of_topics = num_of_topic
    #     coherence_values.append(coherence_lda)
    #     print('\nCoherence Score: ', coherence_lda)
    # x = range(start, limit, step)
    # fig, axs = plt.subplots(2)
    # axs[0].plot(x, coherence_values)
    # plt.xlabel("Num Topics")
    # plt.ylabel("Coherence score")
    # plt.legend(("coherence_values"), loc='best')
    # axs[1].plot(x, perplexity_values)
    # plt.xlabel("Num Topics")
    # plt.ylabel("perplexity score")
    # plt.legend(("perplexity_values"), loc='best')
    # plt.savefig("coherence_perplxity")
    #
    # ldamodel = LdaModel.load("model{}.gensim".format(best_num_of_topics))
    # # Visualize the topics
    # visualisation = pyLDAvis.gensim_models.prepare(ldamodel, corpus, dictionary)
    # pyLDAvis.save_html(visualisation, 'LDA_Visualization.html')
