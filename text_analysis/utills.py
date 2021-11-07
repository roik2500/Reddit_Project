from gensim import corpora
from gensim.models import TfidfModel
from spacy.lang.en import English
import nltk
from nltk.corpus import wordnet as wn
from nltk.stem.wordnet import WordNetLemmatizer
import pickle
import pathlib


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


def assign_txt_data_ids_list(month_key, topics_obj):
    global ids_lst, txt_dt
    if month_key == 'general':
        ids_lst = topics_obj.id_list
        txt_dt = [txt for txt_data in list(topics_obj.text_data.values()) for txt in txt_data]
    else:
        ids_lst = topics_obj.id_list_month[month_key]
        txt_dt = topics_obj.text_data[month_key]
    return ids_lst, txt_dt


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


def dump_prepared_data_files(directory, text_data):
    pathlib.Path(directory).mkdir(parents=True, exist_ok=True)
    # if not os.path.exists(directory):
    #     os.mkdir(directory)
    dic = corpora.Dictionary(text_data)
    dic.save('{}/dictionary.gensim'.format(directory))
    corpus = [dic.doc2bow(text) for text in text_data]
    tfidf = TfidfModel(corpus)
    corpus_tfidf = tfidf[corpus]
    pickle.dump(corpus, open('{}/corpus.pkl'.format(directory), 'wb'))
    pickle.dump(corpus_tfidf, open('{}/corpus_tfidf.pkl'.format(directory), 'wb'))
