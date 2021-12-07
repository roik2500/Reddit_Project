from bertopic import BERTopic
import torch
import pandas as pd
import pickle
import random
from tqdm.notebook import tqdm
from sklearn.feature_extraction.text import CountVectorizer
from umap import UMAP
from bertopic import BERTopic
import gensim.corpora as corpora
from gensim.models.coherencemodel import CoherenceModel
from itertools import product
from tqdm import tqdm as tqdm

def get_topic_model(documents, n_neighbors, min_topic_size,calculate_probabilities = False):
    umap_model = UMAP(n_neighbors=n_neighbors, n_components=10, min_dist=0.0, metric='cosine')

    vectorizer_model = CountVectorizer(ngram_range=(1, 2), stop_words="english", min_df=10)
    topic_model = BERTopic(umap_model=umap_model,vectorizer_model=vectorizer_model, calculate_probabilities=calculate_probabilities, verbose=True, min_topic_size=min_topic_size)
    topic_model.fit(documents)
    return topic_model


def get_coherence(df, topic_model):
    documents_per_topic = df.groupby(['topic'], as_index=False).agg({'title': ' '.join})

    cleaned_docs = topic_model._preprocess_text(documents_per_topic.title.values)

    # Extract vectorizer and analyzer from BERTopic
    vectorizer = topic_model.vectorizer_model
    analyzer = vectorizer.build_analyzer()

    # Extract features for Topic Coherence evaluation
    words = vectorizer.get_feature_names()
    tokens = [analyzer(doc) for doc in cleaned_docs]
    dictionary = corpora.Dictionary(tokens)
    corpus = [dictionary.doc2bow(token) for token in tokens]
    topic_words = [[words for words, _ in topic_model.get_topic(topic)]
                   for topic in range(len(set(topics)) - 1)]

    # Evaluate
    coherence_model = CoherenceModel(topics=topic_words,
                                     texts=tokens,
                                     corpus=corpus,
                                     dictionary=dictionary,
                                     coherence='c_v')

    coherence = coherence_model.get_coherence()
    return coherence

if __name__ == '__main__':
    path_pkl='G:/.shortcut-targets-by-id/1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d/final_project/data/data for bert/technology.pkl'
    with open(path_pkl, "rb") as f:
        df = pickle.load(f)
    dff = pd.DataFrame(df)
    dff
    dff['title']=dff['pushift_api.title']
    dff['url']=dff['pushift_api.url']
    dff['selftext']=dff['pushift_api.selftext']
    dff['removed_by_category']=dff['pushift_api.removed_by_category']
    del dff['pushift_api.selftext']
    del dff['pushift_api.removed_by_category']
    del dff['pushift_api.url']
    del dff['pushift_api.title']

    content = list(dff["title"])
    random.seed(1)
    docs = random.sample(content,83694)
    n_neighbors = [5, 10 ,15, 20, 25, 30, 35, 40, 45]
    min_topic_sizes = [50, 100 ,150, 200, 250, 300, 350 , 400]
    res = []
    subreddit='technology'
    for n_neighbor, min_topic_size  in tqdm(product(n_neighbors, min_topic_sizes), total=36):
        model = get_topic_model(docs, n_neighbor, min_topic_size)
        topics, probas = model.transform(dff["title"])
        dff["topic"] = topics
        tmp = dff[["title","topic"]]
        tmp["ID"] =  range(len(dff))
        coh = get_coherence(tmp, model)
        res.append({"coherenc": coh, "min_topic_size": min_topic_size, "n_neighbor":n_neighbor})
        pd.DataFrame(res).to_csv("{}_coh.csv".format(subreddit))

    pd.DataFrame(res).to_csv("{}_coh.csv".format(subreddit))
    coh = pd.DataFrame(res)
    coh.loc[coh["coherenc"].argmax()]
    model.get_topic_info()
