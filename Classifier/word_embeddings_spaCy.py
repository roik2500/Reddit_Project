import spacy
# from spacy import displacy
import statistics
import numpy as np

class WordEmbedding:

    def __init__(self):
        self.nlp_lg = spacy.load('en_core_web_lg')

    def get_emdeding(self, words):
        docs = self.nlp_lg(words)
        return docs

    def get_emdeding_post_vec_avg(self, words):
        docs = self.get_emdeding(words)
        length = len(words.split())
        doc_mean = self.get_mean(docs, length)
        return doc_mean

    def get_mean(self, doc, length):
        lst_of_vectors = [doc[n].vector for n in range(0, length)]
        return [np.mean(k) for k in zip(*lst_of_vectors)]
