import spacy
from spacy.lang.en import English
import nltk
from nltk.corpus import wordnet as wn
from nltk.stem.wordnet import WordNetLemmatizer

spacy.load('en_core_web_sm')
parser = English()

class Parser:
    def __init__(self):
        self.NER = spacy.load("en_core_web_lg")
        self.lst_NER_types = ["ORG", "GPE", "PRODUCT", "LOC", "DATE", "ORDINAL", "MONEY", "PERSON"]

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