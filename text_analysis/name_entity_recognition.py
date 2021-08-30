import spacy
from spacy import displacy


class NameEntity:
    def __init__(self):
        self.NER = spacy.load("en_core_web_lg")
        self.lst_NER_types = ["ORG", "GPE", "PRODUCT", "LOC", "DATE", "ORDINAL", "MONEY", "PERSON"]

    def get_entites(self,raw_text):
        text1= self.NER(raw_text)
        return [(word.text,word.label_) for word in text1.ents]
        # for word in text1.ents:
        #     print(word.text,word.label_)

    def help_get_NER_types(self):
        return self.lst_NER_types

    def type_explain(self,text_type):
        return spacy.explain(text_type)

if __name__ == '__main__':
    # raw_text = "It only took two hours for Trump's administration to contradict his th..."
    # name_entity = NameEntity()
    # a = name_entity.get_entites(raw_text)

    from db_utils.FileReader import Reader
    from dotenv import load_dotenv
    import pandas as pd
    import os
    import pandas as pd

    load_dotenv()
    reader = Reader()
    posts = reader.get_posts_from_mongodb(auth=os.getenv('AUTH_DB'), db_name="reddit", collection_name="wallstreetbets1609452000")
    data_ = posts.find({}, {"reddit_api": 1, "pushshift_api": 1})
    # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(data_))
    for x in df['reddit_api']:
        print(x[0]['data']['children'][0]['data']['selftext'])
        print(x[0]['data']['children'][0]['data']['title'])
        print(x[0]['data']['children'][0]['data']['created_utc'])



