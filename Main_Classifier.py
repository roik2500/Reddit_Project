from Classifier.model import Model

import os
from dotenv import load_dotenv
import pandas as pd
import ijson
from tqdm import tqdm

from db_utils.FileReader import FileReader

load_dotenv()


PATH_DRIVE = os.getenv("OUTPUTS_DIR")
# corpus_json_file = PATH_DRIVE + 'wallstreetbets_2020_full_1.json'
corpus_json_file = 'C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\wallstreetbets_2020_full_.json'

model = Model(max_post_number=239000, path_data=corpus_json_file, post_or_comment_model='post')

model.split_corpus()

trined_model = model.train_model(model_name="RandomForestClassifier")

y_predict = trined_model.predict(model.df_test)

