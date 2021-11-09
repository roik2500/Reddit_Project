import logging
import re
import time
from datasets import tqdm
from text_analysis.topicsAnalysisClass2 import TopicsAnalysis
import sys

from text_analysis.utills import assign_txt_data_ids_list

if __name__ == "__main__":
    t1 = time.time()
    regex_lda = re.compile('(model*.*gensim$)')
    source_name, source_type = "wallstreetbets_expr_shimon", "json"
    prepare_data = True  # if true load data from mongo and prepare it. else from disk
    prepare_dictionary_corpus = True  # if true prepare it. else from disk
    prepare_models = True  # if true create models. else load models from disk
    prepare_months = True  # if true apply for every month
    start, limit, step = 10, 25, 1
    post_comment_flag = "post"
    for i in range(1, 2):
        removed_flag = i  # if True its all data, if False its only the removed
        topics = TopicsAnalysis(source_name, removed_flag, prepare_data, prepare_dictionary_corpus,  post_comment_flag, start, limit, step)
        rng = []
        if prepare_months:
            rng = list(topics.id_list_month.keys())
        rng.insert(0, "general")
        models = []
        if prepare_models:
            for month_key in tqdm(rng):
                logging.info("{} topics model build".format(month_key))
                ids_lst, txt_dt = assign_txt_data_ids_list(month_key, topics)
                topics.create_or_load_model(month_key, prepare_models, txt_dt)

        for k, month_key in enumerate(tqdm(rng)):
            logging.info("k = {}".format(k))
            logging.info("{} topics model using".format(month_key))
            ids_lst, txt_dt = assign_txt_data_ids_list(month_key, topics)
            model = topics.create_or_load_model(month_key, False, txt_dt)
            logging.info("start to use {}".format(month_key))
            topics.use_models(month_key, ids_lst, model)
        t2 = time.time()
        logging.info("final time {}".format(t2 - t1))
