import logging
import re
from datetime import time
from datasets import tqdm
from text_analysis.topicsAnalysisClass2 import TopicsAnalysis

if __name__ == "__main__":
    t1 = time.time()
    regex_lda = re.compile('(model*.*gensim$)')
    source_name, source_type = "wallstreetbets", "mongo"
    prepare_data = False  # if true load data from mongo and prapre it. else load dic and corp from disk
    prepare_models = False  # if true create models. else load models from disk
    prepare_months = False # if true apply for every month
    post_comment_flag = "post"
    for i in range(1, 2):
        removed_flag = i  # if True its all data, if False its only the removed
        topics = TopicsAnalysis(source_name, removed_flag, prepare_data, post_comment_flag)
        rng = list(topics.id_list_month.keys())
        rng.append("general")
        models = []
        for month_key in tqdm(rng):
            logging.info("{} topics model build".format(month_key))
            if month_key == 'general':
                ids_lst = topics.id_list
                txt_dt = list(topics.text_data.values())[0]
            else:
                ids_lst = topics.id_list_month[month_key]
                txt_dt = topics.text_data[month_key]
            models.append(topics.run_model(month_key, prepare_models, txt_dt))

        for k, month_key in enumerate(tqdm(rng)):
            logging.info("k = {}".format(k))
            logging.info("{} topics model using".format(month_key))
            if month_key == 'general':
                ids_lst = topics.id_list
                txt_dt = list(topics.text_data.values())[0]
            else:
                ids_lst = topics.id_list_month[month_key]
                txt_dt = topics.text_data[month_key]
            logging.info("start to use {}".format(month_key))
            topics.use_model(month_key, ids_lst, models[k])
        t2 = time.time()
        logging.info("final time {}".format(t2 - t1))