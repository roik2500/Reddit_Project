import json
from difflib import SequenceMatcher
import numpy as np
import pandas as pd
from datasets import tqdm
from gensim import corpora
from gensim.models import TfidfModel
from matplotlib.dates import DateFormatter
from spacy.lang.en import English
import nltk
from nltk.corpus import wordnet as wn
from nltk.stem.wordnet import WordNetLemmatizer
import pickle
import pathlib
from matplotlib import pyplot as plt
import matplotlib.colors as mcolors
colors = [color for name, color in mcolors.XKCD_COLORS.items()]  # more colors: 'mcolors.XKCD_COLORS'


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


from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt


def multipage(filename, figs=None, dpi=200):
    pp = PdfPages(filename)
    if figs is None:
        figs = [plt.figure(n) for n in plt.get_fignums()]
    for fig in figs:
        fig.savefig(pp, format='pdf')
    pp.close()


def export_bert_plots_bars(path):
    df = pd.read_csv(path)
    fig1, axes1 = plt.subplots(nrows=2, ncols=1)
    fig2, axes2 = plt.subplots(nrows=2, ncols=1)
    fig3, axes3 = plt.subplots(nrows=2, ncols=1)
    fig4, axes4 = plt.subplots(nrows=2, ncols=1)
    fig5, axes5 = plt.subplots(nrows=2, ncols=1)
    axes_dict = {0: axes1, 1: axes2, 2: axes3, 3: axes4, 4: axes5}
    df_1 = df.loc[:, ["joy", "surprise", "status", "Topic"]]
    df_4 = df.loc[:, ["anger", "disgust", "status", "Topic"]]
    df_5 = df.loc[:, ["fear", "sadness", "status", "Topic"]]
    df_2 = df.loc[:, ["bertweet_neg", "bertweet_pos", "status", "Topic"]]
    df_3 = df.loc[:, ["hate", "offensive", "status", "Topic"]]
    dfs = [df_1, df_2, df_3, df_4, df_5]
    for i, daf in enumerate(dfs):
        for j, col in enumerate(daf):
            if col in ["status", "Topic"]:
                continue
            agg_dfs = []
            statuses = ["exists", "deleted", "shadow_ban", "removed"]
            count_label = []
            for status in statuses:
                tmp = daf.loc[:, [col, "Topic"]][daf["status"] == status].groupby("Topic", as_index=False).mean()
                tmp = tmp.drop(tmp[tmp["Topic"] == -1].index)
                agg_dfs.append(tmp)
                count_label.append(
                    daf[daf["status"] == status].groupby("Topic", as_index=False).count().loc[:, ["status", "Topic"]])
            tmp = agg_dfs[0]
            tmp1 = count_label[0]
            for k in range(1, len(agg_dfs)):
                tmp = pd.merge(tmp, agg_dfs[k], how="outer", left_on="Topic", right_on="Topic")
                tmp1 = pd.merge(tmp1, count_label[k], how="outer", left_on="Topic", right_on="Topic")
            # agg_dfs = pd.concat(agg_dfs, axis=1, join='outer')
            agg_dfs = tmp
            agg_dfs.columns = ["Topic"] + statuses
            agg_dfs.plot(x="Topic", ax=axes_dict[i][j], kind="bar", legend=False, title=col, use_index=False,
                         stacked=True, xticks=range(0, 50, 5), ylabel="Average per topic")
            tmp1 = tmp1.drop(tmp1[tmp1["Topic"] == -1].index)
            del tmp1["Topic"]
            tmp1 = tmp1.fillna(0)
            # tmp1 = tmp1.div(tmp1.sum(axis=1), axis=0).apply(lambda x: np.round(x * 100, 1))
            for c, cc in enumerate(tmp1):
                tmp1.iloc[:, c] = tmp1.iloc[:, c].apply(
                    lambda x: np.round((x - tmp1.iloc[:, c].min()) / (tmp1.iloc[:, c].max() - tmp1.iloc[:, c].min()),
                                       1))
            # tmp1 = tmp1.replace(0, '')
            for r in range(len(tmp1.columns)):
                axes_dict[i][j].bar_label(axes_dict[i][j].containers[r], tmp1.iloc[:, r].values.tolist(),
                                          label_type="center", fontsize=3, padding=1, fmt='{:.1f}% ')
            axes_dict[i][j].set_xlabel("Topic")
    fig1.tight_layout(pad=2)
    fig2.tight_layout(pad=2)
    fig3.tight_layout(pad=2)
    fig4.tight_layout(pad=2)
    fig5.tight_layout(pad=2)
    fig1.legend(statuses, loc="center", ncol=4)
    fig2.legend(statuses, loc="center", ncol=4)
    fig3.legend(statuses, loc="center", ncol=4)
    fig4.legend(statuses, loc="center", ncol=4)
    fig5.legend(statuses, loc="center", ncol=4)
    multipage('final_normalized.pdf')
    # plt.show()


def export_bert_plots_scatter(path):
    df = pd.read_csv(path)
    fig1, axes1 = plt.subplots(nrows=2, ncols=1)
    fig2, axes2 = plt.subplots(nrows=2, ncols=1)
    fig3, axes3 = plt.subplots(nrows=2, ncols=1)
    fig4, axes4 = plt.subplots(nrows=2, ncols=1)
    fig5, axes5 = plt.subplots(nrows=2, ncols=1)
    axes_dict = {0: axes1, 1: axes2, 2: axes3, 3: axes4, 4: axes5}
    df_1 = df.loc[:, ["joy", "surprise", "status", "Topic"]]
    df_4 = df.loc[:, ["anger", "disgust", "status", "Topic"]]
    df_5 = df.loc[:, ["fear", "sadness", "status", "Topic"]]
    df_2 = df.loc[:, ["bertweet_neg", "bertweet_pos", "status", "Topic"]]
    df_3 = df.loc[:, ["hate", "offensive", "status", "Topic"]]
    dfs = [df_1, df_2, df_3, df_4, df_5]
    for i, daf in enumerate(dfs):
        for j, col in enumerate(daf):
            if col in ["status", "Topic"]:
                continue
            agg_dfs = []
            statuses = ["exists", "deleted", "shadow_ban", "removed"]
            count_label = []
            for status in statuses:
                tmp = daf.loc[:, [col, "Topic"]][daf["status"] == status].groupby("Topic", as_index=False).mean()
                tmp = tmp.drop(tmp[tmp["Topic"] == -1].index)
                agg_dfs.append(tmp)
                count_label.append(
                    daf[daf["status"] == status].groupby("Topic", as_index=False).count().loc[:, ["status", "Topic"]])
            tmp = daf.loc[:, [col, "Topic"]].loc[daf["status"].isin(['removed', 'shadow_ban'])].groupby(
                "Topic", as_index=False).mean()
            tmp = tmp.drop(tmp[tmp["Topic"] == -1].index)
            agg_dfs.append(tmp)
            count_label.append(
                daf.loc[daf["status"].isin(['removed', 'shadow_ban'])].groupby("Topic", as_index=False).count().loc[:,
                ["status", "Topic"]])
            tmp = agg_dfs[0]
            tmp1 = count_label[0]
            for k in range(1, len(agg_dfs)):
                tmp = pd.merge(tmp, agg_dfs[k], how="outer", left_on="Topic", right_on="Topic")
                tmp1 = pd.merge(tmp1, count_label[k], how="outer", left_on="Topic", right_on="Topic")
            # agg_dfs = pd.concat(agg_dfs, axis=1, join='outer')
            agg_dfs = tmp
            agg_dfs.columns = ["Topic"] + statuses + ["moderated"]
            # agg_dfs.plot(x="Topic", ax=axes_dict[i][j], kind="bar", legend=False, title=col, use_index=False,
            #              stacked=True, xticks=range(0, 50, 5), ylabel="Average per topic")
            tmp1 = tmp1.drop(tmp1[tmp1["Topic"] == -1].index)
            tmp1.columns = [statuses[0]] + ["Topic"] + statuses[1:] + ["moderated"]
            # del tmp1["Topic"]
            tmp1 = tmp1.fillna(0)
            tmp1.loc[:, statuses] = tmp1.loc[:, statuses].div(tmp1.loc[:, statuses].sum(axis=1), axis=0).apply(
                lambda x: np.round(x * 100, 1))
            tmp1["Moderation ratio"] = (tmp1["removed"] + tmp1["shadow_ban"]) / tmp1["exists"]
            agg_dfs = pd.merge(agg_dfs, tmp1.loc[:, ["Moderation ratio", "Topic"]], how="outer", left_on="Topic",
                               right_on="Topic")
            # del agg_dfs["Topic"]
            agg_dfs.plot(x="Topic", y="moderated", c="Moderation ratio", colormap='viridis', kind="scatter",
                         ax=axes_dict[i][j], legend=False,
                         use_index=False,
                         stacked=True, ylabel="Average {} per topic".format(col), xlabel="Topic",
                         xticks=range(0, 50, 3))
            # tmp1 = tmp1.replace(0, '')
            # for r in range(len(tmp1.columns)):
            #     axes_dict[i][j].bar_label(axes_dict[i][j].containers[r], tmp1.iloc[:, r].values.tolist(),
            #                               label_type="center", fontsize=3, padding=1, fmt='{:.1f}% ')
    fig1.tight_layout(pad=2)
    fig2.tight_layout(pad=2)
    fig3.tight_layout(pad=2)
    fig4.tight_layout(pad=2)
    fig5.tight_layout(pad=2)
    # fig1.legend(statuses, loc="center", ncol=4)
    # fig2.legend(statuses, loc="center", ncol=4)
    # fig3.legend(statuses, loc="center", ncol=4)
    # fig4.legend(statuses, loc="center", ncol=4)
    # fig5.legend(statuses, loc="center", ncol=4)
    multipage('final_new.pdf')
    # plt.show()


# def export_bert_plots(path):
#     df = pd.read_csv(path)
#     fig1, axes1 = plt.subplots(nrows=4, ncols=1)
#     fig2, axes2 = plt.subplots(nrows=4, ncols=1)
#     fig3, axes3 = plt.subplots(nrows=4, ncols=1)
#     statuses = ["shadow_ban", "exists", "deleted", "removed"]
#     for i, status in enumerate(statuses):
#         agg_df = df[df["status"] == status].groupby("Topic", as_index=False).mean()
#         agg_df_1 = agg_df.loc[:, ["anger", "disgust", "fear", "joy", "sadness", "surprise", "Topic"]]
#         agg_df_2 = agg_df.loc[:, ["bertweet_neg", "bertweet_pos", "Topic"]]
#         agg_df_3 = agg_df.loc[:, ["hate", "offensive", "Topic"]]
#         # for i, col in enumerate(agg_df):
#         #     axs[i].plot(agg_df[col])
#         agg_df_1.plot(x="Topic", ax=axes1[i], kind="bar", legend=False, title=status)
#         agg_df_2.plot(x="Topic", ax=axes2[i], kind="bar", legend=False, title=status)
#         agg_df_3.plot(x="Topic", ax=axes3[i], kind="bar", legend=False, title=status)
#     fig1.tight_layout()
#     fig2.tight_layout()
#     fig3.tight_layout()
#     fig1.legend(["anger", "disgust", "fear", "joy", "sadness", "surprise"])
#     fig2.legend(["bertweet_neg", "bertweet_pos"])
#     fig3.legend(["hate", "offensive"])
#     plt.show()

def export_time_series_plot(posts_file_path):
    df = pd.read_csv(posts_file_path)
    fig, axes = plt.subplots(nrows=5, ncols=2)
    df = df.drop(df[df["Topic"] == -1].index)
    df["month"] = df["created_date"].apply(lambda x: x.split("-")[1])
    df = pd.concat(
        [df.loc[df["Topic"] == x].groupby("created_date").count()["Count"] for x in df["Topic"].unique().tolist()],
        axis=1)
    df = df.fillna(0)
    df.index = pd.to_datetime(df.index)
    df.columns = [str(x) for x in range(50)]
    df = df.sort_index()
    # df = df.div(df.sum(axis=1), axis=0).apply(lambda x: np.round(x * 100, 1))
    dfs = [df.iloc[:, x:y] for x, y in zip(range(0, 50, 5), range(5, 55, 5))]
    for i, d in enumerate(dfs):
        if i < 5:
            j = 0
        else:
            j = 1
        d.plot.area(ax=axes[i % 5][j])
    # plt.stackplot(df.index, *[df[col] for col in df])
    plt.show()


def parse_ner(ner):
    s = ner.strip("[ ]").replace("\'", "").split(",")
    s = [','.join(i) for i in zip(s[::2], s[1::2])]
    res = []
    for ner in s:
        res.append(ner.strip("( )").split(","))
    return res


def ner_dupl():
    ind = read_json("top_inverted_index")
    to_del = []
    for k, v in ind.items():
        for kk, vv in v.items():
            if kk in to_del:
                continue
            for kkk, vvv in v.items():
                if SequenceMatcher(None, kkk, kk).ratio() > 0.8:
                    v[kk].extend(v[kkk])
                    to_del.append(kkk)
        for kk in to_del:
            del v[kk]

    dump_json(ind, "top_inverted_index_no_dupl")


def ner_plot(posts_file_path):
    df = pd.read_csv(posts_file_path)
    fig, axes = plt.subplots(nrows=5, ncols=2)
    # df = df.drop(df[df["Topic"] == -1].index)
    # df["month"] = df["created_date"].apply(lambda x: x.split("-")[1])

    ind = read_json("top_inverted_index")
    unique_dates = range(1,13)
    # unique_dates = df["created_date"].unique().tolist()
    for k, v in ind.items():
        final_df = pd.DataFrame(unique_dates, columns=["created_date"])
        data_items = v.items()
        data_list = list(data_items)
        df_ind = pd.DataFrame(data_list)
        df_ind["count_posts_per_ent"] = df_ind.iloc[:, 1].apply(lambda x: len(x))
        date_dict = {}
        for ent in df_ind.iloc[:, 0]:
            date_dict[ent] = df.loc[df["post_id"].isin(df_ind[df_ind.iloc[:, 0] == ent][1].values.tolist()[0])]["created_date"].values.tolist()
            post_date_tmp = pd.DataFrame([df_ind.iloc[:, 1].values.tolist()[0], date_dict[ent]])
            post_date_tmp = post_date_tmp.transpose()
            post_date_tmp.columns = [ent, "created_date"]
            post_date_tmp = post_date_tmp.dropna()
            post_date_tmp["created_date"] = post_date_tmp.loc[:,"created_date"].apply(lambda x: x.split("-")[1])
            count_date = post_date_tmp.groupby("created_date", as_index=False).count()
            final_df = final_df.join(count_date, how="outer", lsuffix="_drop")
            final_df.drop([col for col in final_df.columns if 'drop' in col], axis=1, inplace=True)

        # final_df["created_date"] = pd.to_datetime(final_df["created_date"])
        final_df.plot.area(x="created_date", stacked=True, color=colors)

            # df_ind["date"].loc[df_ind.iloc[:, 0] == ent] =
        # df_date_dict = pd.DataFrame(list(date_dict.values()))
        # df_ind["date"] = df_date_dict.iloc[0]




        # df_ind.plot.area(x=0, y="count_posts_per_ent", stacked=True)
    plt.show()


def ner_top(cut_num):
    index = read_json("inverted_index")
    top_index = {}
    for k, v in index.items():
        for kk, vv in v.items():
            if len(vv) > cut_num:
                top_index.setdefault(k, {})[kk] = vv
    dump_json(top_index, "top_inverted_index")


def ner_inverted_index(file_path):
    inverted_index = {}
    df = pd.read_csv(file_path)
    for i, post in tqdm(df.iterrows()):
        parsed_ner = parse_ner(post["ner"])
        for ner in parsed_ner:
            name = ner[0].lower().replace("'", "")
            typee = ner[1].replace(" ", "")
            if typee in ["DATE", "ORDINAL", "NUMBER", "TIME", "QUANTITY", "CARDINAL", "MONEY", "PERCENT", "LANGUAGE"]:
                continue
            if typee in inverted_index:
                if name in inverted_index[typee]:
                    inverted_index[typee][name].append(post["post_id"])
                else:
                    bool_sim = False
                    for k in inverted_index[typee]:
                        if SequenceMatcher(None, k, name).ratio() > 0.8:
                            inverted_index[typee][k].append(post["post_id"])
                            bool_sim = True
                            break
                    if not bool_sim:
                        inverted_index[typee][name] = [post["post_id"]]
            else:
                inverted_index[typee] = {name: [post["post_id"]]}

    dump_json(inverted_index, "inverted_index")
    print("end")


def dump_json(obj, file_name):
    with open(f"{file_name}.json", "w") as f:
        json.dump(obj, f)


def read_json(file_name):
    with open(f"{file_name}.json", "r") as f:
        return json.load(f)


# export_time_series_plot(r"C:\Users\shimon\Downloads\data_with_time.csv")
export_bert_plots_scatter(r"data_with_time.csv")
# ner_inverted_index(r"data_with_time.csv")
# r = read_json("inverted_index")
# print("--")
# ner_top(1000)
# ner_plot(r"data_with_time.csv")
