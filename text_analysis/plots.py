import json
import datetime
from difflib import SequenceMatcher
import numpy as np
import pandas as pd
from datasets import tqdm
import altair as alt
import plotly.express as px
import matplotlib.colors as mcolors
import altair_viewer
import streamlit as st
from dateutil.relativedelta import relativedelta

colors = ['tab:red', 'tab:blue', 'tab:green', 'tab:orange', 'tab:brown', 'tab:grey', 'tab:pink', 'tab:olive']

# colors = [color for name, color in mcolors.XKCD_COLORS.items()]  # more colors: 'mcolors.XKCD_COLORS'

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
            tmp1["Moderation ratio quantity"] = (tmp1["removed"] + tmp1["shadow_ban"]) / tmp1["exists"]
            agg_dfs = pd.merge(agg_dfs, tmp1.loc[:, ["Moderation ratio quantity", "Topic"]], how="outer",
                               left_on="Topic",
                               right_on="Topic")
            agg_dfs["avg moderation ratio {}".format(col)] = agg_dfs["moderated"] / agg_dfs["exists"]
            # del agg_dfs["Topic"]
            agg_dfs.plot(x="Topic", c="avg moderation ratio {}".format(col), y="Moderation ratio quantity",
                         colormap='viridis', kind="scatter",
                         ax=axes_dict[i][j], legend=False,
                         use_index=False,
                         stacked=True, ylabel="Moderation ratio quantity", xlabel="Topic",
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


def ner_emotion_plot(posts_file_path):
    # keys_replace_dict = {"joe biden 's": }
    df = pd.read_csv(posts_file_path)
    # fig, axes = plt.subplots(nrows=5, ncols=2)
    # df = df.drop(df[df["Topic"] == -1].index)
    # df["month"] = df["created_date"].apply(lambda x: x.split("-")[1])

    ind = read_json("C:/Users/shimon/PycharmProjects/Reddit_Project/text_analysis/top_inverted_index")
    # unique_dates = range(1, 13)
    unique_dates = df["created_date"].apply(
        lambda x: datetime.date(int(x.split("-")[0]), int(x.split("-")[1]), int(x.split("-")[2])).isocalendar()[
            1]).unique().tolist()
    unique_dates = sorted(unique_dates)

    for k, v in ind.items():
        final_df = pd.DataFrame(unique_dates, columns=["created_date"])
        data_items = v.items()
        data_list = list(data_items)
        df_ind = pd.DataFrame(data_list)
        df_ind["count_posts_per_ent"] = df_ind.iloc[:, 1].apply(lambda x: len(x))
        date_dict = {}
        for ent in df_ind.iloc[:, 0]:
            posts_indices = df["post_id"].isin(df_ind[df_ind.iloc[:, 0] == ent][1].values.tolist()[0])
            # date_dict[ent] = df.loc[posts_indices, "created_date"].values.tolist()
            df_1 = df.loc[posts_indices, ["joy", "surprise"]]
            df_4 = df.loc[posts_indices, ["anger", "disgust"]]
            df_5 = df.loc[posts_indices, ["fear", "sadness"]]
            df_2 = df.loc[posts_indices, ["bertweet_neg", "bertweet_pos"]]
            df_3 = df.loc[posts_indices, ["hate", "offensive"]]
            dfs = [df_1, df_2, df_3, df_4, df_5]
            for df in dfs:
                post_date_tmp = pd.DataFrame([df_ind.iloc[:, 1].values.tolist()[0], df.values.tolist()])
                # post_date_tmp = pd.DataFrame([df_ind.iloc[:, 1].values.tolist()[0], date_dict[ent]])
                post_date_tmp = post_date_tmp.transpose()
                post_date_tmp.columns = [ent, "created_date"]
                post_date_tmp = post_date_tmp.dropna()
                post_date_tmp["created_date"] = post_date_tmp.loc[:, "created_date"].apply(
                    lambda x:
                    datetime.date(int(x.split("-")[0]), int(x.split("-")[1]), int(x.split("-")[2])).isocalendar()[
                        1])  # .apply(lambda x: x.split("-")[1])
                count_date = post_date_tmp.groupby("created_date", as_index=False).count()
                final_df = final_df.join(count_date, how="outer", rsuffix="_drop")
                final_df.drop([col for col in final_df.columns if 'drop' in col], axis=1, inplace=True)
                final_df = final_df.fillna(0)

        # final_df["created_date"] = pd.to_datetime(final_df["created_date"])
        to_del = []
        for col in final_df:
            for coll in final_df:
                if col != coll:
                    if col.__contains__(coll):
                        final_df[col] += final_df[coll]
                        to_del.append(coll)
                    elif coll.__contains__(col):
                        final_df[coll] += final_df[col]
                        to_del.append(col)
        if len(to_del) > 0:
            final_df = final_df.drop(list(set(to_del)), axis=1)
        final_df.plot.area(x="created_date", xlabel="week num", ylabel="quantity", stacked=True, color=colors, title=k,
                           alpha=0.8)
        # Lighten borders
        plt.gca().spines["top"].set_alpha(0)
        plt.gca().spines["bottom"].set_alpha(.3)
        plt.gca().spines["right"].set_alpha(0)
        plt.gca().spines["left"].set_alpha(.3)
    plt.show()


def ner_qnty_plot(posts_file_path):
    # keys_replace_dict = {"joe biden 's": }
    df = pd.read_csv(posts_file_path)
    # fig, axes = plt.subplots(nrows=5, ncols=2)
    # df = df.drop(df[df["Topic"] == -1].index)
    # df["month"] = df["created_date"].apply(lambda x: x.split("-")[1])
    time_line_df = pd.DataFrame([(datetime.date(2020, 1, 3), "Soleimani \\nelimination"),
                                 (datetime.date(2020, 3, 13), "Covid-19 \\nNational emergency"),
                                 (datetime.date(2020, 4, 9), "Bernie Sanders \\nDrops Out"),
                                 (datetime.date(2020, 5, 26), 'Protests following \\nGeorge Floyd death'),
                                 (datetime.date(2020, 9, 29), 'First \\nPresidential \\nDebate'),
                                 (datetime.date(2020, 10, 22), 'Third \\nPresidential \\nDebate'),
                                 (datetime.date(2020, 11, 3), 'Election \\nDay'),
                                 ],
                                columns=["date", "event"])
    time_line_df["date"] = pd.to_datetime(time_line_df["date"])
    # time_line_df["date"] = time_line_df["date"].apply(lambda x: x.isocalendar()[1])

    ind = read_json("C:/Users/shimon/PycharmProjects/Reddit_Project/text_analysis/top_inverted_index")
    # unique_dates = range(1, 13)
    unique_dates = df["created_date"].unique().tolist()
    # unique_dates = df["created_date"].apply(
    #     lambda x: datetime.date(int(x.split("-")[0]), int(x.split("-")[1]), int(x.split("-")[2])).isocalendar()[
    #         1]).unique().tolist()
    unique_dates = sorted(unique_dates)
    charts = []
    for k, v in ind.items():
        final_df = pd.DataFrame(unique_dates, columns=["created_date"])
        data_items = v.items()
        data_list = list(data_items)
        df_ind = pd.DataFrame(data_list)
        df_ind["count_posts_per_ent"] = df_ind.iloc[:, 1].apply(lambda x: len(x))
        date_dict = {}
        entities = df_ind.iloc[:, 0].values.tolist()
        for ent in entities:
            date_dict[ent] = df.loc[df["post_id"].isin(df_ind[df_ind.iloc[:, 0] == ent][1].values.tolist()[0])][
                "created_date"].values.tolist()
            post_date_tmp = pd.DataFrame([df_ind.iloc[:, 1].values.tolist()[0], date_dict[ent]])
            post_date_tmp = post_date_tmp.transpose()
            post_date_tmp.columns = [ent, "created_date"]
            post_date_tmp = post_date_tmp.dropna()
            # post_date_tmp["created_date"] = post_date_tmp.loc[:, "created_date"].apply(
            #     lambda x: datetime.date(int(x.split("-")[0]), int(x.split("-")[1]), int(x.split("-")[2])).isocalendar()[
            #         1])  # .apply(lambda x: x.split("-")[1])

            count_date = post_date_tmp.groupby("created_date", as_index=False).count()
            final_df = final_df.join(count_date, how="outer", rsuffix="_drop")
            final_df.drop([col for col in final_df.columns if 'drop' in col], axis=1, inplace=True)
            final_df = final_df.fillna(0)
        final_df["created_date"] = pd.to_datetime(final_df["created_date"])
        # final_df["created_date"] = final_df["created_date"].apply(
        #     lambda x: (datetime.date(2020, 1, 1) + relativedelta(weeks=+x - 1)).month)

        to_del = []
        for col in final_df:
            for coll in final_df:
                if col != coll:
                    if col.__contains__(coll):
                        final_df[col] += final_df[coll]
                        to_del.append(coll)
                    elif coll.__contains__(col):
                        final_df[coll] += final_df[col]
                        to_del.append(col)
        if len(to_del) > 0:
            final_df = final_df.drop(list(set(to_del)), axis=1)
        entities = final_df.columns.tolist()
        months = final_df["created_date"].values.tolist()
        entities.remove("created_date")
        final_df_viz = pd.DataFrame([[ww for i, ww in final_df["created_date"].iteritems() for w in entities],
                                     len(unique_dates) * entities,
                                     final_df[entities].to_numpy().flatten().tolist(),
                                     [ww for ww in months for w in entities]],
                                    index=["created_date", "entity", "count", "month num"]).transpose()
        selection = alt.selection_multi(fields=entities, bind='legend')

        chart = alt.Chart(final_df_viz, title=f"{k} volume in 2020").mark_area().encode(
            alt.X("week(created_date):T",
                  axis=alt.Axis(domain=False, tickSize=0), title="Date"),
            alt.Y('sum(count):Q', stack='center', axis=None, title="Posts count"),
            alt.Color('entity:N'),
            opacity=alt.condition(selection, alt.value(1), alt.value(0.2))
        ).add_selection(
            selection
        )

        chart.configure_header(
            titleColor='green',
            titleFontSize=14,

        )
        rules = alt.Chart(time_line_df).mark_rule().encode(
            x='week(date):T',
            size=alt.value(1)
        )
        text = alt.Chart(time_line_df).mark_text(align='center', dy=-200, fontSize=10, fontWeight=100, opacity=0.7, lineBreak=r'\n'
        ).encode(
            x='week(date):T',
            text='event'
        )
        chart = chart+rules+text
        chart.save(f"{k}.html")
        # st.altair_chart(chart)
        # charts.append(chart)
    # altair_viewer.display(charts)        #
    # final_df.plot.area(x="created_date", xlabel="week num", ylabel="quantity", stacked=True, color=colors, title=k,
    #                    alpha=0.8)
    # # plt.bar_label(axes.containers[-1], labels=time_line_df["event"])
    # # Lighten borders
    # ax = plt.gca()
    #
    # for date_point, label in time_line_df.values.tolist():
    #     plt.axvline(x=date_point)
    #     plt.text(date_point, ax.get_ylim()[1] - 4, label,
    #              horizontalalignment='center',
    #              verticalalignment='center', fontdict=dict(size=7),
    #              bbox=dict(facecolor='white', alpha=0.5))
    # plt.gca().spines["top"].set_alpha(0)
    # plt.gca().spines["bottom"].set_alpha(.3)
    # plt.gca().spines["right"].set_alpha(0)
    # plt.gca().spines["left"].set_alpha(.3)
    # plt.show()


def ner_top(cut_num):
    index = read_json(r"C:\Users\shimon\PycharmProjects\Reddit_Project\text_analysis\inverted_index")
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
# export_bert_plots_bars(r"data_with_time.csv")
# export_bert_plots_scatter(r"data_with_time.csv")
# ner_inverted_index(r"data_with_time.csv")
# r = read_json("inverted_index")
# print("--")
ner_top(4000)
ner_qnty_plot(r"C:\Users\shimon\PycharmProjects\Reddit_Project\text_analysis\data_with_time.csv")
