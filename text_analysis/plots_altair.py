import json
import datetime
import pathlib
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
import matplotlib.pyplot as plt


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


def topics_sntim_status(posts_file_path, time_line_df, time_resolution='date'):
    data = pd.read_csv(posts_file_path)
    data = data.drop(data[data["Topic"] == -1].index)
    count_gb = data.groupby("Topic").count()
    data = data.drop(data[data["Topic"].isin(count_gb[count_gb["Count"] < 3000].index)].index)
    # selection = alt.selection_multi(fields=list(data.columns), bind='legend')
    time_line_df["date"] = pd.to_datetime(time_line_df["date"])
    sntiments = ["joy", "surprise", "anger", "disgust", "fear", "sadness", "bertweet_neg", "bertweet_pos", "hate",
                 "offensive"]
    for col in tqdm(sntiments):
        for topic in data["Name"].unique().tolist():
            chart = alt.Chart(data, title=f"{col.upper()} in {topic} volume in 2020").mark_area().encode(
                alt.X(f"{time_resolution}(created_date):T",
                      axis=alt.Axis(domain=False, tickSize=0), title="Date"),
                alt.Y(f'mean({col}):Q', stack='center', axis=None, title=f"{col} mean"),
                alt.Color(f'status:N', scale=alt.Scale(scheme='tableau20'), legend=alt.Legend(columns=1, labelLimit=0)),
                size=alt.value(32)
                # opacity=alt.condition(selection, alt.value(1), alt.value(0.2))
            ).transform_filter(
                alt.FieldEqualPredicate(field='Name', equal=topic)
            ).properties(
                width=1200,
                height=400
            )
            # .add_selection(
            #     selection
            # )

            chart.configure_header(
                titleColor='green',
                titleFontSize=14,

            )
            rules = alt.Chart(time_line_df).mark_rule().encode(
                x=f"{time_resolution}(date):T",
                size=alt.value(1)
            )
            text = alt.Chart(time_line_df).mark_text(align='center', dy=-200, fontSize=8, fontWeight=100, opacity=0.7,
                                                     lineBreak=r'\n'
                                                     ).encode(
                x=f"{time_resolution}(date):T",
                text='event'
            )
            chart = chart + rules + text
            dir = fr"G:\.shortcut-targets-by-id\1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d\final_project\hackaton\Altair_plots\{col}"
            pathlib.Path(dir).mkdir(parents=True, exist_ok=True)
            chart.save(f"{dir}\{topic}.pdf")
            # st.altair_chart(chart)


def topics_sntim_plot(posts_file_path, time_line_df, time_resolution='date'):
    data = pd.read_csv(posts_file_path)
    data = data.drop(data[data["Topic"] == -1].index)
    count_gb = data.groupby("Topic").count()
    data = data.drop(data[data["Topic"].isin(count_gb[count_gb["Count"] < 3000].index)].index)
    # selection = alt.selection_multi(fields=list(data.columns), bind='legend')
    time_line_df["date"] = pd.to_datetime(time_line_df["date"])
    # sntiments = ["joy", "surprise", "anger", "disgust", "fear", "sadness", "bertweet_neg", "bertweet_pos", "hate", "offensive"]
    sntiments = ["hate"]
    for col in sntiments:
        chart = alt.Chart(data, title=f"{col.upper()} volume in 2020").mark_area().encode(
            alt.X(f"{time_resolution}(created_date):T",
                  axis=alt.Axis(domain=False, tickSize=0), title="Date"),
            alt.Y(f'mean({col}):Q', stack='center', axis=None, title=f"{col} mean"),
            alt.Color('Name:N', scale=alt.Scale(scheme='tableau20'), legend=alt.Legend(columns=1, labelLimit=0)),
            size=alt.value(32)
            # opacity=alt.condition(selection, alt.value(1), alt.value(0.2))
        ).properties(
            width=1200,
            height=400
        )
        # .add_selection(
        #     selection
        # )

        chart.configure_header(
            titleColor='green',
            titleFontSize=14,

        )
        rules = alt.Chart(time_line_df).mark_rule().encode(
            x=f"{time_resolution}(date):T",
            size=alt.value(1)
        )
        text = alt.Chart(time_line_df).mark_text(align='center', dy=-200, fontSize=8, fontWeight=100, opacity=0.7,
                                                 lineBreak=r'\n'
                                                 ).encode(
            x=f"{time_resolution}(date):T",
            text='event'
        )
        chart = chart + rules + text
        chart.save(f"{col}.html")
        # st.altair_chart(chart)


def topic_qnty_plot(posts_file_path, time_line_df):
    data = pd.read_csv(posts_file_path)
    data = data.drop(data[data["Topic"] == -1].index)
    count_gb = data.groupby("Topic").count()
    data = data.drop(data[data["Topic"].isin(count_gb[count_gb["Count"] < 3000].index)].index)
    # selection = alt.selection_multi(fields=list(data.columns), bind='legend')
    time_line_df["date"] = pd.to_datetime(time_line_df["date"])

    chart = alt.Chart(data, title="Topics volume in 2020").mark_area().encode(
        alt.X("week(created_date):T",
              axis=alt.Axis(domain=False, tickSize=0), title="Date"),
        alt.Y('count(post_id):Q', stack='center', axis=None, title="Posts count"),
        alt.Color('Name:N', scale=alt.Scale(scheme='tableau20'), legend=alt.Legend(columns=2, symbolLimit=0)),
        size=alt.value(32)
        # opacity=alt.condition(selection, alt.value(1), alt.value(0.2))
    ).properties(
        width=1200,
        height=400
    )
    # .add_selection(
    #     selection
    # )

    chart.configure_header(
        titleColor='green',
        titleFontSize=14,

    )
    rules = alt.Chart(time_line_df).mark_rule().encode(
        x='week(date):T',
        size=alt.value(1)
    )
    text = alt.Chart(time_line_df).mark_text(align='center', dy=-200, fontSize=8, fontWeight=100, opacity=0.7,
                                             lineBreak=r'\n'
                                             ).encode(
        x='week(date):T',
        text='event'
    )
    chart = chart + rules + text
    chart.save("Topics.html")
    # st.altair_chart(chart)


def ner_sentim_plot(posts_file_path, time_line_df):
    # keys_replace_dict = {"joe biden 's": }
    df = pd.read_csv(posts_file_path)
    # fig, axes = plt.subplots(nrows=5, ncols=2)
    # df = df.drop(df[df["Topic"] == -1].index)
    # df["month"] = df["created_date"].apply(lambda x: x.split("-")[1])
    time_line_df["date"] = pd.to_datetime(time_line_df["date"])
    # time_line_df["date"] = time_line_df["date"].apply(lambda x: x.isocalendar()[1])

    ind = read_json("C:/Users/shimon/PycharmProjects/Reddit_Project/text_analysis/top_inverted_index")
    # unique_dates = range(1, 13)
    unique_dates = df["created_date"].unique().tolist()
    # unique_dates = df["created_date"].apply(
    #     lambda x: datetime.date(int(x.split("-")[0]), int(x.split("-")[1]), int(x.split("-")[2])).isocalendar()[
    #         1]).unique().tolist()
    unique_dates = sorted(unique_dates)
    sntiments = ["joy", "surprise", "anger", "disgust", "fear", "sadness", "bertweet_neg", "bertweet_pos", "hate",
                 "offensive"]
    for col in sntiments:
        for k, v in ind.items():
            # final_df = pd.DataFrame(unique_dates, columns=["created_date"])
            data_items = v.items()
            data_list = list(data_items)
            df_ind = pd.DataFrame(data_list)
            df_ind["count_posts_per_ent"] = df_ind.iloc[:, 1].apply(lambda x: len(x))
            entities = df_ind.iloc[:, 0].values.tolist()
            to_del = []
            for coll in entities:
                for colll in entities:
                    if coll != colll:
                        if coll.__contains__(colll):

                            df_ind.loc[df_ind[0] == coll, 1].values.tolist().extend(df_ind.loc[df_ind[0] == colll, 1].values.tolist())
                            to_del.append(colll)
                        elif colll.__contains__(coll):
                            df_ind.loc[df_ind[0] == colll, 1].values.tolist().extend(df_ind.loc[df_ind[0] == coll, 1].values.tolist())
                            to_del.append(coll)
            if len(to_del) > 0:
                for x in list(set(to_del)):
                    entities.remove(x)
            for ent in entities:
                sentim_col = df.loc[df["post_id"].isin(df_ind[df_ind.iloc[:, 0] == ent][1].values.tolist()[0])][
                    col].values.tolist()
                status_col = df.loc[df["post_id"].isin(df_ind[df_ind.iloc[:, 0] == ent][1].values.tolist()[0])][
                    "status"].values.tolist()
                date_col = df.loc[df["post_id"].isin(df_ind[df_ind.iloc[:, 0] == ent][1].values.tolist()[0])][
                    "created_date"].values.tolist()
                post_date_tmp = pd.DataFrame([df_ind.iloc[:, 1].values.tolist()[0], date_col, sentim_col, status_col])
                post_date_tmp = post_date_tmp.transpose()
                post_date_tmp.columns = [ent, "created_date", col, "status"]
                post_date_tmp = post_date_tmp.dropna()
                # post_date_tmp["created_date"] = post_date_tmp.loc[:, "created_date"].apply(
                #     lambda x: datetime.date(int(x.split("-")[0]), int(x.split("-")[1]), int(x.split("-")[2])).isocalendar()[
                #         1])  # .apply(lambda x: x.split("-")[1])

                # count_date = post_date_tmp.groupby("created_date", as_index=False).count()
                post_date_tmp[col] = post_date_tmp[col].astype(float)
                mean_col = post_date_tmp.groupby(["created_date", "status"], as_index=False).mean()
                final_df = mean_col
                # final_df = final_df.join(mean_col[[col, "status"]], how="outer", rsuffix="_drop")
                # final_df.drop([col for col in final_df.columns if 'drop' in col], axis=1, inplace=True)
                # final_df = final_df.fillna(0)
                final_df["created_date"] = pd.to_datetime(final_df["created_date"])
                chart = alt.Chart(final_df, title=f"{col} volume on {ent} in 2020").mark_area().encode(
                    alt.X("week(created_date):T",
                          axis=alt.Axis(domain=False, tickSize=0), title="Date"),
                    alt.Y(f'mean({col}):Q', stack='center', axis=None, title=f"mean {col}"),
                    alt.Color('status:N')
                ).properties(
                    width=1200,
                    height=400
                )
                rules = alt.Chart(time_line_df).mark_rule().encode(
                    x='week(date):T',
                    size=alt.value(1)
                )
                text = alt.Chart(time_line_df).mark_text(align='center', dy=-200, fontSize=8, fontWeight=100,
                                                         opacity=0.7,
                                                         lineBreak=r'\n'
                                                         ).encode(
                    x='week(date):T',
                    text='event'
                )
                chart = chart + rules + text
                dirr = fr"G:\.shortcut-targets-by-id\1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d\final_project\hackaton\Altair_plots\ner\{k}\{ent}"
                pathlib.Path(dirr).mkdir(parents=True, exist_ok=True)
                chart.save(f"{dirr}\{col}.html")
                # st.altair_chart(chart)

            # entities = final_df.columns.tolist()
            # months = final_df["created_date"].values.tolist()
            # entities.remove("created_date")
            # final_df_viz = pd.DataFrame([[ww for i, ww in final_df["created_date"].iteritems() for w in entities],
            #                              len(unique_dates) * entities,
            #                              final_df[entities].to_numpy().flatten().tolist(),
            #                              [ww for ww in months for w in entities]],
            #                             index=["created_date", "entity", "count", "month num"]).transpose()


def ner_qnty_plot(posts_file_path, time_line_df):
    # keys_replace_dict = {"joe biden 's": }
    df = pd.read_csv(posts_file_path)
    # fig, axes = plt.subplots(nrows=5, ncols=2)
    # df = df.drop(df[df["Topic"] == -1].index)
    # df["month"] = df["created_date"].apply(lambda x: x.split("-")[1])
    time_line_df["date"] = pd.to_datetime(time_line_df["date"])
    # time_line_df["date"] = time_line_df["date"].apply(lambda x: x.isocalendar()[1])

    ind = read_json("C:/Users/shimon/PycharmProjects/Reddit_Project/text_analysis/top_inverted_index")
    # unique_dates = range(1, 13)
    unique_dates = df["created_date"].unique().tolist()
    # unique_dates = df["created_date"].apply(
    #     lambda x: datetime.date(int(x.split("-")[0]), int(x.split("-")[1]), int(x.split("-")[2])).isocalendar()[
    #         1]).unique().tolist()
    unique_dates = sorted(unique_dates)
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
        ).properties(
            width=1200,
            height=400
        )

        chart.configure_header(
            titleColor='green',
            titleFontSize=14,

        )
        rules = alt.Chart(time_line_df).mark_rule().encode(
            x='week(date):T',
            size=alt.value(1)
        )
        text = alt.Chart(time_line_df).mark_text(align='center', dy=-200, fontSize=8, fontWeight=100, opacity=0.7,
                                                 lineBreak=r'\n'
                                                 ).encode(
            x='week(date):T',
            text='event'
        )
        chart = chart + rules + text
        chart.save(f"{k}.html")
        # st.altair_chart(chart)


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
time_line_df_politics = pd.DataFrame([(datetime.date(2020, 1, 3), "\\n\\nSoleimani \\nelimination"),
                                      (datetime.date(2020, 3, 13), "Covid-19 \\nNational emergency"),
                                      (datetime.date(2020, 4, 9), "\\n\\nBernie Sanders \\nDrops Out"),
                                      (datetime.date(2020, 5, 26), 'Protests \\nfollowing \\nGeorge \\nFloyd \\ndeath'),
                                      (datetime.date(2020, 6, 6),
                                       '\\n\\n\\n\\nBiden \\nbecomes \\nDemocratic \\npresidential \\nnominee'),
                                      (datetime.date(2020, 8, 11),
                                       'Kamala Harris \\nchosen as \\nDemocratic VP \\ncandidate'),
                                      (datetime.date(2020, 10, 2),
                                       '\\n\\n\\n\\nTrump tests\\n positive \\nfor COVID-19'),
                                      (datetime.date(2020, 9, 29), 'First \\nPresidential \\nDebate'),
                                      (datetime.date(2020, 10, 22), '\\n\\nThird \\nPresidential \\nDebate'),
                                      (datetime.date(2020, 11, 3), 'Election \\nDay'),
                                      ],
                                     columns=["date", "event"])
data_path = r"G:\.shortcut-targets-by-id\1Zr_v9ggL0ZP7j6DJeTQggwxX7BPmEJ-d\final_project\BertTopic\data for bert\politics\model\model_15_300_0.6844\finalData_with_probas_with_time.csv"
ner_sentim_plot(data_path, time_line_df_politics)
# ner_qnty_plot(data_path, time_line_df_politics)
# topic_qnty_plot(data_path, time_line_df_politics)
# topics_sntim_status(data_path, time_line_df_politics, "week")
# topics_sntim_plot(data_path, time_line_df_politics, "week")
