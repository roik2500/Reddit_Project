import pandas as pd
# limit = 40000
# final_df = pd.DataFrame(columns=["permalink", "id", "is_crosspostable", "removed_by_category",
#                                                  "is_robot_indexable", "link_flair_richtext", "selftext"])
# for i in range(5000,limit,5000):
#    final_df = final_df.append(pd.read_csv("data_{}.csv".format(i)))
# final_df.to_csv("final_data.csv")

reddit_df = pd.read_csv("data_35000.csv")
pushshift_df = pd.read_csv("data_35000_psio1.csv")

new_df = pushshift_df.set_index("id").join(reddit_df.set_index("id"))
new_df.to_csv("joined.csv")