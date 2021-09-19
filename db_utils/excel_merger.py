import pandas as pd
# limit = 40000
# final_df = pd.DataFrame(columns=["permalink", "id", "is_crosspostable", "removed_by_category",
#                                                  "is_robot_indexable", "link_flair_richtext", "selftext"])
# for i in range(5000,limit,5000):
#    final_df = final_df.append(pd.read_csv("data_{}.csv".format(i)))
# final_df.to_csv("final_data.csv")
#
# reddit_df = pd.read_csv("data_35000.csv")
# pushshift_df = pd.read_csv("data_35000_psio1.csv")
#
# new_df = pushshift_df.set_index("id").join(reddit_df.set_index("id"))
# new_df.to_csv("joined.csv")

folder_path = 'C:\\Users\\User\\Documents\\FourthYear\\Project\\resources\\files\\'

removed_df = pd.read_csv(folder_path+"Removed_NER_emotion_rate_mean_wallstreetbets.csv")
not_removed_df = pd.read_csv(folder_path+"NotRemoved_NER_emotion_rate_mean_wallstreetbets.csv")
all_df = pd.read_csv(folder_path+"All_NER_emotion_rate_mean_wallstreetbets.csv")

new_df = removed_df.set_index("id").join(not_removed_df.set_index("id"))
merge_df = new_df.set_index("id").join(all_df.set_index("id"))
merge_df.to_csv(folder_path+"merge.csv")