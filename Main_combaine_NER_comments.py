from text_analysis.name_entity_recognition import NameEntity
import os
from dotenv import load_dotenv
from statistics_analysis.Statistics_deleted import Statistic

from db_utils.Con_DB import Con_DB
from db_utils.FileReader import FileReader
from text_analysis.emotion_detection import EmotionDetection
from api_utils.PushshiftApi import PushshiftApi

load_dotenv()


def get_comment_quantity_by_posts_ids(NER_posts_list, con_db):
    s = Statistic()
    objects = con_db.get_posts_by_ids(posts_ids=NER_posts_list)
    all_comments = []
    for _object in objects:
        res = con_db.get_text_from_post_OR_comment(_object, post_or_comment='comment')
        if res != []:
            all_comments += res

    objects = con_db.get_posts_by_ids(posts_ids=NER_posts_list)
    removed_comments = s.get_removed_quantity(objects, post_or_comment_str='comment',
                                              dict_month=s.comment_quantity_per_month, dict_day= s.comment_quantity_per_day)
    return [all_comments, removed_comments]



def get_removed_comments_quntity_by_NER(con_db, file_reader, removed_post_json_NER_path, notRemoved_post_json_NER_path,
                                        emotion_detection, PLOT_PATH, comment_resources, categorys):

    json_removed_file = file_reader.read_from_json_to_dict(removed_post_json_NER_path)
    quntity_of_removed_NER_post = json_removed_file['ORG']['pltr'][0]
    NER_removed_post_list = json_removed_file['ORG']['pltr'][1]


    json_NotRemoved_file = file_reader.read_from_json_to_dict(notRemoved_post_json_NER_path)
    quntity_of_NotRemoved_NER_post = json_NotRemoved_file['ORG']['pltr'][0]
    NER_notRemoved__post_list = json_NotRemoved_file['ORG']['pltr'][1]

    comment_from_removed_posts, removed_comment_from_removed_posts = get_comment_quantity_by_posts_ids(NER_removed_post_list, con_db)

    comment_from_notRemoved_posts, removed_comment_from_notRemoved_posts = get_comment_quantity_by_posts_ids(NER_notRemoved__post_list, con_db)

    print('quntity_of_removed_NER_post', quntity_of_removed_NER_post)
    print('number_of_comment_from_removed_post', len(comment_from_removed_posts))
    print('len removed_comment_from_removed_posts', sum(removed_comment_from_removed_posts.values()))
    # print('len removed_comment_from_removed_posts', removed_comment_from_removed_posts)

    print('quntity_of_NotRemoved_NER_post', quntity_of_NotRemoved_NER_post)
    print('number_of_comment_from_notRemoved_post', len(comment_from_notRemoved_posts))
    print('len removed_comment_from_notRemoved_posts', sum(removed_comment_from_notRemoved_posts.values()))
    # print('len removed_comment_from_notRemoved_posts', removed_comment_from_notRemoved_posts)

    removed_comment_from_removed_posts = []
    lst = []
    for comment in comment_from_removed_posts:
        if comment[-1]:
            removed_comment_from_removed_posts.append(comment[2])
            lst.append(comment[3])

    # pmaw = PushshiftApi()
    # lst = pmaw.api_pmaw.search_submission_comment_ids(ids=lst)
    removed_comment_from_removed_posts = con_db.get_specific_items_by_object_ids(
        ids_list=removed_comment_from_removed_posts,
        post_or_comment_arg='comment')

    removed_comment_from_notRemoved_posts = []
    for comment in comment_from_notRemoved_posts:
        if comment[-1]:
            removed_comment_from_notRemoved_posts.append(comment[2])

    removed_comment_from_notRemoved_posts = con_db.get_specific_items_by_object_ids(ids_list=removed_comment_from_notRemoved_posts,
                                                                                    post_or_comment_arg='comment')
    emotion_detection.get_plot_and_emotion_rate_from_all_posts_in_category(data_cursor=removed_comment_from_removed_posts,
                                                                           Con_DB=con_db, category='Removed',
                                                                           path_to_save_plt=PLOT_PATH + 'Removed/',
                                                                           subreddit_name=os.getenv('COLLECTION_NAME'),
                                                                           post_need_to_extract=False,
                                                                           post_or_comment_arg='comment',
                                                                           file_reader=file_reader,
                                                                           resources=comment_resources)

    emotion_detection.get_plot_and_emotion_rate_from_all_posts_in_category(data_cursor=removed_comment_from_notRemoved_posts,
                                                                           Con_DB=con_db, category='NotRemoved',
                                                                           path_to_save_plt=PLOT_PATH + 'NotRemoved/',
                                                                           subreddit_name=os.getenv('COLLECTION_NAME'),
                                                                           post_need_to_extract=False,
                                                                           post_or_comment_arg='comment',
                                                                           file_reader=file_reader,
                                                                           resources=comment_resources)

if __name__ == '__main__':

    COLLECTION_NAME = os.getenv("COLLECTION_NAME")
    PATH_DRIVE = os.getenv("OUTPUTS_DIR") + 'emotion_detection/'
    POST_PATH = PATH_DRIVE + 'posts/'
    COMMENT_PATH = PATH_DRIVE + 'comments/'

    post_resources = POST_PATH + 'resources/'
    comment_resources = COMMENT_PATH + 'resources/'


    removed_post_json_NER_path = post_resources + 'RemovedNER_quantity_byType.json'

    notRemoved_post_json_NER_path = post_resources + 'NotRemovedNER_quantity_byType.json'

    PLOT_PATH = COMMENT_PATH + 'plots/'

    con_db = Con_DB()
    file_reader = FileReader()
    emotion_detection = EmotionDetection()

    get_removed_comments_quntity_by_NER(con_db, file_reader, removed_post_json_NER_path, notRemoved_post_json_NER_path,
                                        emotion_detection, PLOT_PATH, comment_resources)

    ' elon mask has move tesla factory to texses so invest in $tsla'

    '''
    tesla
    quntity_of_removed_NER_post 2022
    number_of_comment_from_removed_post 14603
    len removed_comment_from_removed_post 354
    
    quntity_of_NotRemoved_NER_post 3785
    number_of_comment_from_notRemoved_post 72580
    len removed_comment_from_notRemoved_post 3162
    
    
    tsla
    quntity_of_removed_NER_post 1652
    number_of_comment_from_removed_post 11767
    len removed_comment_from_removed_posts 100
    
    quntity_of_NotRemoved_NER_post 3248
    number_of_comment_from_notRemoved_post 73899
    len removed_comment_from_notRemoved_posts 3723
    
    
    elon
    quntity_of_removed_NER_post 461
    number_of_comment_from_removed_post 4518
    len removed_comment_from_removed_posts 90
    quntity_of_NotRemoved_NER_post 1277
    number_of_comment_from_notRemoved_post 25975
    len removed_comment_from_notRemoved_posts 1284
    
    
    pltr
    quntity_of_removed_NER_post 3672
    number_of_comment_from_removed_post 26748
    len removed_comment_from_removed_posts 614
    quntity_of_NotRemoved_NER_post 5406
    number_of_comment_from_notRemoved_post 95480
    len removed_comment_from_notRemoved_posts 4158
    '''

