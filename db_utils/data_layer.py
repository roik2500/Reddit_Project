import pymongo
import logging

logging.getLogger().setLevel(logging.WARN)
logging.basicConfig(format='%(asctime)s %(message)s')


class DataLayer:
    def __init__(self, connection_string):
        self.client = pymongo.MongoClient(connection_string)
        self.logger = logging.getLogger("DataLayer")

    def get_collection(self, year, collection_name, submission_kind):
        db = self.client[f"reddit_{year}"].with_options(write_concern=pymongo.WriteConcern(w=0))
        collection = db[f"{collection_name}_{submission_kind}"]
        return collection

    def get_collection_cursor(self, year, collection_name, submission_kind, fields_list=None):
        collection = self.get_collection(self, year, collection_name, submission_kind)
        return collection.find({}, {field: 1 for field in fields_list})

    @staticmethod
    def get_post_id(submission):
        return submission["post_id"]

    @staticmethod
    def get_status(submission):
        return submission["status"]

    @staticmethod
    def get_date(submission):
        return submission["reddit_api"]["created_utc"]

    def get_title(self, submission):
        try:
            sub_submission = submission["reddit_api"]
        except KeyError as e:
            sub_submission = submission["pushift_api"]
        if 'title' in sub_submission:
            return sub_submission['title']
        else:
            self.logger.warning(
                f"{self.get_post_id(submission)} is a comment and does not contains title")
            return ''

    def get_selftext(self, sub_kind, submission):
        sub_submission = ''
        if sub_kind == 'post':
            selftext = "selftext"
        else:
            selftext = "body"
        if "pushift_api" in submission:
            if selftext in submission["pushift_api"]:
                sub_submission = submission["pushift_api"]
            else:
                self.logger.warning(
                    f"{self.get_post_id(submission)} has the same text in pushift and reddit")
                sub_submission = submission["reddit_api"]
        else:
            self.logger.warning(
                f"{self.get_post_id(submission)} does not contains pushift_api field. you got reddit_api selftext")
            sub_submission = submission["reddit_api"]
        return sub_submission[selftext]
