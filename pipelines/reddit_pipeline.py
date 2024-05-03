from utils.constants import CLIENT_ID, SECRET
from etls.reddit_etl import connect_reddit


def reddit_pipeline(filename: str, subreddit: str, time_filter='day', limit=None):
    # connecting to reddit instance
    instance = connect_reddit(CLIENT_ID,SECRET,'Airscholar Agent')
    # extraction
    # transformation
    # loading into csv
