'''
Google Cloud Function (Main: hello_pubsub)
1. Triggered by Pub/Sub via arc event push
2. Publish count_recent message to Pub/Sub topic to get count filtered tweet for the recent 7 days
'''

import base64, functions_framework, os, requests
from google.cloud import pubsub_v1

"""
Constants
"""
project_id=os.getenv('GOOGLE_CLOUD_PROJECT')
bearer=os.getenv('bearer')
topic='pubsub-test' # Pub/Sub toppic that will receive recent Twitter count
search_url = "https://api.twitter.com/2/tweets/counts/recent" # Twitter API end point

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer}"
    r.headers["User-Agent"] = "v2RecentTweetCountsPython"
    return r


def connect_to_endpoint(url, params):
    '''
    connnect to API end point end get response
    '''
    response = requests.request("GET", search_url, auth=bearer_oauth, params=params)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    # Get query from arc event
    name = base64.b64decode(cloud_event.data["message"]["data"])
    query_params = {'query': name}

    # Connect to API end point and get response
    json_response = connect_to_endpoint(search_url, query_params)
    tweet_count = json_response.get('meta').get('total_tweet_count')

    # Establish connection to Pub/Sub
    publisher = pubsub_v1.PublisherClient() 
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
        project_id=os.getenv('GOOGLE_CLOUD_PROJECT'),
        topic='pubsub-test',
    )

    # Publish message to Pub/Sub topic specified above as a bytestring
    future = publisher.publish(topic_name, str(tweet_count).encode("utf-8"))
    future.result()
    return f"Count tweet: {tweet_count} for keyword: {name}"