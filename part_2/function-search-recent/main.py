'''
Google Cloud Function (Main: hello_pubsub)
1. Triggered by Pub/Sub via arc event push
2. Publish count_recent message to Pub/Sub topic to search recent tweet for the past 7 days
3. Load data to Big Query
'''
import base64, pandas as pd, json, requests, pandas_gbq, os, datetime
from pandas.io import gbq
from google.cloud import bigquery

'''
CONSTANT
'''
project_id=os.getenv('GOOGLE_CLOUD_PROJECT')
dataset_name=os.getenv('dataset_name') # BigQuery dataset
table_name=os.getenv('table_name') # BigQuery table
bearer=os.getenv('bearer')
search_url = "https://api.twitter.com/2/tweets/search/recent"  # Twitter API end point

current_time = datetime.datetime.utcnow()
start_time = (current_time - datetime.timedelta(minutes=15)).isoformat("T") + "Z"

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer}"
    r.headers["User-Agent"] = "v2RecentTweetCountsPython"
    return r


def connect_to_endpoint(url, params, next_token = None):
    '''
    connnect to API end point end get response
    '''
    params['next_token'] = next_token
    response = requests.request("GET", search_url, auth=bearer_oauth, params=params)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def bq_load (table_name, value):
    client = bigquery.Client(project=project_id)

    table_id = '{}.{}.{}'.format(project_id, dataset_name, table_name)

    job_config = bigquery.LoadJobConfig( 
        write_disposition='WRITE_TRUNCATE'
    )
    job = client.load_table_from_json(
        value, table_id, job_config=job_config
    )
    job.result()
# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
# if __name__ == "__main__":
    # Get query from arc event
    name = base64.b64decode(cloud_event.data["message"]["data"])
    # name = 'azure'

    # Build search query
    query_params = {'query': name
        , 'start_time': start_time
        , 'max_results' : 100
        , 'expansions':'author_id'
        , 'user.fields' : 'id,username'
        , 'tweet.fields': 'id,text,author_id,created_at,public_metrics'
        , 'next_token' : {}}

    flag = True
    next_token = None # For pagination
    count = 0 # Get total count
    tweet = []
    user = []
    
    while flag:
        # Connect to API end point and get response
        json_response = connect_to_endpoint(search_url, query_params, next_token)
        result_count = json_response.get('meta').get('result_count') 

        if result_count == 0:
            # Break if no tweet is returned
            flag = False
        else:
            # Pagination. Get next response if any, and append to the list
            if 'next_token' in json_response['meta']:
                  next_token = json_response['meta']['next_token']
                  count += result_count
                  
                  # Divide response into two parts, tweet and user as they have different length in response
                  tweet += json_response['data']
                  user += json_response['includes']['users']
            
            else:
                  len_tweet = len(json_response['data'])
                  count += result_count
                  if len_tweet > 0:
                    tweet += json_response['data']
                    user += json_response['includes']['users']

                  flag = False

    # Append user list dict to tweet list dic with join key on user id
    [x.update(y) for x in tweet for y in user if x['author_id'] == y['id']]

    # Add to Big Query
    bq_load(table_name,tweet)
    
    return f"Counting for keyword: {name}, total count: {count}"