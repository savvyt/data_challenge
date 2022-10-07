'''
Google Cloud Function (Main: hello_pubsub)
1. Triggered by Pub/Sub via arc event push
2. Publish count_recent message to Pub/Sub topic to search recent tweet for the past 7 days
3. Load data to Big Query
'''
import base64, functions_framework, pandas as pd, json, requests, pandas_gbq, os, datetime
from pandas.io import gbq

'''
CONSTANT
'''
project_id=os.getenv('GOOGLE_CLOUD_PROJECT')
dataset_name=os.getenv('dataset_name') # BigQuery dataset (similar to database)
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

def bq_load(table_name, value):
    '''
    function 3: This function just converts your pandas dataframe into a bigquery table, 
    you'll also need to designate the name and location of the table in the variable 
    names below.
    '''
    value.to_gbq(destination_table='{}.{}'.format(dataset_name, table_name), project_id=project_id, if_exists='replace')

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    # Get query from arc event
    name = base64.b64decode(cloud_event.data["message"]["data"])

    # Build search query
    query_params = {'query': name
        , 'start_time': start_time
        , 'max_results' : 100
        , 'expansions':'author_id'
        , 'user.fields' : 'username,id'
        , 'tweet.fields': 'author_id,public_metrics,created_at'
        , 'next_token' : {}}

    flag = True
    next_token = None # For pagination
    count = 0 # Get total count
    
    while flag:
        # Connect to API end point and get response
        json_response = connect_to_endpoint(search_url, query_params, next_token)
        result_count = json_response.get('meta').get('result_count') 

        if result_count == 0:
            # Break if no tweet is returned
            flag = False
        else:
            # Divide response into two parts, tweet and user and convert t two separate data frame
            tweet = pd.DataFrame(json_response['data'])
            user = pd.DataFrame(json_response['includes']['users'])

            # Pagination. Get next response if any, and append to the list
            if 'next_token' in json_response['meta']:
                  next_token = json_response['meta']['next_token']
                  tweet = pd.concat([tweet, pd.DataFrame(json_response['data'])])
                  user = pd.concat([user, pd.DataFrame(json_response['includes']['users'])])
                  count += result_count
            
            else:
                  len_tweet = len(pd.DataFrame(json_response['data']))
                  len_user = len(pd.DataFrame(json_response['includes']['users']))
                  count += result_count
                  if len_tweet > 0:
                     tweet = pd.concat([tweet, pd.DataFrame(json_response['data'])])
                  if len_user > 0:
                     user = pd.concat([user, pd.DataFrame(json_response['includes']['users'])])

                  flag = False
    
    # Append query as a column to the tweet table
    tweet['query'] = name

    # Load data as string to BigQuery tables and drop duplicates
    # TO DO: load directly from JSON response using pre-defined schema and combine result into one table (instead of two)
    bq_load('tweet', tweet.astype(str).drop_duplicates())
    bq_load('user', user.astype(str).drop_duplicates())
    return f"Counting for keyword: {name}, total count: {count}"