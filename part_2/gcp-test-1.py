import pandas as pd, json, requests, pandas_gbq, os, datetime
from pandas.io import gbq
from genson import SchemaBuilder
from google.cloud import bigquery

'''
CONSTANT
'''
project_id=os.getenv('GOOGLE_CLOUD_PROJECT')
dataset_name=os.getenv('dataset_name')
bearer=os.getenv('bearer')
search_url = "https://api.twitter.com/2/tweets/search/recent"

current_time = datetime.datetime.utcnow()
start_time = (current_time - datetime.timedelta(minutes=15)).isoformat("T") + "Z"

builder = SchemaBuilder()

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer}"
    r.headers["User-Agent"] = "v2RecentTweetCountsPython"
    return r


def connect_to_endpoint(url, params, next_token = None):
    params['next_token'] = next_token
    response = requests.request("GET", search_url, auth=bearer_oauth, params=params)
   #  print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def get_schema(json_response):
    builder.add_object(json_response)
    return builder.to_schema()
'''
function 3: This function just converts your pandas dataframe into a bigquery table, 
you'll also need to designate the name and location of the table in the variable 
names below.
'''
  
def bq_load(table_name, value, schema = None):
  value.to_gbq(destination_table='{}.{}'.format(dataset_name, table_name), project_id=project_id, table_schema = schema, if_exists='replace')
  
# def main(request):
#     name = request.get_json().get('name')
if __name__ == "__main__":
    query_params = {'query': 'trondheim'
        , 'start_time': start_time
        , 'max_results' : 100
        , 'expansions':'author_id'
        , 'user.fields' : 'id,name,username'
        , 'tweet.fields': 'author_id,public_metrics,created_at'
        , 'next_token' : {}}

    flag = True
    next_token = None
    dump = []
    # user = []

    json_response = connect_to_endpoint(search_url, query_params, next_token)
    # tweet_schema = get_schema(json_response['data'])
    # user_schema = get_schema(json_response['includes']['users'])
    
    while flag:
        result_count = json_response.get('meta').get('result_count') 

        # tweet = pd.DataFrame(json_response['data'])
        # user = pd.DataFrame(json_response['includes']['users'])

        if 'next_token' in json_response['meta']:
            next_token = json_response['meta']['next_token']
            dump = dump + json_response
            # tweet = tweet.append(json_response['data'])
            # user = user.append(json_response['includes']['users'])
            # tweet = pd.concat([tweet, pd.DataFrame(json_response['data'])])
            # user = pd.concat([user, pd.DataFrame(json_response['includes']['users'])])
        
        else:
            # len_tweet = len(pd.DataFrame(json_response['data']))
            # len_user = len(pd.DataFrame(json_response['includes']['users']))
            len_tweet = len(json_response['data'])
            if len_tweet > 0:
                dump = dump + json_response
                # tweet = tweet + (json_response['data'])
                # tweet = pd.concat([tweet, pd.DataFrame(json_response['data'])])
                # user = user + (json_response['includesk']['users'])
                # user = pd.concat([user, pd.DataFrame(json_response['includes']['users'])])

            flag = False
    
    bq_load('dump1', pd.DataFrame(dump))
    # bq_load('tweet', user, user_schema)
    # bq_load('user', user.astype(str).drop_duplicates())

    # return f"Counting for keyword: {name} "