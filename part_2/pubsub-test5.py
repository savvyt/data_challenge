import os, requests, datetime, json, time
from pandas import isnull
from google.cloud import pubsub_v1

project_id=os.getenv('GOOGLE_CLOUD_PROJECT')
bearer=os.getenv('bearer')
search_url = "https://api.twitter.com/2/tweets/search/recent"

current_time = datetime.datetime.utcnow()
start_time = (current_time - datetime.timedelta(minutes=15)).isoformat("T") + "Z"

next_token = None
#Total number of tweets we collected from the loop
total_tweets = 0
i = 1

publisher = pubsub_v1.PublisherClient() 
topic_name_tweet = 'projects/{project_id}/topics/{topic}'.format(
   project_id=os.getenv('GOOGLE_CLOUD_PROJECT'),
   topic='pubsub-test-tweet',
   )
topic_name_user = 'projects/{project_id}/topics/{topic}'.format(
   project_id=os.getenv('GOOGLE_CLOUD_PROJECT'),
   topic='pubsub-test-user',
   )

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

def write_to_pubsub_tweet(data):
    data_formatted = json.dumps(data).encode("utf-8")
    id = data["id"].encode("utf-8")
    author_id = data["author_id"].encode("utf-8")
    metrics = json.dumps(data["public_metrics"]).encode("utf-8")

    future = publisher.publish(topic_name_tweet,data_formatted, id=id, author_id=author_id, metrics = metrics)
    future.result()
   #  print(future.result())

def write_to_pubsub_user(data):
    data_formatted = json.dumps(data).encode("utf-8")
    id = data["id"].encode("utf-8")
    username = data["username"].encode("utf-8")

    future = publisher.publish(topic_name_tweet,data_formatted,id=id, username=username)
    future.result()

# def main(request):
if __name__ == "__main__":
   #  name = request.get_json().get('name')
    # Optional params: start_time,end_time,since_id,until_id,next_token,granularity
    tweet_fields = ['author_id', 'created_at']
    user_fields = ['username', 'id']
    metrics = ['retweet_count','reply_count','like_count','quote_count']
    query_params = {'query': 'azure'
    , 'start_time': start_time
    , 'max_results' : 50
    , 'expansions':'author_id'
    , 'user.fields' : 'username,id'
    , 'tweet.fields': 'author_id,public_metrics,created_at'
    , 'next_token' : {}}

flag = True
while flag:
    json_response = connect_to_endpoint(search_url, query_params, next_token)
    result_count = json_response.get('meta').get('result_count') 

    if 'next_token' in json_response['meta']:
        next_token = json_response['meta']['next_token']
        total_tweets += result_count
        print("Iteration: ", i)
        i += 1

        future_tweet = [write_to_pubsub_tweet(item) for item in json_response['data']]
        future_user = [write_to_pubsub_user(item) for item in json_response['includes']['users']]
        print("Total # of Tweets added: ", total_tweets)
        print("-------------------")
    
    else:
        total_tweets += result_count
        print("Iteration: ", i)

        future_tweet = [write_to_pubsub_tweet(item) for item in json_response['data']]
        future_user = [write_to_pubsub_user(item) for item in json_response['includes']['users']]
        print("Total # of Tweets added: ", total_tweets)
        print("-------------------")

        flag = False

        # if 'next_token' in json_response['meta']:
        #     next_token = json_response['meta']['next_token']
            
        #     if result_count is not None and result_count > 0 and next_token is not None:
        #         total_tweets += result_count
        #         print("Total # of Tweets added: ", total_tweets)
        #         print("-------------------")
        #     else:
        #         continue
        # else:
        #     flag: False
        #     break
        

    
   #  return f"Counting for keyword: {name}"
