import base64, json, tweepy, os, functions_framework, flair
from google.cloud import pubsub_v1
from flair.models import TextClassifier
from flair.data import Sentence

'''
CONSTANT
'''
project_id=os.getenv('GOOGLE_CLOUD_PROJECT')
bearer=os.getenv('bearer')
topic_id = os.getenv('topic_id')

def write_to_pubsub(data, stream_rule):
    data["stream_rule"] = stream_rule
    data_formatted = json.dumps(data).encode("utf-8")
    id = data["id"].encode("utf-8")
    author_id = data["author_id"].encode("utf-8")
    
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    future = publisher.publish(
        topic_path, data_formatted, id=id, author_id=author_id
    )
    print(future.result())

def sentiment(text):
        sentence = flair.data.Sentence(text)
        classifier.predict(sentence)
        label_score = sentence.labels[0].to_dict()['value']
        return label_score

class Client(tweepy.StreamingClient):
    def __init__(self, bearer_token, stream_rule):
        super().__init__(bearer_token)

        self.stream_rule = stream_rule

    def on_response(self, response):
        tweet_data = response.data.data
        user_data = response.includes['users'][0].data
        result = tweet_data
        result["user"] = user_data
        result['sentiment'] = sentiment(result['text'])

        write_to_pubsub(result, self.stream_rule)

@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    stream_rule = base64.b64decode(cloud_event.data["message"]["data"]).decode('UTF-8')
    tweet_fields = ['id', 'text', 'author_id', 'created_at','public_metrics']
    user_fields = ['id','username']
    expansions = ['author_id']

    streaming_client = Client(bearer, stream_rule)
    classifier = TextClassifier.load('en-sentiment')

    # remove existing rules
    rules = streaming_client.get_rules().data
    if rules is not None:
        existing_rules = [rule.id for rule in streaming_client.get_rules().data]
        streaming_client.delete_rules(ids=existing_rules)

    # add new rules and run stream
    streaming_client.add_rules(tweepy.StreamRule(stream_rule))
    streaming_client.filter(tweet_fields=tweet_fields, expansions=expansions, user_fields=user_fields)
    return f"Streaming for keyword: {stream_rule}"