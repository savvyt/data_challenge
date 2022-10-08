'''
Google Cloud Function
1. Get input from POST HTTPS using cloudshell ex: 
curl -m 70 -X POST <<functions URL endpoint>> \
-H "Authorization: bearer $(gcloud auth print-identity-token)" \
-H "Content-Type: application/json" \
-d '{
  "name": "Hello World"
}'
2. Publish message to Pub/Sub topic
'''
import os, json
from google.cloud import pubsub_v1

project_id=os.getenv('GOOGLE_CLOUD_PROJECT')
topic_id = os.getenv('topic_id')

def test(request):
    # Get query input from user via HTTPS
    name = request.get_json().get('name')

    # Establish Pub/Sub client
    publisher = pubsub_v1.PublisherClient()
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
        project_id=project_id,
        topic=topic_id,
    )

    # Publish message to Pub/Sub topic specified above as a bytestring
    future = publisher.publish(topic_name, name.encode("utf-8"))
    future.result()
    return f"Keyword: {name} "