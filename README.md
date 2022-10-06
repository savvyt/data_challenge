# Part 1

## Load data
Import [dummy user_changes data](part_1/bq_part1_user_changes.csv) to the database for example Big Query following instruction [here] (https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv#loading_csv_data_into_a_table)

## Task 1.1
Objective: SQL query to find the user_id, current name and current email address for all users.

Solution: [query](part_1/find_latest.sql)
## Task 1.2
Objective: SQL query to find the median time between the second and third profile edit

Solution: [query](part_1/median_second_third_change.sql)

# Part 2
## Problem
Create a proof-of-concept for a tool that allows the user to specify a search term and receive every five seconds an updated output of some metrics about tweets that contain the search term.
The specific insights the tool should provide in its output are:
1. What is the total count of tweets matching the search term seen so far?
1. How many tweets containing the search term were there in the last 1, 5 and 15 minutes?
1. What are the ten most frequent terms (excluding the search term) that appear in tweets containing the search term over the last 1, 5 and 15 minutes?
1. Within tweets matching the search term, who were the top ten tweeps (Twitter users) who tweeted the most in the last 1, 5 and 15 minutes?
1. What is the sentiment of tweets matching the search term over the last 1, 5 and 15 minutes?

## Architecture Research
There are several ways to satisfy the requirement:
### 1. Virtual machine
1. Run code in WM streaming data to Pub/Sub. 
1. Create a pipeline in Dataflow to aggregate data and write to BigQuery. 
1. From BigQuery then create a dashboard using Data Studio.

[Forked reference](https://github.com/savvyt/twitter)
### 2. Local machine
1. Download tweets using Twitter API (https://developer.twitter.com/en/docs/twitter-api).
1. Send each tweet as a message to Google Pub/Sub.
1. Process the tweets using Apache Beam pipeline and aggregate the data.
1. Save both raw tweets and aggregated tweet data to BigQuery tables.

[Forked reference](https://github.com/savvyt/tweet-streaming) <br>
[Tutorial](https://dsstream.com/streaming-twitter-data-with-google-cloud-pub-sub-and-apache-beam)
### 3. App Engine

1. Add rules to the stream with the Filtered Stream rules API endpoint
1. Install and involve the toolkit from GitHub in your Google Cloud project
1. Configure the CRON job - Google Cloud Scheduler
1. Configure the dashboard, by connecting to the BigQuery database with DataStudio

[Reference: developer-guide--twitter-api-toolkit-for-google-cloud](https://developer.twitter.com/en/docs/tutorials/developer-guide--twitter-api-toolkit-for-google-cloud1)
### 4. Cloud Functions
[Reference: serverless-twitter-bot-with-google-cloud](https://itnext.io/serverless-twitter-bot-with-google-cloud-35d370676f7) <br>
[Reference: cloud-function-to-publish-messages-to-pub-sub](https://medium.com/@chandrapal/creating-a-cloud-function-to-publish-messages-to-pub-sub-154c2f472ca3)

### Comparison:
||VM   | Local machine  |  App Engine |   Cloud Function|
|---|---|---|---|---|
|Connection|can be 24/7 except during maintenance window | Not always connected  |Can get disconnected and must restart the process by establishing a new connection. Additionally, to ensure that you do not miss any data, you may need to utilize a Redundant Connection, Backfill, or a Replay stream to mitigate or recover data from disconnections from the stream.|Streaming is problematic because you have to be always connected. And with serverless product you have timeout concern (9 minutes for Cloud Functions V1, 60 minutes for Cloud Run and Cloud Functions V2). However you can imagine to invoke regularly your serverless product, stay connected for a while (let say 1h) and schedule trigger every hour.|

## Solution
```mermaid
graph TB
A(Cloud Function POST search term) -->|query|B[Pub/Sub]
B-->B1(Cloud Function counts/recent)
B-->B2(Cloud Function search/recent)
B-->B3(Cloud Function search/stream)
B1-->C[Big Query tweet.twitter and tweet.user]
B2-->C
B3-->C
C-->D(Data Studio) 
click A href "https://console.cloud.google.com/functions/details/europe-north1/pubsub-test-keyword?env=gen2&project=extended-study-364220" "pubsub-test-keyword"
```

1. User input HTTPS Post to Cloud Function pubsub-test-keyword
1. Function will publish a message to a Pub/Sub with topic name projects/extended-study-364220/topics/pubsub-test-keyword  and create an arc event. <br>
Under the hood: <br>
The event will be written from above topic into a subscription projects/extended-study-364220/subscriptions/eventarc-europe-north1-function-1-474919-sub-036 that has push method with end point https://function-1-nibljnhwbq-lz.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2Fextended-study-364220%2Ftopics%2Fpubsub-test-keyword. <br>This end point has an audience https://function-1-nibljnhwbq-lz.a.run.app which is the URL of the next function.
1. 

### TO DO
1. Fix timeout with Cloud Function. Perhaps deploy in Kubernetes
2. Apply Airflow or Dataflow to automate and better tracking the ETL job

### Challenges
1. Unable to use Workflow to input variable to another cloud function due to error 500 link
<br>These intermittent 500s are due to requests timing out in the pending queue while waiting for a clone to be able to service the request. This is quite common for GCF apps when they attempt to scale up extremely rapidly. The best approach would be to configure your workload so that they don't have such sharp spikes from ~0 QPS - e.g. if you ramp up traffic gradually over the course of a minute they'll be far less likely to see these errors.
