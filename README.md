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

## Prereqs
You'll need: 1) Twitter Developer credentials and 2) a GCP account set up.

Once you have a Twitter Developer account, you'll need an app, API consumer key (and secret), access token (and secret), and bearer token.

(You may need to enable to the relevant APIs on your GCP account - Compute Engine, GCS, Dataflow, and BigQuery. You can do that using the search bar at the top of the GCP interface.)

## Data sources
Twitter API
|Type|End Point|Used in|
|---|---|---|
|Recent counts|https://api.twitter.com/2/tweets/counts/recent|Functions 2: Count recent|
|Recent search|https://api.twitter.com/2/tweets/search/recent|Functions 3: Search recent|
|Filtered stream|https://api.twitter.com/2/tweets/search/stream|Functions 4: Filtered stream|

## Flow
<img src="Flowcharts%20-%20Flowchart%20(2).png" width="500">


*Function 2, 3, and 4 are created separately so that when user input new keyword, then recent 7 days, 1 minute, 5 minutes, 10 minutes counts can be generated instantly. These 3 functions are run by cloud scheduler, and also can be triggered by PubSub 1.*

* Functions 3, search recent limited only upto last 15 minutes as per requirement. Then in `Big Query`
### 1. [Functions 1: Input Keyword](part_2/function-keyword)
User is expected to input HTTPS Post to the function. Example from Google Cloud Shell:
```
curl -m 70 -X POST https://pubsub-test-keyword-nibljnhwbq-lz.a.run.app \
-H "Authorization: bearer $(gcloud auth print-identity-token)" \
-H "Content-Type: application/json" \
-d '{
  "name": "trondheim"
}'
````

### 2. Create Pub/Sub topic. 
Create a `Pub/Sub` topic and name it accordingly (something like `twitter` will work). [Functions 1: Input Keyword] will publish a message to the topic. From the GUI, then press the Trigger Cloud Function button. It will create a new Function where you could then modify and insert your [Functions 2: Count recent](part_2/function-count-recent). Create another trigger and insert your [Functions 3: Search recent](part_2/function-search-recent) to the newly created Function and the same for [Functions 4: Filtered stream].

Under the hood: <br>
Google will automatically push the message from your topic into an automatically created Subscription. Then a 'Cloud Run' will push your message from the 'Subscription' to the `Function Count Recent`.

At the console, go to [Eventarc](https://console.cloud.google.com/eventarc/) and you could 

### 3. [Functions 2: Count recent](part_2/function-count-recent)
- Fired one time when user input new keyword to get recent 7 days tweets count.
- Write to Pub/Sub 2: Count
- Pub/Sub 2: Count the push message to `Big Query`'Count' table

### 4. 

Create BigQuery dataset and table
[Function search recent](part_2/function-search-recent/main.py) will write your tweets and users in the past 15 minutes to BigQuery, so it would be necessary to create the dataset and tables beforehand. Update the [main.py](part_2/function-search-recent/main.py) if you decided to give different name to your tables.

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

## TO DO 
1. add `pubsub` topic as `function` argument
1. check and compare count: directly via Twitter API vs these self developed functions
1. use `airflow` to automate and better tracking the ETL job
1. set cloud scheduler to trigger `function` every 5 seconds
1. set queueing and ACK in `pubsub` message/ eventarc
1. check Twitter API request limit for several endpoints used in the `functions`
1. check how to set up refresh rate in `Data Studio`
1. check memory limit in `Cloud Function`when importing `Text Classifier` from Flair as continuously getting error 
1. add lat updated datetime to the `Data Studio`report
1. add try and exception to better catch error in the function
1. add alert if `function` not returning > 5 seconds or if get error
1. create new feature branch for changes and apply proper CI/CD
1. deploy `function`as containerized application
1. get related terms