import argparse, json, typing, os

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions

'''
Constant
'''
project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
bearer = os.getenv('bearer')
topic_id = 'pubsub-stream-filter'
class GetTimestamp(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime().strftime("%Y-%m-%dT%H:%M:%S")
        output = {'timestamp': window_start, 'tweet_count': element.tweet_count}
        yield output


class PerLangAggregation(typing.NamedTuple):
    lang: str
    tweet_count: int


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project_id', type=str, required=True)
    parser.add_argument('--input_topic', type=str, required=True)

    return parser.parse_known_args()


def run():
    # Setting up the Beam pipeline options
    args, pipeline_args = parse_args()

    options = PipelineOptions(pipeline_args, save_main_session=True, streaming=True)
    options.view_as(GoogleCloudOptions).project = args.project_id

    output_table_name = "raw_tweets"
    agg_output_table_name = "minute_level_counts"
    dataset = "part2"

    window_size = 60

    raw_tweets_schema = {
        "fields": [
            {
                "name": "author_id",
                "type": "STRING"
            },
            {
                "name": "created_at",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "id",
                "type": "STRING"
            },
            {
                "name": "stream_rule",
                "type": "STRING",
            },
            {
                "name": "text",
                "type": "STRING",
            },
            {
                "name": "public_metrics",
                "type": "RECORD",
                "mode": "NULLABLE",
                "fields": [
                    {
                        "name": "like_count",
                        "type": "INT",
                        "mode": "NULLABLE"
                    },
                    {
                        "name": "quote_count",
                        "type": "INT",
                        "mode": "NULLABLE"
                    },
                    {
                        "name": "reply_count",
                        "type": "INT",
                        "mode": "NULLABLE"
                    },
                    {
                        "name": "retweet_count",
                        "type": "INT",
                        "mode": "NULLABLE"
                    },
                ]
            },
            {
                "name": "user",
                "type": "RECORD",
                "mode": "NULLABLE",
                "fields": [
                    {
                        "name": "id",
                        "type": "STRING",
                        "mode": "NULLABLE"
                    },
                    {
                        "name": "name",
                        "type": "STRING",
                        "mode": "NULLABLE"
                    },
                    {
                        "name": "username",
                        "type": "STRING",
                        "mode": "NULLABLE"
                    },
                ]
            },
        ]
    }

    agg_tweets_schema = {
        "fields": [
            {
                "name": "timestamp",
                "type": "STRING"
            },
            {
                "name": "tweet_count",
                "type": "STRING",
            }
        ]
    }

    # Create the pipeline
    p = beam.Pipeline(options=options)

    raw_tweets = (p | "ReadFromPubSub" >> beam.io.ReadFromPubSub(args.input_topic)
                    | "ParseJson" >> beam.Map(lambda element: json.loads(element.decode("utf-8")))
                  )

    # write raw tweets to BQ
    raw_tweets | "Write raw to bigquery" >> beam.io.WriteToBigQuery(
        output_table_name,
        dataset=dataset,
        project=args.project_id,
        schema=raw_tweets_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    )

    # aggregate tweets by window and write to BQ
    (raw_tweets
        | "Window" >> beam.WindowInto(beam.window.FixedWindows(window_size))
        | "Aggregate per window" >> beam.combiners.Count.PerKey('tweet_count')
        | "Add Timestamp" >> beam.ParDo(GetTimestamp())
        | "Write agg to bigquery" >> beam.io.WriteToBigQuery(
            agg_output_table_name,
            dataset=dataset,
            project=args.project_id,
            schema=agg_tweets_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )
     )

    p.run().wait_until_finish(10000)


if __name__ == "__main__":
    run()