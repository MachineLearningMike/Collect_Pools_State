## deploy function
gcloud functions deploy my-http-function --gen2 --region=us-central1 --runtime=nodejs16 --source=. --entry-point=myHttpFunction --trigger-http

gcloud functions deploy [function name] --gen2 --region=us-central1 --runtime=python310 --source=. --entry-point=[function name] --trigger-http

## create pubsub topic
gcloud pubsub topics create fetch_pool_events_topic
Created topic [projects/nice-proposal-338510/topics/fetch_pool_events_topic].