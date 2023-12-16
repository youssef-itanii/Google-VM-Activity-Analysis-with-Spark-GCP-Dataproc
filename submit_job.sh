gcloud dataproc jobs submit pyspark gs://large-data/src/main.py \
    --cluster=dataproc-cluster \
    --region=europe-west1 \
    --py-files gs://large-data/src/util.py,gs://large-data/src/schema.py
