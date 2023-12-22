#!/bin/bash

CONFIG_FILE="cluster_config.txt"
if [ -f $CONFIG_FILE ]; then
    source $CONFIG_FILE
else
    echo "Configuration file not found! Make sure you are running the script from the same working directory as this script."
    exit 1
fi
gcloud dataproc jobs submit pyspark gs://large-data/src/Q6_UsagePerRequest_DF.py \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --py-files gs://large-data/src/util.py,gs://large-data/src/schema.py,gs://large-data/src/spark_connection.py
