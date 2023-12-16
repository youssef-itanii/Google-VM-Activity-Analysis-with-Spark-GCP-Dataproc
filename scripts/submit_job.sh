#!/bin/bash

CONFIG_FILE="cluster_config.txt"
if [ -f $CONFIG_FILE ]; then
    source $CONFIG_FILE
else
    echo "Configuration file not found!"
    exit 1
fi
gcloud dataproc jobs submit pyspark gs://large-data/src/main.py \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --py-files gs://large-data/src/util.py,gs://large-data/src/schema.py
