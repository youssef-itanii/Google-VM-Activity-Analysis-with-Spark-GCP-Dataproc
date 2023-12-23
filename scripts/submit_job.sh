#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <file-name-from-bucket>"
    exit 1
fi

FILE_NAME=$1


CONFIG_FILE="cluster_config.txt"
if [ -f $CONFIG_FILE ]; then
    source $CONFIG_FILE
else
    echo "Configuration file not found! Make sure you are running the script from the same working directory as this script."
    exit 1
fi

IS_REMOTE=1

SRC_PATH="gs://large-data/src"
gcloud dataproc jobs submit pyspark gs://large-data/src/$FILE_NAME \
    --"$IS_REMOTE" \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --py-files $SRC_PATH/util.py,$SRC_PATH/schema.py,$SRC_PATH/spark_connection.py,$SRC_PATH/storage_handler.py
