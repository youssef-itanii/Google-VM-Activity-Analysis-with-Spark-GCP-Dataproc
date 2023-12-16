#!/bin/bash

CONFIG_FILE="cluster_config.txt"
if [ -f $CONFIG_FILE ]; then
    source $CONFIG_FILE
else
    echo "Configuration file not found!"
    exit 1
fi

gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION

rm cluster_config.txt
echo "Deleted configuration file"

