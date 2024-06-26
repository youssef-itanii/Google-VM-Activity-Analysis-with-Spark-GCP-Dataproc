#!/bin/bash

while getopts ":r:z:n:w:c" opt; do
  case $opt in
    r) REGION=$OPTARG ;;
    z) ZONE=$OPTARG ;;
    n) CLUSTER_NAME=$OPTARG ;;
    \?) echo "Invalid option -$OPTARG" >&2
        exit 1 ;;
  esac
done

if [ -z "$REGION" ] || [ -z "$ZONE" ] || [ -z "$CLUSTER_NAME" ] ; then
    echo "Usage: $0 -r <region> -z <zone> -n <cluster-name>

NOTE: If you want to create a new bucket, use the -c flag and a bucket will be created with the name 'abbas-youssef-large-data-213'.
Example: Usage: $0 -r <region> -z <zone> -n <cluster-name> -c 
=============================================================================================================
"
    exit 1
fi


gcloud dataproc clusters create $CLUSTER_NAME \
    --region=$REGION \
    --zone=$ZONE \
    --master-machine-type=n1-standard-4 \
    --master-boot-disk-size=100 \
    --num-workers=5 \
    --worker-machine-type=n1-standard-4 \
    --worker-boot-disk-size=100 \
    --image-version=1.5-debian10 \
    --metadata 'PIP_PACKAGES=google-cloud-storage' \
    --initialization-actions gs://goog-dataproc-initialization-actions-$REGION/python/pip-install.sh \
    --properties spark:spark.executor.memoryOverhead=1024    

gcloud dataproc clusters describe $CLUSTER_NAME --region=$REGION

CONFIG_FILE="cluster_config.txt"
echo "REGION=$REGION" > $CONFIG_FILE
echo "ZONE=$ZONE" >> $CONFIG_FILE
echo "CLUSTER_NAME=$CLUSTER_NAME" >> $CONFIG_FILE

