#!/bin/bash
if [ "$#" -ne 1]; then
    echo "Usage: $0 <file-name> 
    [EXCLUDE .py EXTENSION]"
    exit 1
fi


FILE_TO_PROCESS=$1
RESULTS_DIR="../results"
CONFIG_FILE="cluster_config.txt"

if [ -f "$CONFIG_FILE" ]; then
    source "$CONFIG_FILE"
else
    echo "Configuration file not found! Make sure you are running the script from the same working directory as this script."
    exit 1
fi

# Check if CLUSTER_NAME and REGION are set in the configuration file
if [ -z "$CLUSTER_NAME" ] || [ -z "$REGION" ]; then
    echo "Cluster name or region not defined in the configuration file."
    exit 1
fi

OUTPUT_FILE="$RESULTS_DIR/${FILE_TO_PROCESS}_execution_times.json"

mkdir -p "$RESULTS_DIR"

echo "[" > "$OUTPUT_FILE"

for i in {2..5}
do
    echo "Updating cluster to $i workers in region $REGION"
    gcloud dataproc clusters update "$CLUSTER_NAME" --num-workers=$i --region="$REGION"

    if [ $? -ne 0 ]; then
        echo "Failed to update the cluster. Exiting."
        exit 1
    fi

    # Submit the job and capture its output
    JOB_OUTPUT=$(./submit_job.sh "$FILE_TO_PROCESS".py)

    if [ $? -ne 0 ]; then
        echo "Failed to submit the job. Exiting."
        exit 1
    fi

    EXECUTION_TIME=$(echo "$JOB_OUTPUT" | grep "executed in" | awk '{print $3}')

    if [ -n "$EXECUTION_TIME" ]; then
        echo "{\"workerCount\": $i, \"executionTime\": $EXECUTION_TIME}\n" >> "$OUTPUT_FILE"
    else
        echo "{\"workerCount\": $i, \"executionTime\": null}\n" >> "$OUTPUT_FILE"
    fi

    # Add a comma between JSON objects, except for the last iteration
    if [ $i -lt 5 ]; then
        echo "," >> "$OUTPUT_FILE"
    fi

done

echo "]" >> "$OUTPUT_FILE"

echo "Process completed. Results stored in $OUTPUT_FILE."
