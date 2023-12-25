#!/bin/bash
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <source-path> <destination-path>"
    echo "
        File System:
        ------------------------
        |_src/
        |_data/
        |_scripts/
        |_results/
    
    "
    exit 1
fi

SOURCE=$1
DEST=$2


gsutil cp $SOURCE gs://large-data/$DEST