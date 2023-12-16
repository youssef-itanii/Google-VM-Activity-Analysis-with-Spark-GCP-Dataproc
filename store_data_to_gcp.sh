#!/bin/bash

# Set the name of the source and destination buckets
SOURCE_BUCKET=gs://clusterdata-2011-2
DESTINATION_BUCKET=gs://large-data-spark

# Directory to store the files temporarily
LOCAL_STORAGE_PATH="temp"

# List all directories in the source bucket
DIRECTORIES=$(gsutil ls $SOURCE_BUCKET | grep '/$')

# Iterate over each directory
for DIR in $DIRECTORIES; do
    # Extract the directory name
    DIR_NAME=$(basename $DIR)

    # Create a local directory
    LOCAL_DIRECTORY="$LOCAL_STORAGE_PATH/$DIR_NAME"
    mkdir -p $LOCAL_DIRECTORY

    # Download all .gz files from the directory
    gsutil -m cp "$DIR*.gz" $LOCAL_DIRECTORY

    # Navigate to the directory
    cd $LOCAL_DIRECTORY

    # Unzip, upload, and delete each file
    for GZ_FILE in *.gz; do
        # Unzip the file
        gzip -d $GZ_FILE

        # Get the name of the unzipped file
        CSV_FILE=${GZ_FILE%.gz}

        # Upload the unzipped file to your bucket
        gsutil cp $CSV_FILE $DESTINATION_BUCKET/$DIR_NAME/

        # Delete the unzipped local file if the upload was successful
        if [ $? -eq 0 ]; then
            echo "Upload successful, deleting local file: $CSV_FILE"
            rm $CSV_FILE
        else
            echo "Upload failed for file: $CSV_FILE"
        fi
    done

    # Return to the original directory and remove the local directory
    cd -
    rm -rf $LOCAL_DIRECTORY
done
