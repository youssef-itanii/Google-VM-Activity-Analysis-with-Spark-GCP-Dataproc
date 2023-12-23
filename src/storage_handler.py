import json
from google.cloud import storage

class StorageHandler:
    bucket_name = "large-data"
    path_to_data = f"gs://{bucket_name}/data"
    
    def __init__(self):
        self.__storage_client = storage.Client()
        self.__bucket = self.__storage_client.get_bucket(self.bucket_name)
    
    def store(self ,  data):
        file_name = "results/results.json"
        blob = self.__bucket.blob(file_name)

        if blob.exists():
            # Read the existing data and append new data
            existing_data = json.loads(blob.download_as_text())
            if isinstance(existing_data, list) and isinstance(data, list):
                existing_data.extend(data)
            elif isinstance(existing_data, dict) and isinstance(data, dict):
                existing_data.update(data)
            else:
                raise ValueError("Data types of existing file and new data do not match.")
            final_data = existing_data
        else:
            final_data = data

        # Upload the updated data
        blob.upload_from_string(json.dumps(final_data , indent=4), content_type='application/json' )
        print(f"Data written to {file_name} in bucket {self.bucket_name}")