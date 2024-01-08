# Google-Cluster-Analysis
# Spark
## Running Locally
In order to run the code locally, you must first download some data from the cloud storage and you can reach each question on its own.

First, cd into the src directory 
```bash 
$ cd src
```

Then you can run any python code as long as you have the necessary cluster data downloaded and assigned in their correct directory in the **data** folder. 

The runnable files follow this format
Qx_y.py

## Deploying to cloud
**Note: You must be in the same directory**
```bash 
$ ./deploy_cluser -r <region> -z <zone> -n <cluster-name> 
```


You then need to copy your files to the bucket
```bash
$ ./upload_to_storage {src} {trgt} 
```
Example:
```bash
$ ./upload_to_storage ../src/main.py ./src/main.py 
```

Now, you must submit your spark jobs to the cluster.
```
$ ./submit_job <python-file>
```


To delete your cluster:
```
$ ./delete_cluster
```
Note: The script has been modified to also delete the disk of the cluster.