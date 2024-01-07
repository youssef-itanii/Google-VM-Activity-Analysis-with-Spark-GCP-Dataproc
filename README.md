# Google-Cluster-Analysis
# Spark
## Deploying to cloud
```bash 
$ ./deploy_cluser -r <region> -z <zone> -n <cluster-name> 
```


You then need to copy your files to the bucket
```bash
$ gsutil cp -r {Target Path} gs://abbas-youssef-large-data-213/{Target}
```
If you want to copy a single file, you can remove the -r flag.



Now, you must submit your spark jobs to the cluster.
```
$ ./submit_job <python-file>
```


To delete your cluster:
```
$ ./delete_cluster
```