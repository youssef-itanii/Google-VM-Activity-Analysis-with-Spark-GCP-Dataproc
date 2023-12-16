# Google-Cluster-Analysis
# Spark
## Deploying to cloud
```bash 
    ./deploy_cluser  <region> <zone> <cluster-name> <num-workers>
```

If you don't have a bucket, you can include the -c flag to create a bucket with the name 'large-data'

You then need to copy your files to the bucket
```bash
    gsutil cp -r {Target Path} gs://large-data/
```


Now, you must submit your spark jobs to the cluster.
```
    ./submit_job
```

If you want to copy a single file, you can remove the -r flag.

To delete your cluster:
```
   ./delete_cluster
```