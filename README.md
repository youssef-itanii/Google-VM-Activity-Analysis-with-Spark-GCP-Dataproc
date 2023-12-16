# Google-Cluster-Analysis
# Spark
## Deploying to cloud
```bash 
    ./deploy_cluser.sh  <region> <zone> <cluster-name> <num-workers>
```

You then need to copy your files to the bucket
```bash
    gsutil cp -r {Target Path} gs://large-data/
```
If you want to copy a single file, you can remove the -r flag.