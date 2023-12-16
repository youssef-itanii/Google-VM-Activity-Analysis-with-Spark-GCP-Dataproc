mkdir data
cd data
gsutil cp gs://clusterdata-2011-2/schema.csv .

mkdir task_events
gsutil cp gs://clusterdata-2011-2/task_events/part-00000-of-00500.csv.gz task_events
gunzip task_events/part-00000-of-00500.csv.gz 

mkdir job_events
gsutil cp gs://clusterdata-2011-2/job_events/part-00000-of-00500.csv.gz job_events
gunzip job_events/part-00000-of-00500.csv.gz 

mkdir machine_events
gsutil cp gs://clusterdata-2011-2/machine_events/part-00000-of-00001.csv.gz machine_events
gunzip machine_events/part-00000-of-00001.csv.gz 

mkdir task_usage
gsutil cp gs://clusterdata-2011-2/task_usage/part-00000-of-00500.csv.gz task_usage
gunzip task_usage/part-00000-of-00500.csv.gz 