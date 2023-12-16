gcloud dataproc clusters create dataproc-cluster \
    --region=europe-west1 \
    --zone=europe-west1-b \
    --master-machine-type=n1-standard-4 \
    --master-boot-disk-size=500 \
    --num-workers=2 \
    --worker-machine-type=n1-standard-4 \
    --worker-boot-disk-size=500


gcloud dataproc clusters describe dataproc-cluster --region=europe-west1
