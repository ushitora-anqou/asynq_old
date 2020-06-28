# AsynQ
Yet another storage system running on client side with AWS S3 backend

## Test

```
$ docker pull minio/minio
$ docker run -p 9000:9000 minio/minio server /data
# Then make a bucket 'asynq' in MinIO.
$ AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin cargo test
# Or, if you want to use AWS S3,
$ AWS_ACCESS_KEY_ID="..." AWS_SECRET_ACCESS_KEY="..." S3_REGION="..." S3_BUCKET="..." cargo test
```
