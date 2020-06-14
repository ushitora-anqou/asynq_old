# AsynQ
Yet another storage system running on client side with AWS S3 backend

```
$ docker pull minio/minio
$ docker run -p 9000:9000 minio/minio server /data
# Make a bucket 'buckethoge' in MinIO.
$ AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_BUCKET=buckethoge cargo run
```
