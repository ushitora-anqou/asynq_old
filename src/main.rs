use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, PutObjectRequest, S3Client, S3};
use std::env;

#[tokio::main]
async fn main() {
    let s3client = S3Client::new(Region::Custom {
        name: "hoge".to_string(),
        endpoint: "http://localhost:9000".to_string(),
    });
    let bucket = env::var("AWS_BUCKET").unwrap().to_string();

    let mut request = PutObjectRequest::default();
    request.bucket = bucket.clone();
    request.key = String::from("dir/file.hoge");
    request.body = Some("piyopiyo".to_string().into_bytes().into());
    let result = s3client.put_object(request).await.unwrap();
    println!("{:?}", result);

    let mut request = GetObjectRequest::default();
    request.bucket = bucket;
    request.key = String::from("dir/file.hoge");
    let result = s3client.get_object(request).await.unwrap();
    println!("{:?}", result);
}
