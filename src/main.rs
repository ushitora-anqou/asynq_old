use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, PutObjectRequest, S3Client, S3};
use std::{env, str::FromStr};
use tokio::io::AsyncReadExt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum GetFileError {
    RusotoFail(String),
}

#[derive(Debug, PartialEq)]
enum PutFileError {
    RusotoFail(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct File {
    path: String,
    body: Vec<u8>,
    create_datetime: DateTime<Utc>,
    modify_datetime: DateTime<Utc>,
}

#[async_trait]
trait StorageEntity {
    async fn put_file(&self, path: String, file: &File) -> Result<(), PutFileError>;
    async fn get_file(&self, path: String, file: &mut File) -> Result<(), GetFileError>;
}

struct S3Storage {
    client: S3Client,
    bucket: String,
}

impl S3Storage {
    fn new(region: Region, bucket: String) -> S3Storage {
        S3Storage {
            client: S3Client::new(region),
            bucket: bucket,
        }
    }
}

#[async_trait]
impl StorageEntity for S3Storage {
    async fn put_file(&self, path: String, file: &File) -> Result<(), PutFileError> {
        let mut request = PutObjectRequest::default();
        request.bucket = self.bucket.clone();
        request.key = path;
        request.body = Some(file.body.clone().into());
        self.client
            .put_object(request)
            .await
            .map_err(|e| PutFileError::RusotoFail(format!("{}", e)))?;
        Ok(())
    }

    async fn get_file(&self, path: String, file: &mut File) -> Result<(), GetFileError> {
        let mut request = GetObjectRequest::default();
        request.bucket = self.bucket.clone();
        request.key = path;
        self.client
            .get_object(request)
            .await
            .map_err(|e| GetFileError::RusotoFail(format!("{}", e)))?
            .body
            .ok_or(GetFileError::RusotoFail("body is empty".to_string()))?
            .into_async_read()
            .read_to_end(&mut file.body)
            .await
            .map_err(|e| GetFileError::RusotoFail(format!("Can't read body: {}", e)))?;

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let region = match env::var("S3_REGION") {
        Ok(s) => Region::from_str(&s).unwrap(),
        Err(_) => Region::Custom {
            name: "s3-asynq".to_string(),
            endpoint: env::var("S3_ENDPOINT").unwrap_or("http://localhost:9000".to_string()),
        },
    };
    let bucket = env::var("S3_BUCKET").unwrap_or("asynq".to_string());

    let storage = S3Storage::new(region, bucket);
    let src_file = File {
        path: "/hoge".to_string(),
        body: "hogehoge".to_string().into_bytes(),
        create_datetime: Utc::now(),
        modify_datetime: Utc::now(),
    };

    let mut dst_file = File {
        body: "".to_string().into_bytes(),
        //TODO: These attributes must be retrieved from file meta info.
        path: "/hoge".to_string(),
        create_datetime: Utc::now(),
        modify_datetime: Utc::now(),
    };
    storage
        .put_file("journal/hoge".to_string(), &src_file)
        .await
        .unwrap();
    storage
        .get_file("journal/hoge".to_string(), &mut dst_file)
        .await
        .unwrap();
    println!("{:?}", dst_file);
}
