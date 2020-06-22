use crate::aqfs;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, ListObjectsV2Request, PutObjectRequest, S3Client, S3};
use std::{env, str::FromStr};
use tokio::io::AsyncReadExt;

pub struct Storage {
    client: S3Client,
    bucket: String,
}

impl Storage {
    pub fn new(region: Region, bucket: String) -> Storage {
        Storage {
            client: S3Client::new(region),
            bucket: bucket,
        }
    }
}

pub struct File {
    meta: aqfs::FileMeta,
}

#[async_trait]
impl aqfs::File for File {
    fn meta(&self) -> aqfs::FileMeta {
        self.meta.clone()
    }

    async fn read_all(&self) -> Result<Vec<u8>, aqfs::Error> {
        Err(aqfs::Error::NotImplemented)
    }
}

#[async_trait(?Send)]
impl aqfs::StorageEntity for Storage {
    async fn list_filemetas(&self) -> Result<Vec<aqfs::FileMeta>, aqfs::Error> {
        let mut request = ListObjectsV2Request::default();
        request.bucket = self.bucket.clone();
        let meta = self
            .client
            .list_objects_v2(request)
            .await
            .map_err(|e| aqfs::Error::Unexpected(format!("Rusoto failed: {}", e)))?
            .contents
            .ok_or(aqfs::Error::Unexpected(format!("Rusoto failed")))?
            .into_iter()
            .map(|o| aqfs::FileMeta {
                path: aqfs::Path::new(vec![o.key.expect("hoge")]),
                create_datetime: Utc::now(),
                modify_datetime: Utc::now(),
            })
            .collect();
        Ok(meta)
    }

    async fn fetch_file(&self, meta: &aqfs::FileMeta) -> Result<Box<dyn aqfs::File>, aqfs::Error> {
        Ok(Box::new(File {
            meta: aqfs::FileMeta {
                path: aqfs::Path::new(vec!["hoge".to_string(), "piyo".to_string()]),
                create_datetime: Utc::now(),
                modify_datetime: Utc::now(),
            },
        }))
    }

    async fn create_file(&self, file: &mut impl aqfs::File) -> Result<(), aqfs::Error> {
        Err(aqfs::Error::NotImplemented)
    }

    async fn remove_file(&self, meta: &aqfs::FileMeta) -> Result<(), aqfs::Error> {
        Err(aqfs::Error::NotImplemented)
    }

    async fn create_dir(&self, meta: &aqfs::FileMeta) -> Result<(), aqfs::Error> {
        Err(aqfs::Error::NotImplemented)
    }
}
