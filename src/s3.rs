use crate::aqfs;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures;
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
        // Get list of journal files (objects) from S3.
        let mut request = ListObjectsV2Request::default();
        request.bucket = self.bucket.clone();
        request.prefix = Some("journal/".to_string());
        // FIXME: Use request.start_after to get the latest journal files.
        let mut journal_objects = self
            .client
            .list_objects_v2(request)
            .await
            .map_err(|e| aqfs::Error::Unexpected(format!("Rusoto failed: {}", e)))?
            .contents
            .ok_or(aqfs::Error::Unexpected(format!("Rusoto failed")))?;
        // Sort by its name.
        journal_objects.sort_by_key(|o| o.key.clone().unwrap());
        // Fetch all journal objects from S3 in parallel.
        let futures = journal_objects
            .into_iter()
            .map(|o| {
                let mut request = GetObjectRequest::default();
                request.bucket = self.bucket.clone();
                request.key = o.key.unwrap();
                self.client.get_object(request)
            })
            .collect::<Vec<_>>();
        let outputs = futures::future::try_join_all(futures)
            .await
            .map_err(|e| aqfs::Error::Unexpected(format!("Rusoto failed: {}", e)))?;
        // Join all journals into one string.
        let mut journal = String::new();
        for o in outputs.into_iter() {
            o.body
                .unwrap()
                .into_async_read()
                .read_to_string(&mut journal)
                .await
                .map_err(|e| aqfs::Error::Unexpected(format!("Rusoto failed: {}", e)))?;
        }
        // FIXME: Parse the journal to get Vec<aqfs::FileMeta>.
        let journal = journal;
        println!("{:?}", journal);
        Err(aqfs::Error::Unexpected("".to_string()))
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
