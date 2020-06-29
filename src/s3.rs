use crate::aqfs;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures;
use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, ListObjectsV2Request, PutObjectRequest, S3Client, S3};
use serde::{Deserialize, Serialize};
use std::{env, str::FromStr};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

impl<E: std::error::Error + 'static> From<rusoto_core::RusotoError<E>> for aqfs::Error {
    fn from(from: rusoto_core::RusotoError<E>) -> Self {
        aqfs::Error::RusotoFail(format!("{}", from))
    }
}

impl From<bincode::Error> for aqfs::Error {
    fn from(from: bincode::Error) -> Self {
        aqfs::Error::SerdeFail(from.to_string())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct File {
    meta: aqfs::FileMeta,
    key: String,
}

#[async_trait]
impl aqfs::File for File {
    fn meta(&self) -> &aqfs::FileMeta {
        &self.meta
    }

    async fn read_all(&mut self) -> Result<Vec<u8>, aqfs::Error> {
        Err(aqfs::Error::NotImplemented)
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum Journal {
    CreateFile { file: File },
}

#[derive(Serialize, Deserialize, Debug)]
struct JournalRecord {
    journal: Journal,
    timestamp: DateTime<Utc>,
    key: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct JournalFile {
    records: Vec<JournalRecord>,
    // FIXME: Add blockchain to detect any branch on the journal.
}

pub struct Storage {
    client: S3Client,
    bucket: String,
}

impl Storage {
    pub fn new(region: Region, bucket: String) -> Self {
        Storage {
            client: S3Client::new(region),
            bucket: bucket,
        }
    }

    pub fn default() -> Storage {
        let region = match env::var("S3_REGION") {
            Ok(s) => Region::from_str(&s).unwrap(),
            Err(_) => Region::Custom {
                name: "s3-asynq".to_string(),
                endpoint: env::var("S3_ENDPOINT").unwrap_or("http://localhost:9000".to_string()),
            },
        };
        let bucket = env::var("S3_BUCKET").unwrap_or("asynq".to_string());
        Self::new(region, bucket)
    }

    async fn get_object(&self, key: String) -> Result<rusoto_s3::GetObjectOutput, aqfs::Error> {
        let mut request = GetObjectRequest::default();
        request.bucket = self.bucket.clone();
        request.key = key;
        Ok(self.client.get_object(request).await?)
    }

    async fn put_object(
        &self,
        key: String,
        body: Option<rusoto_s3::StreamingBody>,
    ) -> Result<rusoto_s3::PutObjectOutput, aqfs::Error> {
        let mut request = PutObjectRequest::default();
        request.bucket = self.bucket.clone();
        request.key = key;
        request.body = body;
        Ok(self.client.put_object(request).await?)
    }

    async fn list_objects_v2(
        &self,
        prefix: String,
    ) -> Result<rusoto_s3::ListObjectsV2Output, aqfs::Error> {
        let mut request = ListObjectsV2Request::default();
        request.bucket = self.bucket.clone();
        request.prefix = Some(prefix);
        Ok(self.client.list_objects_v2(request).await?)
    }
}

#[async_trait(?Send)]
impl aqfs::StorageEntity for Storage {
    async fn list_filemetas(&self) -> Result<Vec<aqfs::FileMeta>, aqfs::Error> {
        // Get list of journal files (objects) from S3.
        let mut journal_objects = self
            .list_objects_v2("journal/".to_string())
            .await?
            .contents
            .ok_or(aqfs::Error::RusotoFail(
                "Failed in ListObjectsV2: Can't read the content".to_string(),
            ))?;
        // Sort by its name.
        journal_objects.sort_by_key(|o| o.key.clone().unwrap());
        // Fetch all journal files from S3 in parallel.
        let futures = journal_objects
            .into_iter()
            .map(|o| async {
                // Get the object, read it, and parse it into struct JournalFile.
                let mut src = Vec::new();
                self.get_object(o.key.unwrap())
                    .await?
                    .body
                    .unwrap()
                    .into_async_read()
                    .read_to_end(&mut src)
                    .await?;
                Ok::<JournalFile, aqfs::Error>(bincode::deserialize::<JournalFile>(&src[..])?)
            })
            .collect::<Vec<_>>();
        let journal_files: Vec<JournalFile> = futures::future::try_join_all(futures).await?;
        // Turn journal files into one journal.
        let journal = journal_files
            .into_iter()
            .flat_map(|j| j.records.into_iter())
            .collect::<Vec<_>>();
        // FIXME: Cache for journal.
        // FIXME: Follow the journal to get correct current files.
        let metas = journal
            .into_iter()
            .map(|r| match r.journal {
                Journal::CreateFile { file } => file.meta,
            })
            .collect::<Vec<_>>();
        Ok(metas)
    }

    async fn fetch_file(&self, _meta: &aqfs::FileMeta) -> Result<Box<dyn aqfs::File>, aqfs::Error> {
        Err(aqfs::Error::NotImplemented)
    }

    async fn create_file(&self, file: &mut impl aqfs::File) -> Result<(), aqfs::Error> {
        // Upload the file's content.
        let key = format!("data/{}", Uuid::new_v4().to_simple().to_string());
        self.put_object(key.clone(), Some(file.read_all().await?.into()))
            .await?;

        // Create journal and put it to journal/.
        let timestamp = Utc::now();
        let journal_key = format!(
            "journal/{}-{}",
            timestamp.format("%Y%m%d%H%M%S%f"),
            Uuid::new_v4().to_simple().to_string()
        );
        let meta = file.meta().clone();
        let journal = bincode::serialize(&JournalFile {
            records: vec![JournalRecord {
                timestamp,
                key: journal_key.clone(),
                journal: Journal::CreateFile {
                    file: File { meta, key },
                },
            }],
        })?;
        self.put_object(journal_key, Some(journal.into())).await?;
        // FIXME: Check if the upload has been done successfully, especially any branch of the journal did not occur.

        Ok(())
    }

    async fn remove_file(&self, _meta: &aqfs::FileMeta) -> Result<(), aqfs::Error> {
        Err(aqfs::Error::NotImplemented)
    }

    async fn create_dir(&self, _meta: &aqfs::FileMeta) -> Result<(), aqfs::Error> {
        Err(aqfs::Error::NotImplemented)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::aqfs::StorageEntity;

    async fn get_test_storage() -> Storage {
        let region = Region::Custom {
            name: "s3-asynq-test".to_string(),
            endpoint: "http://localhost:9000".to_string(),
        };
        let bucket = format!("asynq-test-{}", Uuid::new_v4().to_simple());
        let client = S3Client::new(region.clone());

        // create new bucket.
        let mut request = rusoto_s3::CreateBucketRequest::default();
        request.bucket = bucket.clone();
        client
            .create_bucket(request)
            .await
            .expect("No connection to S3");

        Storage::new(region, bucket)
    }

    #[tokio::test]
    async fn works() -> Result<(), aqfs::Error> {
        let cloud = get_test_storage().await;

        cloud
            .create_file(&mut aqfs::RamFile::new(
                aqfs::FileMeta {
                    path: aqfs::Path::new(vec!["dummy-path".to_string()]),
                    mtime: Utc::now(),
                },
                "dummy content".to_string().into_bytes(),
            ))
            .await?;

        cloud.list_filemetas().await?;

        Ok(())
    }
}
