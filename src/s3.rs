use crate::aqfs;
use crate::aqfs::File as FileTrait;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures;
use rusoto_core::Region;
use rusoto_s3::S3;
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, collections::HashMap, env, rc::Rc, str::FromStr};
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

struct S3Client {
    client: rusoto_s3::S3Client,
    bucket: String,
}

impl S3Client {
    pub fn new(region: Region, bucket: String) -> Self {
        Self {
            client: rusoto_s3::S3Client::new(region),
            bucket: bucket,
        }
    }

    async fn get_object(&self, key: String) -> Result<rusoto_s3::GetObjectOutput, aqfs::Error> {
        let mut request = rusoto_s3::GetObjectRequest::default();
        request.bucket = self.bucket.clone();
        request.key = key;
        Ok(self.client.get_object(request).await?)
    }

    async fn put_object(
        &self,
        key: String,
        body: Option<rusoto_s3::StreamingBody>,
    ) -> Result<rusoto_s3::PutObjectOutput, aqfs::Error> {
        let mut request = rusoto_s3::PutObjectRequest::default();
        request.bucket = self.bucket.clone();
        request.key = key;
        request.body = body;
        Ok(self.client.put_object(request).await?)
    }

    async fn list_objects_v2(
        &self,
        prefix: String,
    ) -> Result<rusoto_s3::ListObjectsV2Output, aqfs::Error> {
        let mut request = rusoto_s3::ListObjectsV2Request::default();
        request.bucket = self.bucket.clone();
        request.prefix = Some(prefix);
        Ok(self.client.list_objects_v2(request).await?)
    }
}

pub struct File {
    client: Rc<RefCell<S3Client>>,
    meta: aqfs::FileMeta,
    key: String,
}

#[async_trait(?Send)]
impl aqfs::File for File {
    fn meta(&self) -> &aqfs::FileMeta {
        &self.meta
    }

    async fn read_all(&mut self) -> Result<Vec<u8>, aqfs::Error> {
        let mut src = Vec::new();
        self.client
            .borrow()
            .get_object(self.key.clone())
            .await?
            .body
            .unwrap()
            .into_async_read()
            .read_to_end(&mut src)
            .await?;
        Ok(src)
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum Journal {
    CreateFile { meta: aqfs::FileMeta, key: String },
    RemoveFile { meta: aqfs::FileMeta },
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
    client: Rc<RefCell<S3Client>>,
}

impl Storage {
    pub fn new(region: Region, bucket: String) -> Self {
        Storage {
            client: Rc::new(RefCell::new(S3Client {
                client: rusoto_s3::S3Client::new(region),
                bucket: bucket,
            })),
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

    // Fetch and parse journal, and construct whole file system.
    async fn fetch_remote_filesystem(&mut self) -> Result<HashMap<aqfs::Path, File>, aqfs::Error> {
        // Get list of journal files (objects) from S3.
        let mut journal_objects = self
            .client
            .borrow()
            .list_objects_v2("journal/".to_string())
            .await?
            .contents
            .unwrap_or(vec![]);
        // Sort by its name.
        journal_objects.sort_by_key(|o| o.key.clone().unwrap());
        // Fetch all journal files from S3 in parallel.
        let futures = journal_objects
            .into_iter()
            .map(|o| async {
                // Get the object, read it, and parse it into struct JournalFile.
                let mut src = Vec::new();
                self.client
                    .borrow()
                    .get_object(o.key.unwrap())
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
        // Follow the journal and construct whole file system.
        let mut fs = HashMap::new();
        for rec in journal_files
            .into_iter()
            .flat_map(|j| j.records.into_iter())
        {
            match rec.journal {
                Journal::CreateFile { meta, key } => {
                    fs.insert(
                        meta.path.clone(),
                        File {
                            meta,
                            key,
                            client: Rc::clone(&self.client),
                        },
                    );
                }
                Journal::RemoveFile { meta } => {
                    fs.remove(&meta.path);
                }
            }
        }
        Ok(fs)
    }
}

#[async_trait(?Send)]
impl aqfs::StorageEntity<File> for Storage {
    async fn list_files(&mut self) -> Result<Vec<File>, aqfs::Error> {
        Ok(self
            .fetch_remote_filesystem()
            .await?
            .into_iter()
            .map(|(_, f)| f)
            .collect())
    }

    async fn create_file(
        &mut self,
        mut file: impl aqfs::File + 'async_trait,
    ) -> Result<(), aqfs::Error> {
        // Upload the file's content.
        let key = format!("data/{}", Uuid::new_v4().to_simple().to_string());
        self.client
            .borrow()
            .put_object(key.clone(), Some((&mut file).read_all().await?.into()))
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
                journal: Journal::CreateFile { meta, key },
            }],
        })?;
        self.client
            .borrow()
            .put_object(journal_key, Some(journal.into()))
            .await?;
        // FIXME: Check if the upload has been done successfully, especially any branch of the journal did not occur.

        Ok(())
    }

    async fn remove_file(&mut self, file: &File) -> Result<(), aqfs::Error> {
        // FIXME: Check if the file exists.
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
                journal: Journal::RemoveFile { meta },
            }],
        })?;
        self.client
            .borrow()
            .put_object(journal_key, Some(journal.into()))
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::aqfs::StorageEntity;
    use chrono::offset::TimeZone;

    async fn get_test_storage() -> Storage {
        let region = Region::Custom {
            name: "s3-asynq-test".to_string(),
            endpoint: "http://localhost:9000".to_string(),
        };
        let bucket = format!("asynq-test-{}", Uuid::new_v4().to_simple());
        let client = rusoto_s3::S3Client::new(region.clone());

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
        let mut storage = get_test_storage().await;
        let files = storage.list_files().await?;
        assert_eq!(files.len(), 0);
        storage
            .create_file(aqfs::RamFile::new(
                aqfs::FileMeta {
                    path: aqfs::Path::new(vec!["dummy-path".to_string()]),
                    mtime: Utc.timestamp(0, 0),
                },
                "dummy content".to_string().into_bytes(),
            ))
            .await?;
        let mut files = storage.list_files().await?;
        assert_eq!(files.len(), 1);
        let bytes = files[0].read_all().await?;
        assert_eq!(std::str::from_utf8(&bytes).unwrap(), "dummy content");
        storage.remove_file(&files[0]).await?;
        let files = storage.list_files().await?;
        assert_eq!(files.len(), 0);

        Ok(())
    }
}
