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
}

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
        self.client
            .get_object(request)
            .await
            .map_err(|e| aqfs::Error::RusotoFail(format!("Failed in GetObject: {}", e)))
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
        self.client
            .put_object(request)
            .await
            .map_err(|e| aqfs::Error::RusotoFail(format!("Failed in PutObject: {}", e)))
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
            .map_err(|e| aqfs::Error::RusotoFail(format!("Failed in ListObjectsV2: {}", e)))?
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
                    .await
                    .map_err(|e| aqfs::Error::SerdeFail(format!("Failed in GetObject: {}", e)))?;
                bincode::deserialize::<JournalFile>(&src[..]).map_err(|e| {
                    aqfs::Error::SerdeFail(format!("Failed in bincode::deserialize: {}", e))
                })
            })
            .collect::<Vec<_>>();
        let journal_files: Vec<JournalFile> = futures::future::try_join_all(futures).await?;
        // Turn journal files into one journal.
        let journal = journal_files
            .into_iter()
            .flat_map(|j| j.records.into_iter())
            .collect::<Vec<_>>();
        // FIXME: Cache for journal
        let metas = journal
            .into_iter()
            .map(|r| match r.journal {
                Journal::CreateFile { file } => file.meta,
            })
            .collect::<Vec<_>>();
        Ok(metas)
    }

    async fn fetch_file(&self, _meta: &aqfs::FileMeta) -> Result<Box<dyn aqfs::File>, aqfs::Error> {
        Ok(Box::new(File {
            key: "hoho".to_string(),
            meta: aqfs::FileMeta {
                path: aqfs::Path::new(vec!["hoge".to_string(), "piyo".to_string()]),
                create_datetime: Utc::now(),
                modify_datetime: Utc::now(),
            },
        }))
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
        })
        .map_err(|e| aqfs::Error::SerdeFail(format!("Failed in bincode::serialize: {}", e)))?;
        self.put_object(journal_key, Some(journal.into())).await?;

        Ok(())
    }

    async fn remove_file(&self, _meta: &aqfs::FileMeta) -> Result<(), aqfs::Error> {
        Err(aqfs::Error::NotImplemented)
    }

    async fn create_dir(&self, _meta: &aqfs::FileMeta) -> Result<(), aqfs::Error> {
        Err(aqfs::Error::NotImplemented)
    }
}
