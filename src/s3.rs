use crate::aqfs;
use async_trait::async_trait;
use chrono::Utc;
use futures;
use rusoto_core::Region;
use rusoto_s3::{GetObjectRequest, ListObjectsV2Request, PutObjectRequest, S3Client, S3};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

pub struct File {
    meta: aqfs::FileMeta,
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
        // Fetch all journal objects from S3 in parallel.
        let futures = journal_objects
            .into_iter()
            .map(|o| self.get_object(o.key.unwrap()))
            .collect::<Vec<_>>();
        let outputs = futures::future::try_join_all(futures).await?;
        // Join all journals into one string.
        let mut journal = Vec::new();
        for o in outputs.into_iter() {
            o.body
                .unwrap()
                .into_async_read()
                .read_to_end(&mut journal)
                .await
                .map_err(|e| aqfs::Error::RusotoFail(format!("Failed in GetObject: {}", e)))?;
        }
        // FIXME: Parse the journal to get Vec<aqfs::FileMeta>.
        let journal = journal;
        println!("{:?}", journal);
        Err(aqfs::Error::NotImplemented)
    }

    async fn fetch_file(&self, _meta: &aqfs::FileMeta) -> Result<Box<dyn aqfs::File>, aqfs::Error> {
        Ok(Box::new(File {
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
        self.put_object(key, Some(file.read_all().await?.into()))
            .await?;

        // Create journal and put it to journal/.
        let journal_key = format!(
            "journal/{}-{}",
            Utc::now().format("%Y%m%d%H%M%S%f"),
            Uuid::new_v4().to_simple().to_string()
        );
        // FIXME: Create proper journal
        let meta = file.meta();
        let journal = format!(
            "{}{}{}",
            meta.path.to_string(),
            meta.create_datetime,
            meta.modify_datetime
        )
        .into_bytes();
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
