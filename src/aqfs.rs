use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Error {
    // Generic
    Unexpected(String),
    NotImplemented,

    // For s3
    RusotoFail(String),
    SerdeFail(String),
}

impl From<std::io::Error> for Error {
    fn from(from: std::io::Error) -> Self {
        Error::Unexpected(from.to_string())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct Path {
    elms: Vec<String>,
}

impl Path {
    pub fn new(elms: Vec<String>) -> Self {
        Path { elms }
    }
}

impl From<&Path> for std::path::PathBuf {
    fn from(from: &Path) -> Self {
        let mut path = std::path::PathBuf::new();
        for elm in &from.elms {
            path.push(elm);
        }
        path
    }
}

impl ToString for Path {
    fn to_string(&self) -> String {
        self.elms.join("/")
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileMeta {
    pub path: Path,
    pub mtime: DateTime<Utc>,
}

#[async_trait(?Send)]
pub trait File {
    fn meta(&self) -> &FileMeta;
    async fn read_all(&mut self) -> Result<Vec<u8>, Error>;
}

#[async_trait(?Send)]
pub trait StorageEntity<F: File> {
    async fn list_files(&mut self) -> Result<Vec<F>, Error>;
    async fn create_file(&mut self, mut file: impl File + 'async_trait) -> Result<(), Error>;
    async fn remove_file(&mut self, file: &F) -> Result<(), Error>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RamFile {
    meta: FileMeta,
    data: Vec<u8>,
}

impl RamFile {
    pub fn new(meta: FileMeta, data: Vec<u8>) -> Self {
        Self { meta, data }
    }
}

#[async_trait(?Send)]
impl File for RamFile {
    fn meta(&self) -> &FileMeta {
        &self.meta
    }

    async fn read_all(&mut self) -> Result<Vec<u8>, Error> {
        Ok(self.data.clone())
    }
}

pub struct RamStorage {
    files: HashMap<Path, RamFile>,
}

impl RamStorage {
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
        }
    }
}

#[async_trait(?Send)]
impl StorageEntity<RamFile> for RamStorage {
    async fn list_files(&mut self) -> Result<Vec<RamFile>, Error> {
        Ok(self
            .files
            .iter()
            .map(|f| f.1.clone())
            .collect::<Vec<RamFile>>())
    }

    async fn create_file(&mut self, mut file: impl File + 'async_trait) -> Result<(), Error> {
        self.files.insert(
            file.meta().path.clone(),
            RamFile::new(file.meta().clone(), file.read_all().await?),
        );
        Ok(())
    }

    async fn remove_file(&mut self, file: &RamFile) -> Result<(), Error> {
        self.files.remove(&file.meta().path);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::offset::TimeZone;

    #[tokio::test]
    async fn ram_storage_works() -> Result<(), Error> {
        let mut storage = RamStorage::new();
        let files = storage.list_files().await?;
        assert_eq!(files.len(), 0);
        storage
            .create_file(RamFile::new(
                FileMeta {
                    path: Path::new(vec!["dummy-path".to_string()]),
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
