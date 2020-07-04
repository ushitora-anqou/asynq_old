use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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

#[async_trait(?Send)]
pub trait StorageEntity<F: File> {
    async fn list_files(&mut self) -> Result<Vec<F>, Error>;
    async fn create_file(&mut self, file: &mut impl File) -> Result<(), Error>;
    async fn remove_file(&mut self, file: &F) -> Result<(), Error>;
}
