use async_trait::async_trait;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Error {
    Unexpected(String),
    NotImplemented,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Path {
    elms: Vec<String>,
}

impl Path {
    pub fn new(elms: Vec<String>) -> Self {
        Path { elms }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileMeta {
    pub path: Path,
    pub create_datetime: DateTime<Utc>,
    pub modify_datetime: DateTime<Utc>,
}

#[async_trait]
pub trait File {
    fn meta(&self) -> FileMeta;
    async fn read_all(&self) -> Result<Vec<u8>, Error>;
}

#[async_trait(?Send)]
pub trait StorageEntity {
    async fn list_filemetas(&self) -> Result<Vec<FileMeta>, Error>;
    async fn fetch_file(&self, meta: &FileMeta) -> Result<Box<dyn File>, Error>;
    async fn create_file(&self, file: &mut impl File) -> Result<(), Error>;
    async fn remove_file(&self, meta: &FileMeta) -> Result<(), Error>;
    async fn create_dir(&self, meta: &FileMeta) -> Result<(), Error>;
}
