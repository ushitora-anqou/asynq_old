use crate::aqfs;
use crate::aqfs::File as FileTrait;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::io::{Read, Write};

pub struct File {
    meta: aqfs::FileMeta,
    realpath: std::path::PathBuf,
}

#[async_trait(?Send)]
impl aqfs::File for File {
    fn meta(&self) -> &aqfs::FileMeta {
        &self.meta
    }

    async fn read_all(&mut self) -> Result<Vec<u8>, aqfs::Error> {
        let mut f = std::fs::File::open(&self.realpath)?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        Ok(buf)
    }
}

pub struct Storage {
    root: std::path::PathBuf,
}

impl Storage {
    pub fn new(root: std::path::PathBuf) -> Self {
        if !root.is_dir() {
            panic!("Root should be a directory.");
        }
        Self { root }
    }

    fn get_real_path(&self, src: &aqfs::Path) -> std::path::PathBuf {
        self.root.join(std::path::PathBuf::from(src))
    }
}

#[async_trait(?Send)]
impl aqfs::StorageEntity<File> for Storage {
    async fn list_files(&mut self) -> Result<Vec<File>, aqfs::Error> {
        // FIXME: recursion
        let metas = std::fs::read_dir(&self.root)
            .map_err(|e| {
                aqfs::Error::Unexpected(format!(
                    "Can't read directory {}: {}",
                    self.root.to_string_lossy(),
                    e
                ))
            })?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let file_type = entry.file_type().ok()?;
                if !file_type.is_file() {
                    return None;
                }
                let metadata = entry.metadata().ok()?;
                let file_name = entry.file_name().into_string().ok()?;
                let path = aqfs::Path::new(vec![file_name]);
                let mtime = DateTime::<Utc>::from(metadata.modified().ok()?);
                Some(File {
                    realpath: self.get_real_path(&path),
                    meta: aqfs::FileMeta { path, mtime },
                })
            })
            .collect();
        Ok(metas)
    }

    async fn create_file(&mut self, file: &mut impl aqfs::File) -> Result<(), aqfs::Error> {
        // FIXME: Use a temporary file and move it to the correct path.
        let realpath = self.get_real_path(&file.meta().path);
        {
            let mut realfile = std::fs::File::create(&realpath)?;
            realfile.write_all(&file.read_all().await?)?;
        }
        filetime::set_file_mtime(
            &realpath,
            filetime::FileTime::from_system_time(std::time::SystemTime::from(file.meta().mtime)),
        )?;

        Ok(())
    }

    async fn remove_file(&mut self, file: &File) -> Result<(), aqfs::Error> {
        std::fs::remove_file(&file.realpath)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::aqfs::StorageEntity;
    use chrono::offset::TimeZone;
    use tempfile::TempDir;

    #[tokio::test]
    async fn works() -> Result<(), aqfs::Error> {
        let tmp_dir = TempDir::new()?;
        let mut storage = Storage::new(tmp_dir.path().to_path_buf());
        let files = storage.list_files().await?;
        assert_eq!(files.len(), 0);
        storage
            .create_file(&mut aqfs::RamFile::new(
                aqfs::FileMeta {
                    path: aqfs::Path::new(vec!["dummy-path".to_string()]),
                    mtime: Utc.timestamp(0, 0),
                },
                "dummy content".to_string().into_bytes(),
            ))
            .await?;
        assert_eq!(
            std::fs::metadata(tmp_dir.path().join("dummy-path"))?.modified()?,
            std::time::SystemTime::from(Utc.timestamp(0, 0))
        );
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
