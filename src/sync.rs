use crate::aqfs;

struct StorageSyncer<
    ST0: aqfs::StorageEntity<F0>,
    ST1: aqfs::StorageEntity<F1>,
    F0: aqfs::File,
    F1: aqfs::File,
> {
    st0: ST0,
    st1: ST1,

    // Thanks to: https://qnighy.hatenablog.com/entry/2018/01/14/220000
    _marker0: std::marker::PhantomData<fn() -> F0>,
    _marker1: std::marker::PhantomData<fn() -> F1>,
}

impl<
        ST0: aqfs::StorageEntity<F0>,
        ST1: aqfs::StorageEntity<F1>,
        F0: aqfs::File,
        F1: aqfs::File,
    > StorageSyncer<ST0, ST1, F0, F1>
{
    pub fn new(st0: ST0, st1: ST1) -> Self {
        Self {
            st0,
            st1,
            _marker0: std::marker::PhantomData,
            _marker1: std::marker::PhantomData,
        }
    }

    pub async fn sync(&mut self) -> Result<(), aqfs::Error> {
        // FIXME: We MUST need MUCH MUCH smarter algorithms here.
        // Send files from st0 to st1.
        for f in self.st0.list_files().await?.into_iter() {
            self.st1.create_file(f).await?;
        }
        // Send files from st1 to st0.
        for f in self.st1.list_files().await?.into_iter() {
            self.st0.create_file(f).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::aqfs::{File, StorageEntity};
    use chrono::offset::TimeZone;
    use chrono::Utc;
    use std::collections::HashMap;

    async fn is_storages_equivalent(
        st0: &mut aqfs::RamStorage,
        st1: &mut aqfs::RamStorage,
    ) -> bool {
        let st0_files = st0.list_files().await.unwrap();
        let st1_files = st1.list_files().await.unwrap();
        let mut files = HashMap::new();
        for mut f in st0_files.into_iter() {
            files.insert(f.meta().clone(), f.read_all().await.unwrap());
        }
        let files = files;
        for mut f in st1_files.into_iter() {
            match files.get(f.meta()) {
                None => return false,
                Some(str0) => {
                    let str1 = f.read_all().await.unwrap();
                    if *str0 != str1 {
                        return false;
                    }
                }
            }
        }
        true
    }

    #[tokio::test]
    async fn works() -> Result<(), aqfs::Error> {
        let mut st0 = aqfs::RamStorage::new();
        st0.create_file(aqfs::RamFile::new(
            aqfs::FileMeta {
                path: aqfs::Path::new(vec!["dummy-path0".to_string()]),
                mtime: Utc.timestamp(0, 0),
            },
            "dummy content 0".to_string().into_bytes(),
        ))
        .await?;
        let mut st1 = aqfs::RamStorage::new();
        st1.create_file(aqfs::RamFile::new(
            aqfs::FileMeta {
                path: aqfs::Path::new(vec!["dummy-path1".to_string()]),
                mtime: Utc.timestamp(0, 0),
            },
            "dummy content 1".to_string().into_bytes(),
        ))
        .await?;
        let mut syncer = StorageSyncer::new(st0, st1);
        syncer.sync().await?;
        assert_eq!(syncer.st0.list_files().await.unwrap().len(), 2);
        assert_eq!(syncer.st1.list_files().await.unwrap().len(), 2);
        assert!(is_storages_equivalent(&mut syncer.st0, &mut syncer.st1).await);
        Ok(())
    }
}
