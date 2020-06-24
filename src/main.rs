mod aqfs;
mod s3;

use crate::aqfs::StorageEntity;
use chrono::Utc;

#[tokio::main]
async fn main() {
    let cloud = s3::Storage::default();
    cloud
        .create_file(&mut aqfs::RamFile::new(
            aqfs::FileMeta {
                path: aqfs::Path::new(vec!["hogehogehoge".to_string()]),
                create_datetime: Utc::now(),
                modify_datetime: Utc::now(),
            },
            "piyopiyopiyo".to_string().into_bytes(),
        ))
        .await
        .unwrap();

    println!("{:?}", cloud.list_filemetas().await.unwrap());
}
