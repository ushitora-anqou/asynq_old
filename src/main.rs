mod aqfs;
mod s3;

use crate::aqfs::StorageEntity;

use rusoto_core::Region;
use std::{env, str::FromStr};

#[tokio::main]
async fn main() {
    let region = match env::var("S3_REGION") {
        Ok(s) => Region::from_str(&s).unwrap(),
        Err(_) => Region::Custom {
            name: "s3-asynq".to_string(),
            endpoint: env::var("S3_ENDPOINT").unwrap_or("http://localhost:9000".to_string()),
        },
    };
    let bucket = env::var("S3_BUCKET").unwrap_or("asynq".to_string());

    let cloud = s3::Storage::new(region, bucket);
    println!("{:?}", cloud.list_filemetas().await.unwrap());
}
