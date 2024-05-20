// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs;
use std::fs::File;
use std::io::Write;
use futures::stream;
use databend_common_catalog::table_context::TableContext;
use databend_common_config::InnerConfig;
use databend_common_exception::Result;
use databend_common_meta_app::storage::{StorageFsConfig, StorageParams};
use databend_query::test_kits::TestFixture;
use databend_storages_common_io::Files;

fn generate_files(path: &str, num_files: usize, file_size: usize) -> Result<Vec<String>> {
    let mut result = Vec::new();
    fs::create_dir_all(path)?;
    for i in 0..num_files {
        let file_path = format!("{}/file_{}.txt", path, i);
        result.push(file_path.clone());
        let mut file = File::create(&file_path)?;
        let data = vec![0u8; file_size];
        file.write_all(&data)?;
    }
    Ok(result)
}

// cargo test -- --show-output io::opendal::test_remove_via_files
#[tokio::test(flavor = "multi_thread")]
async fn test_remove_via_files() -> Result<()> {
    let tmp_dir = tempfile::Builder::new()
        .prefix("test_delete_files")
        .tempdir()
        .unwrap();

    let path = tmp_dir.path().to_str().unwrap();
    let mut config = InnerConfig::default();
    config.storage.params = StorageParams::Fs(StorageFsConfig {
        root: path.to_string()
    });

    let test = TestFixture::setup_with_config(&config).await?;
    let ctx = test.new_query_ctx().await?;
    let op = ctx.get_data_operator()?.operator();


    let files = generate_files(path, 100000, 0)?;

    let now = std::time::SystemTime::now();
    op.remove_via(stream::iter(files)).await?;
    let elapsed = now.elapsed().unwrap();
    println!("Time elapsed in deleting files: {:?}", elapsed);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_batch_remove_files() -> Result<()> {
    let tmp_dir = tempfile::Builder::new()
        .prefix("test_delete_files")
        .tempdir()
        .unwrap();

    let path = tmp_dir.path().to_str().unwrap();
    let mut config = InnerConfig::default();
    config.storage.params = StorageParams::Fs(StorageFsConfig {
        root: path.to_string()
    });

    let test = TestFixture::setup_with_config(&config).await?;
    let ctx = test.new_query_ctx().await?;
    let op = ctx.get_data_operator()?.operator();

    let files_op = Files::create(ctx.clone(), op.clone());

    let files = generate_files(path, 100000, 0)?;

    let now = std::time::SystemTime::now();
    files_op.remove_file_in_batch(files).await?;
    let elapsed = now.elapsed().unwrap();
    println!("Time elapsed in deleting files: {:?}", elapsed);

    Ok(())
}

