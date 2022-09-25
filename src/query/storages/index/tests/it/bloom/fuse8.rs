// Copyright 2021 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::Series;
use common_datavalues::SeriesFrom;
use common_exception::Result;
use common_storages_index::bloom::Bloom;
use common_storages_index::bloom::Fuse8Bloom;
use rand::prelude::random;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

#[test]
fn test_xor_bitmap_u64() -> Result<()> {
    let seed: u64 = random();
    let numbers = 1_000_000;

    let size = 8 * numbers;
    let mut rng = StdRng::seed_from_u64(seed);
    let keys: Vec<u64> = (0..numbers).map(|_| rng.gen::<u64>()).collect();

    let mut filter = Fuse8Bloom::create(numbers as u32);
    filter.add_keys(&keys);
    filter.build()?;

    for key in keys.iter() {
        assert!(filter.contains(key), "key {} not present", key);
    }

    let val = filter.to_bytes()?;
    let (_, n) = Fuse8Bloom::from_bytes(&val)?;
    assert_eq!(n, val.len(), "{} {}", n, val.len());

    // Lock the size.
    assert_eq!(n, 1130544);

    // fuse8: u64 bitmap encoding:1130544 bytes, raw:8000000 bytes, ratio:0.141318
    println!(
        "fuse8: u64 bitmap encoding:{} bytes, raw:{} bytes, ratio:{}",
        val.len(),
        size,
        val.len() as f32 / size as f32
    );

    Ok(())
}

#[test]
fn test_xor_bitmap_bool() -> Result<()> {
    let seed: u64 = random();
    let numbers = 1_000_000;

    let size = numbers;
    let mut rng = StdRng::seed_from_u64(seed);
    let keys: Vec<bool> = (0..numbers).map(|_| rng.gen::<u64>() % 2 == 0).collect();

    let mut filter = Fuse8Bloom::create(numbers as u32);
    for key in keys.clone().into_iter() {
        filter.add_key(&key);
    }
    filter.build()?;

    for key in keys.iter() {
        assert!(filter.contains(key), "key {} not present", key);
    }

    let val = filter.to_bytes()?;
    let (_, n) = Fuse8Bloom::from_bytes(&val)?;
    assert_eq!(n, val.len(), "{} {}", n, val.len());

    // Lock the size.
    assert_eq!(n, 1130544);

    // fuse8: bool bitmap encoding:1130544 bytes, raw:1000000 bytes, ratio:1.130544
    println!(
        "fuse8: bool bitmap encoding:{} bytes, raw:{} bytes, ratio:{}",
        val.len(),
        size,
        val.len() as f32 / size as f32
    );

    Ok(())
}

#[test]
fn test_xor_bitmap_string() -> Result<()> {
    let seed: u64 = random();
    let numbers = 100_000;

    let len = 30;
    let size = 30 * numbers;
    let mut rng = StdRng::seed_from_u64(seed);
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                            abcdefghijklmnopqrstuvwxyz\
                            0123456789)(*&^%$#@!~";

    let keys: Vec<String> = (0..numbers)
        .map(|_| {
            (0..len)
                .map(|_| {
                    let idx = rng.gen_range(0..CHARSET.len());
                    CHARSET[idx] as char
                })
                .collect()
        })
        .collect();

    let mut filter = Fuse8Bloom::create(numbers as u32);
    filter.add_keys(&keys);
    filter.build()?;

    for key in keys.iter() {
        assert!(filter.contains(key), "key {} not present", key);
    }

    let val = filter.to_bytes()?;
    let (_, n) = Fuse8Bloom::from_bytes(&val)?;
    assert_eq!(n, val.len(), "{} {}", n, val.len());

    // Lock the size.
    assert_eq!(n, 118832);

    // fuse8: string encoding:118832 bytes, raw:3000000 bytes, ratio:0.039610665
    println!(
        "fuse8: string encoding:{} bytes, raw:{} bytes, ratio:{}",
        val.len(),
        size,
        val.len() as f32 / size as f32
    );

    Ok(())
}

#[test]
fn test_xor_bitmap_duplicate_string() -> Result<()> {
    let numbers = 100_000;

    let key = "123456789012345678901234567890";
    let len = key.len();
    let size = len * numbers;

    let keys: Vec<String> = (0..numbers).map(|_| key.to_string()).collect();

    let mut filter = Fuse8Bloom::create(numbers as u32);
    for key in keys.clone().into_iter() {
        filter.add_key(&key);
    }
    filter.build()?;

    assert!(filter.contains(&keys[0]), "key {} not present", key);

    let val = filter.to_bytes()?;
    let (_, n) = Fuse8Bloom::from_bytes(&val)?;
    assert_eq!(n, val.len(), "{} {}", n, val.len());

    // Lock the size.
    assert_eq!(n, 118832);

    // fuse8: same string encoding:118832 bytes, raw:3000000 bytes, ratio:0.039610665
    println!(
        "fuse8: same string encoding:{} bytes, raw:{} bytes, ratio:{}",
        val.len(),
        size,
        val.len() as f32 / size as f32
    );

    Ok(())
}

#[test]
fn test_xor_bitmap_data_block() -> Result<()> {
    let seed: u64 = random();
    let numbers = 1_000_000;

    let size = 8 * numbers;
    let mut rng = StdRng::seed_from_u64(seed);
    let keys: Vec<i64> = (0..numbers).map(|_| rng.gen::<i64>()).collect();

    let schema = DataSchemaRefExt::create(vec![DataField::new("a", i64::to_data_type())]);
    let block = DataBlock::create(schema, vec![Series::from_data(keys)]);
    let column = block.try_column_by_name("a")?;

    let mut filter = Fuse8Bloom::create(numbers as u32);
    filter.add_keys(&column.to_values());
    filter.build()?;

    for key in column.to_values() {
        assert!(filter.contains(&key), "key {} not present", key);
    }

    let val = filter.to_bytes()?;
    let (_, n) = Fuse8Bloom::from_bytes(&val)?;
    assert_eq!(n, val.len(), "{} {}", n, val.len());

    // data block(i64) enc:1130544, raw:8000000, ratio:0.141318
    // Actually, it not related to datablock, it related to the type of the column
    // Here it same as u64.
    println!(
        "data block(i64) enc:{}, raw:{}, ratio:{}",
        val.len(),
        size,
        val.len() as f32 / size as f32
    );

    Ok(())
}
