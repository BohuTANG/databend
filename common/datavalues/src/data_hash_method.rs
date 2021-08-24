// Copyright 2020 Datafuse Labs.
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

use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

use common_exception::Result;

use crate::prelude::*;
use crate::series::Series;

pub trait HashMethod<T>
where T: std::cmp::Eq + Hash + Clone + Debug
{
    fn name(&self) -> String;
    fn build_keys(&self, group_columns: &[&DataColumn], rows: usize) -> Result<Vec<T>>;
}

pub type HashMethodKeysU8 = HashMethodFixedKeys<UInt8Type>;
pub type HashMethodKeysU16 = HashMethodFixedKeys<UInt16Type>;
pub type HashMethodKeysU32 = HashMethodFixedKeys<UInt32Type>;
pub type HashMethodKeysU64 = HashMethodFixedKeys<UInt64Type>;

pub enum HashMethodKind {
    Serializer(HashMethodSerializer),
    KeysU8(HashMethodKeysU8),
    KeysU16(HashMethodKeysU16),
    KeysU32(HashMethodKeysU32),
    KeysU64(HashMethodKeysU64),
}

impl HashMethodKind {
    pub fn name(&self) -> String {
        match self {
            HashMethodKind::Serializer(v) => v.name(),
            HashMethodKind::KeysU8(v) => v.name(),
            HashMethodKind::KeysU16(v) => v.name(),
            HashMethodKind::KeysU32(v) => v.name(),
            HashMethodKind::KeysU64(v) => v.name(),
        }
    }
    pub fn data_type(&self) -> DataType {
        match self {
            HashMethodKind::Serializer(_) => DataType::Binary,
            HashMethodKind::KeysU8(_) => DataType::UInt8,
            HashMethodKind::KeysU16(_) => DataType::UInt16,
            HashMethodKind::KeysU32(_) => DataType::UInt32,
            HashMethodKind::KeysU64(_) => DataType::UInt64,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct HashMethodSerializer {}

impl HashMethodSerializer {
    #[inline]
    pub fn get_key(&self, array: &DFBinaryArray, row: usize) -> Vec<u8> {
        let v = array.as_ref().value(row);
        v.to_owned()
    }

    pub fn de_group_columns(
        &self,
        keys: Vec<Vec<u8>>,
        group_fields: &[DataField],
    ) -> Result<Vec<Series>> {
        let mut keys: Vec<&[u8]> = keys.iter().map(|x| x.as_slice()).collect();
        let rows = keys.len();

        let mut res = Vec::with_capacity(group_fields.len());
        for f in group_fields.iter() {
            let data_type = f.data_type();
            let mut deserializer = data_type.create_deserializer(rows)?;

            for (_row, key) in keys.iter_mut().enumerate() {
                deserializer.de(key)?;
            }
            res.push(deserializer.finish_to_series());
        }
        Ok(res)
    }
}

impl HashMethod<Vec<u8>> for HashMethodSerializer {
    fn name(&self) -> String {
        "Serializer".to_string()
    }

    fn build_keys(&self, group_columns: &[&DataColumn], rows: usize) -> Result<Vec<Vec<u8>>> {
        let mut group_keys = Vec::with_capacity(rows);
        {
            let mut group_key_len = 0;
            for col in group_columns {
                let typ = col.data_type();
                if is_integer(&typ) {
                    group_key_len += numeric_byte_size(&typ)?;
                } else {
                    group_key_len += 4;
                }
            }

            for _i in 0..rows {
                group_keys.push(Vec::with_capacity(group_key_len));
            }

            for col in group_columns {
                col.serialize(&mut group_keys)?;
            }
        }
        Ok(group_keys)
    }
}

pub struct HashMethodFixedKeys<T> {
    t: PhantomData<T>,
}

impl<T> HashMethodFixedKeys<T>
where T: DFNumericType
{
    pub fn default() -> Self {
        HashMethodFixedKeys { t: PhantomData }
    }

    #[inline]
    pub fn get_key(&self, array: &DataArray<T>, row: usize) -> T::Native {
        array.as_ref().value(row)
    }
    pub fn de_group_columns(
        &self,
        keys: Vec<T::Native>,
        group_fields: &[DataField],
    ) -> Result<Vec<Series>> {
        let mut keys = keys;
        let rows = keys.len();
        let step = std::mem::size_of::<T::Native>();
        let length = rows * step;
        let capacity = keys.capacity() * step;
        let mutptr = keys.as_mut_ptr() as *mut u8;
        let vec8 = unsafe {
            std::mem::forget(keys);
            // construct new vec
            Vec::from_raw_parts(mutptr, length, capacity)
        };

        let mut res = Vec::with_capacity(group_fields.len());
        let mut offsize = 0;
        for f in group_fields.iter() {
            let data_type = f.data_type();
            let mut deserializer = data_type.create_deserializer(rows)?;
            let reader = vec8.as_slice();
            deserializer.de_batch(&reader[offsize..], step, rows)?;
            res.push(deserializer.finish_to_series());

            offsize += numeric_byte_size(data_type)?;
        }
        Ok(res)
    }
}

impl<T> HashMethod<T::Native> for HashMethodFixedKeys<T>
where
    T: DFNumericType,
    T::Native: std::cmp::Eq + Hash + Clone + Debug,
{
    fn name(&self) -> String {
        format!("FixedKeys{}", std::mem::size_of::<T::Native>())
    }

    fn build_keys(&self, group_columns: &[&DataColumn], rows: usize) -> Result<Vec<T::Native>> {
        let step = std::mem::size_of::<T::Native>();
        let mut group_keys: Vec<T::Native> = vec![T::Native::default(); rows];
        let ptr = group_keys.as_mut_ptr() as *mut u8;
        let mut offsize = 0;
        let mut size = step;
        while size > 0 {
            build(size, &mut offsize, group_columns, ptr, step)?;
            size /= 2;
        }
        Ok(group_keys)
    }
}

#[inline]
fn build(
    mem_size: usize,
    offsize: &mut usize,
    group_columns: &[&DataColumn],
    writer: *mut u8,
    step: usize,
) -> Result<()> {
    for col in group_columns.iter() {
        let data_type = col.data_type();
        let size = numeric_byte_size(&data_type)?;
        if size == mem_size {
            let series = col.to_array()?;

            let writer = unsafe { writer.add(*offsize) };
            series.fixed_hash(writer, step)?;
            *offsize += size;
        }
    }
    Ok(())
}
