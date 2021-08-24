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

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

use common_datavalues::DataValue;
use common_exception::Result;

use crate::DataBlock;

type GroupIndices<T> = HashMap<T, (Vec<u32>, Vec<DataValue>), ahash::RandomState>;
type GroupBlock<T> = Vec<(T, Vec<DataValue>, DataBlock)>;

impl DataBlock {
    /// Hash group based on row index then return indices and keys.
    /// For example:
    /// row_idx, A
    /// 0, 1
    /// 1, 2
    /// 2, 3
    /// 3, 4
    /// 4, 5
    ///
    /// grouping by [A%3]
    /// 1)
    /// row_idx, group_key, A
    /// 0, 1, 1
    /// 1, 2, 2
    /// 2, 0, 3
    /// 3, 1, 4
    /// 4, 2, 5
    ///
    /// 2) make indices group(for vector compute)
    /// group_key, indices
    /// 0, [2]
    /// 1, [0, 3]
    /// 2, [1, 4]
    ///

    pub fn group_by_get_indices<T>(
        hash: Box<dyn common_datavalues::HashMethod<T>>,
        block: &DataBlock,
        column_names: &[String],
    ) -> Result<GroupIndices<T>>
    where
        T: std::cmp::Eq + Hash + Clone + Debug,
    {
        // Table for <group_key, (indices, keys) >
        let mut group_indices = GroupIndices::<T>::default();
        // 1. Get group by columns.
        let mut group_columns = Vec::with_capacity(column_names.len());
        {
            for col in column_names {
                group_columns.push(block.try_column_by_name(col)?);
            }
        }

        // 2. Build serialized keys
        let group_keys = hash.build_keys(&group_columns, block.num_rows())?;
        // 2. Make group with indices.
        {
            for (row, group_key) in group_keys.iter().enumerate().take(block.num_rows()) {
                match group_indices.get_mut(group_key) {
                    None => {
                        let mut group_values = Vec::with_capacity(group_columns.len());
                        for col in &group_columns {
                            group_values.push(col.try_get(row)?);
                        }
                        group_indices.insert(group_key.clone(), (vec![row as u32], group_values));
                    }
                    Some((v, _)) => {
                        v.push(row as u32);
                    }
                }
            }
        }

        Ok(group_indices)
    }

    /// Hash group based on row index by column names.
    ///
    /// group_by_get_indices and make blocks.
    pub fn group_by<T>(
        hash: Box<dyn common_datavalues::HashMethod<T>>,
        block: &DataBlock,
        column_names: &[String],
    ) -> Result<GroupBlock<T>>
    where
        T: std::cmp::Eq + Hash + Clone + Debug,
    {
        let group_indices = DataBlock::group_by_get_indices(hash, block, column_names)?;
        // Table for <(group_key, keys, block)>
        let mut group_blocks = GroupBlock::with_capacity(group_indices.len());

        for (group_key, (group_indices, group_keys)) in group_indices {
            let take_block = DataBlock::block_take_by_indices(block, column_names, &group_indices)?;
            group_blocks.push((group_key, group_keys, take_block));
        }

        Ok(group_blocks)
    }
}
