// Copyright 2021 Datafuse Labs
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

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::group_hash_columns_slice;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Value;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::PipeItem;

use super::WindowPartitionMeta;

pub struct TransformWindowPartitionScatter {
    input_port: Arc<InputPort>,
    output_ports: Vec<Arc<OutputPort>>,
    input_data_blocks: VecDeque<DataBlock>,
    output_data_blocks: Vec<VecDeque<DataBlock>>,
    is_initialized: bool,
    hash_keys: Vec<usize>,
    num_processors: usize,
    num_partitions: usize,
}

impl TransformWindowPartitionScatter {
    pub fn new(
        num_processors: usize,
        num_partitions: usize,
        hash_keys: Vec<usize>,
    ) -> Result<Self> {
        let input_port = InputPort::create();
        let output_ports = (0..num_processors)
            .map(|_| OutputPort::create())
            .collect::<Vec<_>>();
        Ok(Self {
            input_port,
            output_ports,
            input_data_blocks: VecDeque::new(),
            output_data_blocks: vec![VecDeque::new(); num_processors],
            is_initialized: false,
            hash_keys,
            num_processors,
            num_partitions,
        })
    }

    pub fn finish(&mut self) -> Result<Event> {
        self.input_port.finish();
        for output_port in self.output_ports.iter() {
            output_port.finish();
        }
        Ok(Event::Finished)
    }

    pub fn into_pipe_item(self) -> PipeItem {
        let inputs = vec![self.input_port.clone()];
        let outputs = self.output_ports.clone();
        let processor_ptr = ProcessorPtr::create(Box::new(self));
        PipeItem::create(processor_ptr, inputs, outputs)
    }
}

impl Processor for TransformWindowPartitionScatter {
    fn name(&self) -> String {
        "TransformWindowPartitionScatter".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if !self.is_initialized {
            self.is_initialized = true;
            self.input_port.set_need_data();
            return Ok(Event::NeedData);
        }

        let mut all_output_finished = true;
        let mut need_consume = false;
        for (index, output_port) in self.output_ports.iter().enumerate() {
            if output_port.is_finished() {
                continue;
            }
            all_output_finished = false;

            if !output_port.can_push() {
                need_consume = true;
                continue;
            }

            if let Some(data_block) = self.output_data_blocks[index].pop_front() {
                output_port.push_data(Ok(data_block));
                need_consume = true;
            }
        }

        if all_output_finished {
            return self.finish();
        }

        if need_consume {
            return Ok(Event::NeedConsume);
        }

        if self.input_port.has_data() {
            let data_block = self.input_port.pull_data().unwrap()?;
            self.input_data_blocks.push_back(data_block);
            return Ok(Event::Sync);
        }

        if self.input_port.is_finished() {
            return self.finish();
        }

        self.input_port.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_data_blocks.pop_front() {
            let num_rows = data_block.num_rows();

            // Extract the columns used for hash computation.
            let hash_cols = self
                .hash_keys
                .iter()
                .map(|&offset| {
                    let entry = data_block.get_by_offset(offset);
                    match &entry.value {
                        Value::Scalar(s) => {
                            ColumnBuilder::repeat(&s.as_ref(), num_rows, &entry.data_type).build()
                        }
                        Value::Column(c) => c.clone(),
                    }
                })
                .collect::<Vec<_>>();

            // Compute the hash value for each row.
            let mut hashes = vec![0u64; num_rows];
            group_hash_columns_slice(&hash_cols, &mut hashes);

            // Scatter the data block to different partitions.
            let indices = hashes
                .iter()
                .map(|&hash| (hash % self.num_partitions as u64) as u8)
                .collect::<Vec<_>>();
            let scatter_blocks = DataBlock::scatter(&data_block, &indices, self.num_partitions)?;

            // Partition the data blocks to different processors.
            let mut output_data_blocks = vec![vec![]; self.num_processors];
            for (partition_id, data_block) in scatter_blocks.into_iter().enumerate() {
                output_data_blocks[partition_id % self.num_processors]
                    .push((partition_id, data_block));
            }

            // Union data blocks for each processor.
            for (partition_id, partitioned_data) in output_data_blocks.into_iter().enumerate() {
                let meta = WindowPartitionMeta::create(partitioned_data);
                let data_block = DataBlock::empty_with_meta(meta);
                self.output_data_blocks[partition_id].push_back(data_block);
            }
        }
        Ok(())
    }
}