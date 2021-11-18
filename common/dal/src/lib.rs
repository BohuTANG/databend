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

pub use context::DalContext;
pub use context::DalMetrics;
pub use data_accessor::read_obj;
pub use data_accessor::AsyncSeekableReader;
pub use data_accessor::Bytes;
pub use data_accessor::DataAccessor;
pub use data_accessor::DataAccessorBuilder;
pub use data_accessor::InputStream;
pub use data_accessor::SeekableReader;
pub use impls::aws_s3::S3InputStream;
pub use impls::aws_s3::S3;
pub use impls::azure_blob::AzureBlobAccessor;
pub use impls::azure_blob::AzureBlobInputStream;
pub use impls::local::Local;
pub use in_memory_data::InMemoryData;
pub use schemes::StorageScheme;

pub use self::metrics::DalWithMetric;
pub use self::metrics::InputStreamWithMetric;
pub use self::metrics::METRIC_DAL_READ_BYTES;
pub use self::metrics::METRIC_DAL_WRITE_BYTES;

mod context;
mod data_accessor;
mod impls;
mod in_memory_data;
mod metrics;
mod schemes;
