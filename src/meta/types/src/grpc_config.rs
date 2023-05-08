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

/// Grpc default configuration.
pub struct GrpcConfig {}

impl GrpcConfig {
    /// The maximum message size the client or server can **send**.
    pub const MAX_ENCODING_SIZE: usize = 16 * 1024 * 1024;

    /// The maximum message size the client or server can **receive**.
    pub const MAX_DECODING_SIZE: usize = 16 * 1024 * 1024;
}