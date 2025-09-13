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

use databend_common_catalog::table_args::TableArgs;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_storages_fuse::table_functions::string_value;

#[derive(Clone, Debug)]
pub struct ReadFileArgsParsed {
    pub stage_name: String,
    pub relative_path: String,
}

impl ReadFileArgsParsed {
    pub fn parse(table_args: &TableArgs) -> Result<Self> {
        // Support both positional and named arguments
        if let Ok(positional_args) = table_args.expect_all_positioned("read_file", Some(2)) {
            // Positional: read_file(@stage, 'path')
            let stage_arg = string_value(&positional_args[0])?;
            let relative_path = string_value(&positional_args[1])?;
            
            let stage_name = if let Some(name) = stage_arg.strip_prefix('@') {
                name.to_string()
            } else {
                return Err(ErrorCode::BadArguments(format!(
                    "First argument must be a stage name starting with @, got: {}",
                    stage_arg
                )));
            };
            
            return Ok(Self {
                stage_name,
                relative_path,
            });
        }
        
        // Named arguments fallback: read_file(stage_name => 'stage', relative_path => 'path')
        let args = table_args.expect_all_named("read_file")?;

        let mut stage_name = None;
        let mut relative_path = None;

        for (k, v) in &args {
            match k.to_lowercase().as_str() {
                "stage_name" => {
                    stage_name = Some(string_value(v)?);
                }
                "relative_path" => {
                    relative_path = Some(string_value(v)?);
                }
                _ => {
                    return Err(ErrorCode::BadArguments(format!(
                        "unknown param {} for read_file, expected stage_name and relative_path",
                        k
                    )));
                }
            }
        }

        let stage_name = stage_name
            .ok_or_else(|| ErrorCode::BadArguments("read_file must specify stage_name"))?;
        let relative_path = relative_path
            .ok_or_else(|| ErrorCode::BadArguments("read_file must specify relative_path"))?;

        Ok(Self {
            stage_name,
            relative_path,
        })
    }
}