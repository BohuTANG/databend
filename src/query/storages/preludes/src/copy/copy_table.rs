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

use std::any::Any;
use std::sync::Arc;

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_meta_app::schema::TableInfo;
use common_planner::extras::Extras;
use common_planner::extras::Statistics;
use common_planner::Partitions;

pub struct CopyTable {
    table_info: TableInfo,
}

impl CopyTable {
    pub fn try_create(table_info: TableInfo) -> Result<Box<dyn Table>> {
        Ok(Box::new(CopyTable { table_info }))
    }
}

#[async_trait::async_trait]
impl Table for CopyTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        todo!()
    }
}
