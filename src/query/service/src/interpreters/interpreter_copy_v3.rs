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

use std::sync::Arc;

use common_exception::Result;
use common_sql::executor::PhysicalPlanBuilder;
use common_sql::plans::CopyPlanV2;
use common_sql::Metadata;
use parking_lot::RwLock;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct CopyInterpreterV3 {
    ctx: Arc<QueryContext>,
    plan: CopyPlanV2,
}

impl CopyInterpreterV3 {
    /// Create a CopyInterpreterV3 with context and [`CopyPlanV2`].
    pub fn try_create(ctx: Arc<QueryContext>, plan: CopyPlanV2) -> Result<Self> {
        Ok(CopyInterpreterV3 { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for InsertInterpreterV3 {
    fn name(&self) -> &str {
        "CopyInterpreterV3"
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let metadata = Arc::new(RwLock::new(Metadata::default()));
        let builder = PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone());
        // let plan = builder.build(s_expr).await?;

        // let pipeline_builder = PipelineBuilder::create(self.ctx.clone());
        // let build_res = pipeline_builder.finalize(&plan)?;
    }
}
