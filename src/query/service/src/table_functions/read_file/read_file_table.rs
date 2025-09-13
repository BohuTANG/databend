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
use std::sync::Arc;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_sql::binder::resolve_stage_location;
use databend_common_storages_stage::StageTable;
use databend_common_users::Object;

use crate::table_functions::read_file::table_args::ReadFileArgsParsed;

const READ_FILE: &str = "read_file";

pub struct ReadFileTable {
    args_parsed: ReadFileArgsParsed,
    table_args: TableArgs,
    table_info: TableInfo,
}

impl ReadFileTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args_parsed = ReadFileArgsParsed::parse(&table_args)?;
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: Self::schema(),
                engine: READ_FILE.to_owned(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(Self {
            table_info,
            args_parsed,
            table_args,
        }))
    }

    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("filename", TableDataType::String),
            TableField::new("content", TableDataType::Binary),
            TableField::new("size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("last_modified", TableDataType::String),
            TableField::new(
                "content_type", 
                TableDataType::Nullable(Box::new(TableDataType::String))
            ),
            TableField::new(
                "etag", 
                TableDataType::Nullable(Box::new(TableDataType::String))
            ),
            TableField::new("stage", TableDataType::String),
            TableField::new("relative_path", TableDataType::String),
        ])
    }
}

#[async_trait::async_trait]
impl Table for ReadFileTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::default(), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(self.table_args.clone())
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        pipeline.add_source(
            |output| ReadFileSource::create(ctx.clone(), output, self.args_parsed.clone()),
            1,
        )?;
        Ok(())
    }
}

impl TableFunction for ReadFileTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

struct ReadFileSource {
    ctx: Arc<dyn TableContext>,
    args_parsed: ReadFileArgsParsed,
    finished: bool,
}

impl ReadFileSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        args_parsed: ReadFileArgsParsed,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, ReadFileSource {
            ctx,
            args_parsed,
            finished: false,
        })
    }

    async fn read_single_file(&self) -> Result<DataBlock> {
        let stage_location = format!("@{}/{}", self.args_parsed.stage_name, self.args_parsed.relative_path);
        let (stage_info, file_path) =
            resolve_stage_location(self.ctx.as_ref(), &stage_location).await?;
        
        let enable_experimental_rbac_check = self
            .ctx
            .get_settings()
            .get_enable_experimental_rbac_check()?;
        if enable_experimental_rbac_check {
            let visibility_checker = self
                .ctx
                .get_visibility_checker(false, Object::Stage)
                .await?;
            if !(stage_info.is_temporary
                || visibility_checker.check_stage_read_visibility(&stage_info.stage_name)
                || stage_info.stage_type == StageType::User
                    && stage_info.stage_name == self.ctx.get_current_user()?.name)
            {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied: privilege READ is required on stage {} for user {}",
                    stage_info.stage_name.clone(),
                    &self.ctx.get_current_user()?.identity().display(),
                )));
            }
        }
        
        let op = StageTable::get_op(&stage_info)?;

        let metadata = op.stat(&file_path).await?;
        let content = op.read(&file_path).await?;
        
        let filename = file_path.clone();
        let size = metadata.content_length();
        let last_modified = metadata
            .last_modified()
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S.%3f %z").to_string())
            .unwrap_or_default();
        
        // Extract content type from metadata
        let content_type = metadata.content_type().map(|ct| ct.to_string());
        
        // Extract etag from metadata  
        let etag = metadata.etag().map(|e| e.to_string());
        
        // Use the parsed stage name and relative path
        let stage_name = self.args_parsed.stage_name.clone();
        let relative_path = self.args_parsed.relative_path.clone();

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(vec![filename]),
            BinaryType::from_data(vec![content.to_vec()]),
            UInt64Type::from_data(vec![size]),
            StringType::from_data(vec![last_modified]),
            StringType::from_opt_data(vec![content_type]),
            StringType::from_opt_data(vec![etag]),
            StringType::from_data(vec![stage_name]),
            StringType::from_data(vec![relative_path]),
        ]))
    }
}

#[async_trait::async_trait]
impl AsyncSource for ReadFileSource {
    const NAME: &'static str = READ_FILE;

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }
        
        self.finished = true;
        let block = self.read_single_file().await?;
        Ok(Some(block))
    }
}