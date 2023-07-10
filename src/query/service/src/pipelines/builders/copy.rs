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

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::Utc;
use common_base::runtime::GlobalIORuntime;
use common_catalog::table::AppendMode;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::Scalar;
use common_meta_app::principal::StageInfo;
use common_meta_app::schema::TableCopiedFileInfo;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_pipeline_core::Pipeline;
use common_sql::executor::DistributedCopyIntoTable;
use common_sql::plans::CopyIntoTableMode;
use common_sql::plans::CopyIntoTablePlan;
use common_storage::StageFileInfo;
use common_storages_fuse::io::Files;
use common_storages_stage::StageTable;
use tracing::debug;
use tracing::error;
use tracing::info;

use crate::pipelines::builders::build_append2table_without_commit_pipeline;
use crate::pipelines::processors::transforms::TransformAddConstColumns;
use crate::pipelines::processors::TransformCastSchema;
use crate::sessions::QueryContext;

pub enum CopyPlanType {
    CopyIntoTablePlanOption(CopyIntoTablePlan),
    DistributedCopyIntoTable(DistributedCopyIntoTable),
}

pub fn build_append_data_pipeline(
    ctx: Arc<QueryContext>,
    main_pipeline: &mut Pipeline,
    plan: CopyPlanType,
    source_schema: Arc<DataSchema>,
    to_table: Arc<dyn Table>,
) -> Result<bool> {
    let plan_required_source_schema: DataSchemaRef;
    let plan_required_values_schema: DataSchemaRef;
    let plan_values_consts: Vec<Scalar>;
    let mut insert_overwrite_option: bool = false;
    let plan_write_mode: CopyIntoTableMode;

    match plan {
        CopyPlanType::CopyIntoTablePlanOption(plan) => {
            plan_required_source_schema = plan.required_source_schema;
            plan_required_values_schema = plan.required_values_schema;
            plan_values_consts = plan.values_consts;
            plan_write_mode = plan.write_mode;
        }
        CopyPlanType::DistributedCopyIntoTable(plan) => {
            plan_required_source_schema = plan.required_source_schema;
            plan_required_values_schema = plan.required_values_schema;
            plan_values_consts = plan.values_consts;
            plan_write_mode = plan.write_mode;
        }
    }

    if source_schema != plan_required_source_schema {
        // only parquet need cast
        let func_ctx = ctx.get_function_context()?;
        main_pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformCastSchema::try_create(
                transform_input_port,
                transform_output_port,
                source_schema.clone(),
                plan_required_source_schema.clone(),
                func_ctx.clone(),
            )
        })?;
    }

    if !plan_values_consts.is_empty() {
        fill_const_columns(
            ctx.clone(),
            main_pipeline,
            source_schema,
            plan_required_values_schema.clone(),
            plan_values_consts,
        )?;
    }

    // append data without commit.
    let write_mode = plan_write_mode;
    match write_mode {
        CopyIntoTableMode::Insert { overwrite } => {
            insert_overwrite_option = overwrite;
            build_append2table_without_commit_pipeline(
                ctx.clone(),
                main_pipeline,
                to_table.clone(),
                plan_required_values_schema,
                AppendMode::Copy,
            )?
        }
        CopyIntoTableMode::Replace => {}
        CopyIntoTableMode::Copy => build_append2table_without_commit_pipeline(
            ctx.clone(),
            main_pipeline,
            to_table.clone(),
            plan_required_values_schema,
            AppendMode::Copy,
        )?,
    }
    Ok(insert_overwrite_option)
}

#[allow(clippy::too_many_arguments)]
pub fn build_commit_data_pipeline(
    ctx: Arc<QueryContext>,
    main_pipeline: &mut Pipeline,
    stage_info: StageInfo,
    to_table: Arc<dyn Table>,
    files: Vec<StageFileInfo>,
    copy_force_option: bool,
    copy_purge_option: bool,
    insert_overwrite_option: bool,
) -> Result<()> {
    // Source node will do:
    // 1. commit
    // 2. purge
    // commit
    let copied_files_meta_req = build_upsert_copied_files_to_meta_req(
        ctx.clone(),
        to_table.clone(),
        stage_info.clone(),
        files.clone(),
        copy_force_option,
    )?;
    to_table.commit_insertion(
        ctx.clone(),
        main_pipeline,
        copied_files_meta_req,
        insert_overwrite_option,
    )?;

    // set on_finished callback.
    set_copy_on_finished(
        ctx,
        files,
        copy_purge_option,
        stage_info.clone(),
        main_pipeline,
    )?;
    Ok(())
}

pub fn set_copy_on_finished(
    ctx: Arc<QueryContext>,
    files: Vec<StageFileInfo>,
    copy_purge_option: bool,
    stage_info: StageInfo,
    main_pipeline: &mut Pipeline,
) -> Result<()> {
    // set on_finished callback.
    main_pipeline.set_on_finished(move |may_error| {
        match may_error {
            None => {
                GlobalIORuntime::instance().block_on(async move {
                    {
                        let status =
                            format!("end of commit, number of copied files:{}", files.len());
                        ctx.set_status_info(&status);
                        info!(status);
                    }

                    // 1. log on_error mode errors.
                    // todo(ariesdevil): persist errors with query_id
                    if let Some(error_map) = ctx.get_maximum_error_per_file() {
                        for (file_name, e) in error_map {
                            error!(
                                "copy(on_error={}): file {} encounter error {},",
                                stage_info.copy_options.on_error,
                                file_name,
                                e.to_string()
                            );
                        }
                    }

                    // 2. Try to purge copied files if purge option is true, if error will skip.
                    // If a file is already copied(status with AlreadyCopied) we will try to purge them.
                    if copy_purge_option {
                        try_purge_files(ctx.clone(), &stage_info, &files).await;
                    }

                    Ok(())
                })?;
            }
            Some(error) => {
                error!("copy failed, reason: {}", error);
            }
        }
        Ok(())
    });
    Ok(())
}

pub fn build_upsert_copied_files_to_meta_req(
    ctx: Arc<QueryContext>,
    to_table: Arc<dyn Table>,
    stage_info: StageInfo,
    copied_files: Vec<StageFileInfo>,
    force: bool,
) -> Result<Option<UpsertTableCopiedFileReq>> {
    let mut copied_file_tree = BTreeMap::new();
    for file in &copied_files {
        // Short the etag to 7 bytes for less space in metasrv.
        let short_etag = file.etag.clone().map(|mut v| {
            v.truncate(7);
            v
        });
        copied_file_tree.insert(file.path.clone(), TableCopiedFileInfo {
            etag: short_etag,
            content_length: file.size,
            last_modified: Some(file.last_modified),
        });
    }

    let expire_hours = ctx.get_settings().get_load_file_metadata_expire_hours()?;

    let upsert_copied_files_request = {
        if stage_info.copy_options.purge && force {
            // if `purge-after-copy` is enabled, and in `force` copy mode,
            // we do not need to upsert copied files into meta server
            info!(
                "[purge] and [force] are both enabled,  will not update copied-files set. ({})",
                &to_table.get_table_info().desc
            );
            None
        } else if copied_file_tree.is_empty() {
            None
        } else {
            debug!("upsert_copied_files_info: {:?}", copied_file_tree);
            let expire_at = expire_hours * 60 * 60 + Utc::now().timestamp() as u64;
            let req = UpsertTableCopiedFileReq {
                file_info: copied_file_tree,
                expire_at: Some(expire_at),
                fail_if_duplicated: !force,
            };
            Some(req)
        }
    };

    Ok(upsert_copied_files_request)
}

fn fill_const_columns(
    ctx: Arc<QueryContext>,
    pipeline: &mut Pipeline,
    input_schema: DataSchemaRef,
    output_schema: DataSchemaRef,
    const_values: Vec<Scalar>,
) -> Result<()> {
    pipeline.add_transform(|transform_input_port, transform_output_port| {
        TransformAddConstColumns::try_create(
            ctx.clone(),
            transform_input_port,
            transform_output_port,
            input_schema.clone(),
            output_schema.clone(),
            const_values.clone(),
        )
    })?;
    Ok(())
}

#[async_backtrace::framed]
async fn try_purge_files(
    ctx: Arc<QueryContext>,
    stage_info: &StageInfo,
    stage_files: &[StageFileInfo],
) {
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    let op = StageTable::get_op(stage_info);
    match op {
        Ok(op) => {
            let file_op = Files::create(table_ctx, op);
            let files = stage_files
                .iter()
                .map(|v| v.path.clone())
                .collect::<Vec<_>>();
            if let Err(e) = file_op.remove_file_in_batch(&files).await {
                error!("Failed to delete file: {:?}, error: {}", files, e);
            }
        }
        Err(e) => {
            error!("Failed to get stage table op, error: {}", e);
        }
    }
}
