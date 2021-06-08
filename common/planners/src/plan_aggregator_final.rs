// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchemaRef;

use crate::Expression;
use crate::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct AggregatorFinalPlan {
    pub aggr_expr: Vec<Expression>,
    pub group_expr: Vec<Expression>,
    pub schema: DataSchemaRef,
    pub schema_before_groupby: DataSchemaRef,
    pub input: Arc<PlanNode>,
}

impl AggregatorFinalPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }

    pub fn set_input(&mut self, node: &PlanNode) {
        self.input = Arc::new(node.clone());
    }
}
