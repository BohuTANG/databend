// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataValue;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone, Debug, Default)]
pub struct Statistics {
    /// Total rows of the query read.
    pub read_rows: usize,
    /// Total bytes of the query read.
    pub read_bytes: usize,

    // Exact value.
    pub count :Option<DataValue>,
    pub min:Option<DataValue>,
    pub max:Option<DataValue>,

    /// Is the statistics exact.
    pub is_exact: bool,
}

impl Statistics {
    pub fn new_estimated(read_rows: usize, read_bytes: usize) -> Self {
        Statistics {
            read_rows,
            read_bytes,
            count:None,
            min:None,
            max:None,
            is_exact: false,
        }
    }

    pub fn set_exact_count(&mut self, count:DataValue) -> Self {
        self.is_exact = true;
        self.count = Some(count);
    }

    pub fn set_exact_min(&mut self, min:DataValue) -> Self {
        self.is_exact = true;
        self.min= Some(min);
    }

    pub fn set_exact_max(&mut self, max:DataValue) -> Self {
        self.is_exact = true;
        self.max= Some(max);
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }
}
