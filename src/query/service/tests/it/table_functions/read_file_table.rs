//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;

use databend_common_catalog::table_args::TableArgs;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_query::table_functions::ReadFileTable;
use pretty_assertions::assert_eq;

#[test]
fn test_read_file_table_creation() -> Result<()> {
    // Test positional arguments
    {
        let tbl_args = TableArgs::new_positioned(vec![
            Scalar::String("@data".to_string()),
            Scalar::String("csv/books.csv".to_string()),
        ]);
        let table = ReadFileTable::create("system", "read_file", 1, tbl_args)?;
        assert_eq!(table.function_name(), "read_file");
        
        // Verify schema has all expected columns
        let table_info = table.get_table_info();
        let schema = &table_info.meta.schema;
        assert_eq!(schema.fields().len(), 8);
        assert_eq!(schema.field(0).name(), "filename");
        assert_eq!(schema.field(1).name(), "content");
        assert_eq!(schema.field(2).name(), "size");
        assert_eq!(schema.field(3).name(), "last_modified");
        assert_eq!(schema.field(4).name(), "content_type");
        assert_eq!(schema.field(5).name(), "etag");
        assert_eq!(schema.field(6).name(), "stage");
        assert_eq!(schema.field(7).name(), "relative_path");
    }

    // Test named arguments  
    {
        let mut named_args = HashMap::new();
        named_args.insert("stage_name".to_string(), Scalar::String("data".to_string()));
        named_args.insert("relative_path".to_string(), Scalar::String("csv/books.csv".to_string()));
        let tbl_args = TableArgs::new_named(named_args);
        let table = ReadFileTable::create("system", "read_file", 1, tbl_args)?;
        assert_eq!(table.function_name(), "read_file");
    }

    Ok(())
}

#[test]
fn test_read_file_args_parsing() -> Result<()> {
    use databend_query::table_functions::read_file::table_args::ReadFileArgsParsed;

    // Test positional arguments
    {
        let tbl_args = TableArgs::new_positioned(vec![
            Scalar::String("@data".to_string()),
            Scalar::String("csv/test.csv".to_string()),
        ]);
        let args = ReadFileArgsParsed::parse(&tbl_args)?;
        assert_eq!(args.stage_name, "data");
        assert_eq!(args.relative_path, "csv/test.csv");
    }

    // Test named arguments
    {
        let mut named_args = HashMap::new();
        named_args.insert("stage_name".to_string(), Scalar::String("backup".to_string()));
        named_args.insert("relative_path".to_string(), Scalar::String("daily/2024.sql".to_string()));
        let tbl_args = TableArgs::new_named(named_args);
        let args = ReadFileArgsParsed::parse(&tbl_args)?;
        assert_eq!(args.stage_name, "backup");
        assert_eq!(args.relative_path, "daily/2024.sql");
    }

    // Test error: missing @ prefix
    {
        let tbl_args = TableArgs::new_positioned(vec![
            Scalar::String("data".to_string()), // Missing @
            Scalar::String("csv/test.csv".to_string()),
        ]);
        let result = ReadFileArgsParsed::parse(&tbl_args);
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("must be a stage name starting with @"));
    }

    // Test error: missing required arguments
    {
        let tbl_args = TableArgs::new_positioned(vec![Scalar::String("@data".to_string())]); // Missing second arg
        let result = ReadFileArgsParsed::parse(&tbl_args);
        assert!(result.is_err());
    }

    Ok(())
}