// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::sql::TableReference;
use log::info;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::SystemTime;

use lakesoul_io::lakesoul_io_config::{LakeSoulIOConfig, LakeSoulIOConfigBuilder};
use lakesoul_metadata::{LakeSoulMetaDataError, MetaDataClientRef};
use proto::proto::entity::{CommitOp, DataCommitInfo, DataFileOp, FileOp, TableInfo, Uuid};
use crate::error::{LakeSoulError, Result};
use crate::lakesoul_table::helpers::create_io_config_builder_from_table_info;
use crate::serialize::arrow_java::ArrowJavaSchema;

pub mod lakesoul_catalog;
//  used in catalog_test, but still say unused_imports, I think it is a bug about rust-lint.
// this is a workaround
#[cfg(test)]
pub use lakesoul_catalog::*;
mod lakesoul_namespace;
pub use lakesoul_namespace::*;

fn deserialize_hash_bucket_num<'de, D>(deserializer: D) -> std::result::Result<Option<usize>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrNum {
        String(String),
        Number(usize),
    }

    let opt = Option::deserialize(deserializer)?;
    match opt {
        None => Ok(None),
        Some(StringOrNum::String(s)) => {
            s.parse::<usize>().map(Some).map_err(serde::de::Error::custom)
        }
        Some(StringOrNum::Number(n)) => Ok(Some(n)),
    }
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct LakeSoulTableProperty {
    #[serde(
        rename = "hashBucketNum",
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_hash_bucket_num"
    )]
    pub hash_bucket_num: Option<usize>,
    #[serde(rename = "datafusionProperties", skip_serializing_if = "Option::is_none")] 
    pub datafusion_properties: Option<HashMap<String, String>>,
    #[serde(rename = "partitions", default, skip_serializing_if = "Option::is_none")]
    pub partitions: Option<String>,
    #[serde(rename = "lakesoul_cdc_change_column", default, skip_serializing_if = "Option::is_none")]
    pub cdc_change_column: Option<String>,
    #[serde(rename = "use_cdc", default, skip_serializing_if = "Option::is_none")]
    pub use_cdc: Option<String>,
}

pub(crate) async fn create_table(client: MetaDataClientRef, table_name: &str, config: LakeSoulIOConfig) -> Result<()> {
    info!("create_table: {:?}", &table_name);
    client
        .create_table(TableInfo {
            table_id: format!("table_{}", uuid::Uuid::new_v4()),
            table_name: table_name.to_string(),
            table_path: format!(
                "file://{}/default/{}",
                env::current_dir()
                    .unwrap()
                    .to_str()
                    .ok_or(LakeSoulError::Internal("can not get $TMPDIR".to_string()))?,
                table_name
            ),
            table_schema: serde_json::to_string::<ArrowJavaSchema>(&config.target_schema().into())?,
            table_namespace: "default".to_string(),
            properties: serde_json::to_string(&LakeSoulTableProperty {
                hash_bucket_num: Some(4),
                ..Default::default()
            })?,
            partitions: format!(
                "{};{}",
                config
                    .range_partitions_slice()
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(","),
                config
                    .primary_keys_slice()
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            domain: "public".to_string(),
        })
        .await?;
    Ok(())
}

pub(crate) async fn create_io_config_builder(
    client: MetaDataClientRef,
    table_name: Option<&str>,
    fetch_files: bool,
    namespace: &str,
    options: HashMap<String, String>,
    object_store_options: HashMap<String, String>,
) -> Result<LakeSoulIOConfigBuilder> {
    if let Some(table_name) = table_name {
        let table_info = client.get_table_info_by_table_name(table_name, namespace).await?;
        if let Some(table_info) = table_info {
            let data_files = if fetch_files {
                client.get_data_files_by_table_name(table_name, namespace).await?
            } else {
                vec![]
            };
            create_io_config_builder_from_table_info(Arc::new(table_info), options, object_store_options).map(|builder| builder.with_files(data_files))
        } else {
            Err(LakeSoulError::MetaDataError(LakeSoulMetaDataError::NotFound(format!(
                "Table '{}' not found",
                table_name
            ))))
        }
    } else {
        Ok(LakeSoulIOConfigBuilder::new())
    }
}

pub(crate) fn parse_table_info_partitions(partitions: String) -> Result<(Vec<String>, Vec<String>)> {
    let (range_keys, hash_keys) = partitions.split_at(
        partitions
            .find(';')
            .ok_or(LakeSoulError::Internal("wrong partition format".to_string()))?,
    );
    let hash_keys = &hash_keys[1..];
    Ok((
        range_keys
            .split(',')
            .collect::<Vec<&str>>()
            .iter()
            .filter_map(|str| if str.is_empty() { None } else { Some(str.to_string()) })
            .collect::<Vec<String>>(),
        hash_keys
            .split(',')
            .collect::<Vec<&str>>()
            .iter()
            .filter_map(|str| if str.is_empty() { None } else { Some(str.to_string()) })
            .collect::<Vec<String>>(),
    ))
}

pub(crate) fn format_table_info_partitions(range_keys: &[String], hash_keys: &[String]) -> String {
    format!(
        "{};{}",
        range_keys.join(","),
        hash_keys.join(",")
    )
}


pub(crate) async fn commit_data(
    client: MetaDataClientRef,
    table_name: &str,
    partition_desc: String,
    files: &[String],
) -> Result<()> {
    let table_ref = TableReference::from(table_name);
    let table_name_id = client
        .get_table_name_id_by_table_name(table_ref.table(), table_ref.schema().unwrap_or("default"))
        .await?;
    client
        .commit_data_commit_info(DataCommitInfo {
            table_id: table_name_id.table_id,
            partition_desc,
            file_ops: files
                .iter()
                .map(|file| DataFileOp {
                    file_op: FileOp::Add as i32,
                    path: file.clone(),
                    ..Default::default()
                })
                .collect(),
            commit_op: CommitOp::AppendCommit as i32,
            timestamp: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_secs() as i64,
            commit_id: {
                let (high, low) = uuid::Uuid::new_v4().as_u64_pair();
                Some(Uuid { high, low })
            },
            committed: false,
            domain: "public".to_string(),
        })
        .await?;
    Ok(())
}
