// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use atomic_refcell::AtomicRefCell;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::physical_plan::SendableRecordBatchStream;
use std::sync::Arc;

use arrow_schema::SchemaRef;

pub use datafusion::arrow::error::ArrowError;
pub use datafusion::arrow::error::Result as ArrowResult;
pub use datafusion::arrow::record_batch::RecordBatch;
pub use datafusion::error::{DataFusionError, Result};

use datafusion::prelude::SessionContext;

use futures::StreamExt;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::datasource::file_format::LakeSoulParquetFormat;
use crate::datasource::listing::LakeSoulTableProvider;
use crate::datasource::physical_plan::merge::convert_filter;
use crate::datasource::physical_plan::merge::prune_filter_and_execute;
use crate::lakesoul_io_config::{create_session_context, LakeSoulIOConfig};

pub struct LakeSoulReader {
    sess_ctx: SessionContext,
    config: LakeSoulIOConfig,
    stream: Option<SendableRecordBatchStream>,
    pub(crate) schema: Option<SchemaRef>,
}

impl LakeSoulReader {
    pub fn new(mut config: LakeSoulIOConfig) -> Result<Self> {
        let sess_ctx = create_session_context(&mut config)?;
        Ok(LakeSoulReader {
            sess_ctx,
            config,
            stream: None,
            schema: None,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        let target_schema: SchemaRef = self.config.target_schema.0.clone();
        if self.config.files.is_empty() {
            Err(DataFusionError::Internal(
                "LakeSoulReader has wrong number of file".to_string(),
            ))
        } else {
            let file_format = Arc::new(LakeSoulParquetFormat::new(
                Arc::new(ParquetFormat::new()),
                self.config.clone(),
            ));
            let source = LakeSoulTableProvider::new_with_config_and_format(
                &self.sess_ctx.state(),
                self.config.clone(),
                file_format,
                false,
            )
            .await?;

            let dataframe = self.sess_ctx.read_table(Arc::new(source))?;
            let filters = convert_filter(
                &dataframe,
                self.config.filter_strs.clone(),
                self.config.filter_protos.clone(),
            )?;
            let stream =
                prune_filter_and_execute(dataframe, target_schema.clone(), filters, self.config.batch_size).await?;
            self.schema = Some(stream.schema());
            self.stream = Some(stream);

            Ok(())
        }
    }

    pub async fn next_rb(&mut self) -> Option<Result<RecordBatch>> {
        if let Some(stream) = &mut self.stream {
            stream.next().await
        } else {
            None
        }
    }
}

// Reader will be used in async closure sent to tokio
// while accessing its mutable methods.
pub struct SyncSendableMutableLakeSoulReader {
    inner: Arc<AtomicRefCell<Mutex<LakeSoulReader>>>,
    runtime: Arc<Runtime>,
    schema: Option<SchemaRef>,
}

impl SyncSendableMutableLakeSoulReader {
    pub fn new(reader: LakeSoulReader, runtime: Runtime) -> Self {
        SyncSendableMutableLakeSoulReader {
            inner: Arc::new(AtomicRefCell::new(Mutex::new(reader))),
            runtime: Arc::new(runtime),
            schema: None,
        }
    }

    pub fn start_blocked(&mut self) -> Result<()> {
        let inner_reader = self.inner.clone();
        let runtime = self.get_runtime();
        runtime.block_on(async {
            let reader = inner_reader.borrow();
            let mut reader = reader.lock().await;
            reader.start().await?;
            self.schema = reader.schema.clone();
            Ok(())
        })
    }

    pub fn next_rb_callback(&self, f: Box<dyn FnOnce(Option<Result<RecordBatch>>) + Send + Sync>) -> JoinHandle<()> {
        let inner_reader = self.get_inner_reader();
        let runtime = self.get_runtime();
        runtime.spawn(async move {
            let reader = inner_reader.borrow();
            let mut reader = reader.lock().await;
            let rb = reader.next_rb().await;
            f(rb);
        })
    }

    pub fn next_rb_blocked(&self) -> Option<std::result::Result<RecordBatch, DataFusionError>> {
        let inner_reader = self.get_inner_reader();
        let runtime = self.get_runtime();
        runtime.block_on(async move {
            let reader = inner_reader.borrow();
            let mut reader = reader.lock().await;
            reader.next_rb().await
        })
    }

    pub fn get_schema(&self) -> Option<SchemaRef> {
        self.schema.clone()
    }

    fn get_runtime(&self) -> Arc<Runtime> {
        self.runtime.clone()
    }

    fn get_inner_reader(&self) -> Arc<AtomicRefCell<Mutex<LakeSoulReader>>> {
        self.inner.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::as_primitive_array;
    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use rand::prelude::*;
    use std::mem::ManuallyDrop;
    use std::ops::Not;
    use std::sync::mpsc::sync_channel;
    use std::time::Instant;
    use tokio::runtime::Builder;

    use arrow::datatypes::{DataType, Field, Schema, TimestampSecondType};
    use arrow::util::pretty::print_batches;

    use rand::{distributions::DistString, Rng};

    #[tokio::test]
    async fn test_reader_local() -> Result<()> {
        let project_dir = std::env::current_dir()?;
        let reader_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec![
                project_dir.join("../lakesoul-io-java/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet").into_os_string().into_string().unwrap()
                ])
            .with_thread_num(1)
            .with_batch_size(256)
            .build();
        let mut reader = LakeSoulReader::new(reader_conf)?;
        reader.start().await?;
        let mut row_cnt: usize = 0;

        while let Some(rb) = reader.next_rb().await {
            let num_rows = &rb.unwrap().num_rows();
            row_cnt += num_rows;
        }
        assert_eq!(row_cnt, 1000);
        Ok(())
    }

    #[test]
    fn test_reader_local_blocked() -> Result<()> {
        let project_dir = std::env::current_dir()?;
        let reader_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec![
                 project_dir.join("../lakesoul-io-java/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet").into_os_string().into_string().unwrap()
            ])
            .with_thread_num(2)
            .with_batch_size(11)
            .with_primary_keys(vec!["id".to_string()])
            .with_schema(Arc::new(Schema::new(vec![
                // Field::new("name", DataType::Utf8, true),
                Field::new("id", DataType::Int64, false),
                // Field::new("x", DataType::Float64, true),
                // Field::new("y", DataType::Float64, true),
            ])))
            .build();
        let reader = LakeSoulReader::new(reader_conf)?;
        let runtime = Builder::new_multi_thread()
            .worker_threads(reader.config.thread_num)
            .build()
            .unwrap();
        let mut reader = SyncSendableMutableLakeSoulReader::new(reader, runtime);
        reader.start_blocked()?;
        let mut rng = thread_rng();
        loop {
            let (tx, rx) = sync_channel(1);
            let start = Instant::now();
            let f = move |rb: Option<Result<RecordBatch>>| match rb {
                None => tx.send(true).unwrap(),
                Some(rb) => {
                    thread::sleep(Duration::from_millis(200));
                    let _ = print_batches(&[rb.as_ref().unwrap().clone()]);

                    println!("time cost: {:?} ms", start.elapsed().as_millis()); // ms
                    tx.send(false).unwrap();
                }
            };
            thread::sleep(Duration::from_millis(rng.gen_range(600..1200)));

            reader.next_rb_callback(Box::new(f));
            let done = rx.recv().unwrap();
            if done {
                break;
            }
        }
        Ok(())
    }

    #[test]
    fn test_reader_partition() -> Result<()> {
        let reader_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec!["/path/to/file.parquet".to_string()])
            .with_thread_num(1)
            .with_batch_size(8192)
            .build();
        let reader = LakeSoulReader::new(reader_conf)?;
        let runtime = Builder::new_multi_thread()
            .worker_threads(reader.config.thread_num)
            .build()
            .unwrap();
        let mut reader = SyncSendableMutableLakeSoulReader::new(reader, runtime);
        reader.start_blocked()?;
        static mut ROW_CNT: usize = 0;
        loop {
            let (tx, rx) = sync_channel(1);
            let f = move |rb: Option<Result<RecordBatch>>| match rb {
                None => tx.send(true).unwrap(),
                Some(rb) => {
                    let rb = rb.unwrap();
                    let num_rows = &rb.num_rows();
                    unsafe {
                        ROW_CNT += num_rows;
                        println!("{}", ROW_CNT);
                    }

                    tx.send(false).unwrap();
                }
            };
            reader.next_rb_callback(Box::new(f));
            let done = rx.recv().unwrap();
            if done {
                break;
            }
        }
        Ok(())
    }

    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_reader_s3() -> Result<()> {
        let reader_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec!["s3://path/to/file.parquet".to_string()])
            .with_thread_num(1)
            .with_batch_size(8192)
            .with_object_store_option(String::from("fs.s3a.access.key"), String::from("fs.s3.access.key"))
            .with_object_store_option(String::from("fs.s3a.secret.key"), String::from("fs.s3.secret.key"))
            .with_object_store_option(String::from("fs.s3a.region"), String::from("us-east-1"))
            .with_object_store_option(String::from("fs.s3a.bucket"), String::from("fs.s3.bucket"))
            .with_object_store_option(String::from("fs.s3a.endpoint"), String::from("fs.s3.endpoint"))
            .build();
        let reader = LakeSoulReader::new(reader_conf)?;
        let mut reader = ManuallyDrop::new(reader);
        reader.start().await?;
        static mut ROW_CNT: usize = 0;

        let start = Instant::now();
        while let Some(rb) = reader.next_rb().await {
            let num_rows = &rb.unwrap().num_rows();
            unsafe {
                ROW_CNT += num_rows;
                println!("{}", ROW_CNT);
            }
            sleep(Duration::from_millis(20)).await;
        }
        println!("time cost: {:?}ms", start.elapsed().as_millis()); // ms

        Ok(())
    }

    use std::thread;
    #[test]
    fn test_reader_s3_blocked() -> Result<()> {
        let reader_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec!["s3://path/to/file.parquet".to_string()])
            .with_thread_num(1)
            .with_batch_size(8192)
            .with_object_store_option(String::from("fs.s3a.access.key"), String::from("fs.s3.access.key"))
            .with_object_store_option(String::from("fs.s3a.secret.key"), String::from("fs.s3.secret.key"))
            .with_object_store_option(String::from("fs.s3a.region"), String::from("us-east-1"))
            .with_object_store_option(String::from("fs.s3a.bucket"), String::from("fs.s3.bucket"))
            .with_object_store_option(String::from("fs.s3a.endpoint"), String::from("fs.s3.endpoint"))
            .build();
        let reader = LakeSoulReader::new(reader_conf)?;
        let runtime = Builder::new_multi_thread()
            .worker_threads(reader.config.thread_num)
            .enable_all()
            .build()
            .unwrap();
        let mut reader = SyncSendableMutableLakeSoulReader::new(reader, runtime);
        reader.start_blocked()?;
        static mut ROW_CNT: usize = 0;
        let start = Instant::now();
        loop {
            let (tx, rx) = sync_channel(1);

            let f = move |rb: Option<Result<RecordBatch>>| match rb {
                None => tx.send(true).unwrap(),
                Some(rb) => {
                    let num_rows = &rb.unwrap().num_rows();
                    unsafe {
                        ROW_CNT += num_rows;
                        println!("{}", ROW_CNT);
                    }

                    thread::sleep(Duration::from_millis(20));
                    tx.send(false).unwrap();
                }
            };
            reader.next_rb_callback(Box::new(f));
            let done = rx.recv().unwrap();

            if done {
                println!("time cost: {:?} ms", start.elapsed().as_millis()); // ms
                break;
            }
        }
        Ok(())
    }

    use crate::lakesoul_io_config::LakeSoulIOConfigBuilder;
    use crate::lakesoul_writer::SyncSendableMutableLakeSoulWriter;
    use datafusion::logical_expr::{col, Expr};
    use datafusion_common::ScalarValue;

    async fn get_num_rows_of_file_with_filters(file_path: String, filters: Vec<Expr>) -> Result<usize> {
        let reader_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec![file_path])
            .with_thread_num(1)
            .with_batch_size(32)
            .with_filters(filters)
            .build();
        let reader = LakeSoulReader::new(reader_conf)?;
        let mut reader = ManuallyDrop::new(reader);
        reader.start().await?;
        let mut row_cnt: usize = 0;

        while let Some(rb) = reader.next_rb().await {
            row_cnt += &rb?.num_rows();
        }

        Ok(row_cnt)
    }

    #[tokio::test]
    async fn test_expr_eq_neq() -> Result<()> {
        let mut filters1: Vec<Expr> = vec![];
        let v = ScalarValue::Utf8(Some("Amanda".to_string()));
        let filter = col("first_name").eq(Expr::Literal(v));
        filters1.push(filter);
        let mut row_cnt1 = 0;
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters1).await;
        if let Ok(row_cnt) = result {
            row_cnt1 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        let mut filters2: Vec<Expr> = vec![];
        let v = ScalarValue::Utf8(Some("Amanda".to_string()));
        let filter = col("first_name").not_eq(Expr::Literal(v));
        filters2.push(filter);

        let mut row_cnt2 = 0;
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters2).await;
        if let Ok(row_cnt) = result {
            row_cnt2 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        assert_eq!(row_cnt1 + row_cnt2, 1000);

        Ok(())
    }

    #[tokio::test]
    async fn test_expr_lteq_gt() -> Result<()> {
        let mut filters1: Vec<Expr> = vec![];
        let v = ScalarValue::Float64(Some(139177.2));
        let filter = col("salary").lt_eq(Expr::Literal(v));
        filters1.push(filter);

        let mut row_cnt1 = 0;
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters1).await;
        if let Ok(row_cnt) = result {
            row_cnt1 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        let mut filters2: Vec<Expr> = vec![];
        let v = ScalarValue::Float64(Some(139177.2));
        let filter = col("salary").gt(Expr::Literal(v));
        filters2.push(filter);

        let mut row_cnt2 = 0;
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters2).await;
        if let Ok(row_cnt) = result {
            row_cnt2 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        let mut filters3: Vec<Expr> = vec![];
        let filter = col("salary").is_null();
        filters3.push(filter);

        let mut row_cnt3 = 0;
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters3).await;
        if let Ok(row_cnt) = result {
            row_cnt3 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        assert_eq!(row_cnt1 + row_cnt2, 1000 - row_cnt3);

        Ok(())
    }

    #[tokio::test]
    async fn test_expr_null_notnull() -> Result<()> {
        let mut filters1: Vec<Expr> = vec![];
        let filter = col("cc").is_null();
        filters1.push(filter);

        let mut row_cnt1 = 0;
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters1).await;
        if let Ok(row_cnt) = result {
            row_cnt1 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        let mut filters2: Vec<Expr> = vec![];
        let filter = col("cc").is_not_null();
        filters2.push(filter);

        let mut row_cnt2 = 0;
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters2).await;
        if let Ok(row_cnt) = result {
            row_cnt2 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        assert_eq!(row_cnt1 + row_cnt2, 1000);

        Ok(())
    }

    #[tokio::test]
    async fn test_expr_or_and() -> Result<()> {
        let mut filters1: Vec<Expr> = vec![];
        let first_name = ScalarValue::Utf8(Some("Amanda".to_string()));
        // let filter = col("first_name").eq(Expr::Literal(first_name)).and(col("last_name").eq(Expr::Literal(last_name)));
        let filter = col("first_name").eq(Expr::Literal(first_name));

        filters1.push(filter);
        let mut row_cnt1 = 0;
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters1).await;
        if let Ok(row_cnt) = result {
            row_cnt1 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }
        // println!("{}", row_cnt1);

        let mut filters2: Vec<Expr> = vec![];
        let last_name = ScalarValue::Utf8(Some("Jordan".to_string()));
        // let filter = col("first_name").eq(Expr::Literal(first_name)).and(col("last_name").eq(Expr::Literal(last_name)));
        let filter = col("last_name").eq(Expr::Literal(last_name));

        filters2.push(filter);
        let mut row_cnt2 = 0;
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters2).await;
        if let Ok(row_cnt) = result {
            row_cnt2 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        let mut filters: Vec<Expr> = vec![];
        let first_name = ScalarValue::Utf8(Some("Amanda".to_string()));
        let last_name = ScalarValue::Utf8(Some("Jordan".to_string()));
        let filter = col("first_name")
            .eq(Expr::Literal(first_name))
            .and(col("last_name").eq(Expr::Literal(last_name)));

        filters.push(filter);
        let mut row_cnt3 = 0;
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters).await;
        if let Ok(row_cnt) = result {
            row_cnt3 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        let mut filters: Vec<Expr> = vec![];
        let first_name = ScalarValue::Utf8(Some("Amanda".to_string()));
        let last_name = ScalarValue::Utf8(Some("Jordan".to_string()));
        let filter = col("first_name")
            .eq(Expr::Literal(first_name))
            .or(col("last_name").eq(Expr::Literal(last_name)));

        filters.push(filter);
        let mut row_cnt4 = 0;
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters).await;
        if let Ok(row_cnt) = result {
            row_cnt4 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        assert_eq!(row_cnt1 + row_cnt2 - row_cnt3, row_cnt4);

        Ok(())
    }

    #[tokio::test]
    async fn test_expr_not() -> Result<()> {
        let mut filters: Vec<Expr> = vec![];
        let filter = col("salary").is_null();
        filters.push(filter);
        let mut row_cnt1 = 0;
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters).await;
        if let Ok(row_cnt) = result {
            row_cnt1 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        let mut filters: Vec<Expr> = vec![];
        let filter = Expr::not(col("salary").is_null());
        filters.push(filter);
        let mut row_cnt2 = 0;
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters).await;
        if let Ok(row_cnt) = result {
            row_cnt2 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        assert_eq!(row_cnt1 + row_cnt2, 1000);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_with_partition_column() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Decimal128(10, 0), true),
        ]));
        let partition_schema = Arc::new(Schema::new(vec![Field::new("order_id", DataType::Int32, true)]));
        let reader_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec!["file:/var/folders/4c/34n9w2cd65n0pyjkc3n4q7pc0000gn/T/lakeSource/user_nonpk_partitioned/order_id=4/part-00011-989b7a5d-6ed7-4e51-a3bd-a9fa7853155d_00011.c000.parquet".to_string()])
            // .with_files(vec!["file:/var/folders/4c/34n9w2cd65n0pyjkc3n4q7pc0000gn/T/lakeSource/user1/order_id=4/part-59guLCg5R6v4oLUT_0000.parquet".to_string()])
            .with_thread_num(1)
            .with_batch_size(8192)
            .with_schema(schema)
            .with_partition_schema(partition_schema)
            .with_default_column_value("order_id".to_string(), "4".to_string())
            .set_inferring_schema(true)
            .build();
        let reader = LakeSoulReader::new(reader_conf)?;
        let mut reader = ManuallyDrop::new(reader);
        reader.start().await?;
        static mut ROW_CNT: usize = 0;

        let start = Instant::now();
        while let Some(rb) = reader.next_rb().await {
            let num_rows = &rb.unwrap().num_rows();
            unsafe {
                ROW_CNT += num_rows;
                println!("{}", ROW_CNT);
            }
            sleep(Duration::from_millis(20)).await;
        }
        println!("time cost: {:?}ms", start.elapsed().as_millis()); // ms

        Ok(())
    }

    #[tokio::test]
    async fn test_read_file() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("range", DataType::Int32, true),
            Field::new("datetimeSec", DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some(Arc::from("UTC"))), true),
        ]));
        let partition_schema = Arc::new(Schema::new(vec![Field::new("range", DataType::Int32, true)]));
        let reader_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec!["file:/private/tmp/test_local_java_table/range=0/part-AX31Bzzu4jGi23qY_0000.parquet".to_string()])
            // .with_files(vec!["file:/var/folders/4c/34n9w2cd65n0pyjkc3n4q7pc0000gn/T/lakeSource/user1/order_id=4/part-59guLCg5R6v4oLUT_0000.parquet".to_string()])
            .with_thread_num(1)
            .with_batch_size(8192)
            .with_schema(schema)
            .with_partition_schema(partition_schema)
            .with_default_column_value("range".to_string(), "0".to_string())
            // .set_inferring_schema(true)
            .build();
        let reader = LakeSoulReader::new(reader_conf)?;
        let mut reader = ManuallyDrop::new(reader);
        reader.start().await?;
        static mut ROW_CNT: usize = 0;

        let start = Instant::now();
        while let Some(rb) = reader.next_rb().await {
            let num_rows = &rb.unwrap().num_rows();
            unsafe {
                ROW_CNT += num_rows;
                println!("{}", ROW_CNT);
            }
            sleep(Duration::from_millis(20)).await;
        }
        println!("time cost: {:?}ms", start.elapsed().as_millis()); // ms

        Ok(())
    }

    #[tokio::test]
    async fn test_as_primitive_array_timestamp_second_type() -> Result<()> {
        use arrow_array::{Array, TimestampSecondArray};
        use std::sync::Arc;

        // 创建一个 TimestampSecondArray
        let data = vec![Some(1627846260), Some(1627846261), None, Some(1627846263)];
        let array = Arc::new(TimestampSecondArray::from(data)) as ArrayRef;

        // 调用 as_primitive_array::<TimestampSecondType>(&array)
        let primitive_array = as_primitive_array::<TimestampSecondType>(&array);

        // 验证结果
        assert_eq!(primitive_array.value(0), 1627846260);
        assert_eq!(primitive_array.value(1), 1627846261);
        assert!(primitive_array.is_null(2));
        assert_eq!(primitive_array.value(3), 1627846263);
        Ok(())
    }

    #[test]
    fn test_primary_key_generator() -> Result<()> {
        let mut generator = LinearPKGenerator::new(2, 3);
        assert_eq!(generator.next_pk(), 3); // x=0: 2*0 + 3
        assert_eq!(generator.next_pk(), 5); // x=1: 2*1 + 3
        assert_eq!(generator.next_pk(), 7); // x=2: 2*2 + 3
        Ok(())
    }

    struct LinearPKGenerator {
        a: i64,
        b: i64,
        current: i64,
    }

    impl LinearPKGenerator {
        fn new(a: i64, b: i64) -> Self {
            LinearPKGenerator { 
                a,
                b,
                current: 0
            }
        }

        fn next_pk(&mut self) -> i64 {
            let pk = self.a * self.current + self.b;
            self.current += 1;
            pk
        }
    }

    fn create_batch(num_columns: usize, num_rows: usize, str_len: usize, pk_generator:  &mut Option<LinearPKGenerator>) -> RecordBatch {
        let mut rng = rand::thread_rng();
        let mut len_rng = rand::thread_rng();
        let mut iter = vec![];
        if let Some(generator) = pk_generator {
            let pk_iter = (0..num_rows).map(|_| generator.next_pk());
            iter.push((
                "pk".to_string(),
                Arc::new(Int64Array::from_iter_values(pk_iter)) as ArrayRef,
                true,
            ));
        }
        for i in 0..num_columns {
            iter.push((
                format!("col_{}", i),
                Arc::new(StringArray::from(
                    (0..num_rows)
                        .into_iter()
                        .map(|_| {
                            rand::distributions::Alphanumeric
                                .sample_string(&mut rng, len_rng.gen_range(str_len..str_len * 3))
                        })
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                true,
            ));
        }
        RecordBatch::try_from_iter_with_nullable(iter).unwrap()
    }

    fn create_schema(num_columns: usize, with_pk: bool) -> Schema {
        let mut fields = vec![];
        if with_pk {
            fields.push(Field::new("pk", DataType::Int64, true));
        }
        for i in 0..num_columns {
            fields.push(Field::new(format!("col_{}", i), DataType::Utf8, true));
        }
        Schema::new(fields)
    }


    #[test]
    fn profiling_2ways_merge_on_read() -> Result<()> {
        let num_batch = 10;
        let num_rows = 1000;
        let num_columns = 100;
        let str_len = 4;
        let _temp_dir = tempfile::tempdir()?.into_path();
        let temp_dir = std::env::current_dir()?.join("temp_dir");
        let with_pk = true;
        let to_write_schema = create_schema(num_columns, with_pk);
        
        for i in 0..2 {

            let mut generator = if with_pk { Some(LinearPKGenerator::new(i + 2, 0)) } else { None } ;
            let to_write = create_batch(num_columns, num_rows, str_len, &mut generator);
            let path = temp_dir
                .clone()
                .join(format!("test{}.parquet", i))
                .into_os_string()
                .into_string()
                .unwrap();
            let writer_conf = LakeSoulIOConfigBuilder::new()
                .with_files(vec![path.clone()])
                // .with_prefix(tempfile::tempdir()?.into_path().into_os_string().into_string().unwrap())
                .with_thread_num(2)
                .with_batch_size(num_rows)
                // .with_max_row_group_size(2000)
                // .with_max_row_group_num_values(4_00_000)
                .with_schema(to_write.schema())
                .with_primary_keys(
                    vec!["pk".to_string()]
                )
                // .with_aux_sort_column("col2".to_string())
                // .with_option(OPTION_KEY_MEM_LIMIT, format!("{}", 1024 * 1024 * 48))
                // .set_dynamic_partition(true)
                .with_hash_bucket_num(4)
                // .with_max_file_size(1024 * 1024 * 32)
                .build();

            let mut writer = SyncSendableMutableLakeSoulWriter::try_new(
                writer_conf, 
                Builder::new_multi_thread().enable_all().build().unwrap()
            )?;

            let _start = Instant::now();
            for _ in 0..num_batch {
                let once_start = Instant::now();
                writer.write_batch(create_batch(num_columns, num_rows, str_len, &mut generator))?;
                println!("write batch once cost: {}", once_start.elapsed().as_millis());
            }
            let _flush_start = Instant::now();
            writer.flush_and_close()?;
        }

        let reader_conf = LakeSoulIOConfigBuilder::new()
            .with_files((0..2).map(|i| temp_dir.join(format!("test{}.parquet", i)).into_os_string().into_string().unwrap()).collect::<Vec<_>>())
            .with_thread_num(2)
            .with_batch_size(num_rows)
            .with_schema(Arc::new(to_write_schema))
            .with_primary_keys(vec!["pk".to_string()])
            .build();

        let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let lakesoul_reader = LakeSoulReader::new(reader_conf)?;
        let mut reader = SyncSendableMutableLakeSoulReader::new(lakesoul_reader, runtime);
        reader.start_blocked();
        let start = Instant::now();
        while let Some(rb) = reader.next_rb_blocked() {
            dbg!(&rb.unwrap().num_rows());
        }
        println!("time cost: {:?}ms", start.elapsed().as_millis()); // ms
        Ok(())
    }

}
