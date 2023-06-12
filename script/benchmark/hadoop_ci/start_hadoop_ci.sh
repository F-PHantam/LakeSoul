#!/bin/bash

# Change current path from LakeSoul/docker/lakesoul-docker-compose-env to Lakesoul/script/benchmark
function change_path_from_docker_compose_to_script_benchmark {
  cd ../../script/benchmark
}

# Start Flink CDC Job
function start_flink_cdc_job {
  docker exec -it $flink_container_name bash -c "flink run-application -t yarn-application -d -Dyarn.application.name=flink_mysql_cdc -c org.apache.flink.lakesoul.entry.MysqlCdc $flink_mysql_cdc_jar_path --source_db.host $mysql_host --source_db.port $mysql_port --source_db.db_name $mysql_db --source_db.user $mysql_user --source_db.password $mysql_password --warehouse_path $flink_mysql_cdc_warehouse --flink.checkpoint $flink_mysql_cdc_chk  --source.parallelism $flink_mysql_cdc_source_parallelism --sink.parallelism $flink_mysql_cdc_sink_parallelism --job.checkpoint_interval $flink_mysql_cdc_checkpoint_interval --server_time_zone $mysql_timezone"
}

# Start Flink Source to Sink Job
function start_flink_source_to_sink_job {
  docker exec -t $flink_cluster_name flink run -d -c org.apache.flink.lakesoul.test.benchmark.LakeSoulSourceToSinkTable -C file:///opt/flink/work-dir/$FLINK_JAR_NAME /opt/flink/work-dir/$FLINK_TEST_JAR_NAME --source.database.name $mysql_db --source.table.name default_init --sink.database.name $flink_source_to_sink_db --sink.table.name $flink_source_to_sink_table --use.cdc true --hash.bucket.number $flink_source_to_sink_hash_bucket_number --job.checkpoint_interval $flink_source_to_sink_checkpoint_interval --server_time_zone $mysql_timezone --warehouse.path $flink_source_to_sink_warehouse --flink.checkpoint $flink_source_to_sink_chk # Start Flink Source to Sink Job
}

# Start Flink Write table without primary key Job
function start_flink_write_table_without_primary_key_job {
  docker exec -t $flink_cluster_name flink run -d -c org.apache.flink.lakesoul.test.benchmark.LakeSoulDataGenSourceTable -C file:///opt/flink/work-dir/$FLINK_JAR_NAME /opt/flink/work-dir/$FLINK_TEST_JAR_NAME --sink.database.name $flink_datagen_sink_db --sink.table.name $flink_datagen_sink_table --job.checkpoint_interval $flink_datagen_sink_checkpoint_interval --server_time_zone $mysql_timezone --warehouse.path $flink_datagen_sink_warehouse --flink.checkpoint $flink_datagen_sink_chk --sink.parallel $flink_datagen_sink_parallelism --data.size $flink_datagen_sink_batch_data_row_num --write.time $flink_datagen_sink_batch_time # Start Flink Write table without primary keys Job
}

# Insert data into tables in mysql:test_cdc database
function insert_data_into_tables {
  echo "====== Inserting data into tables ======"
  docker exec -ti $mysql_container_name sh /2_insert_table_data.sh
  echo "====== Data has been inserted successfully! ======"
}

# Insert data into tables by pass db info
function insert_data_into_tables_pass_db_info {
  echo "====== Inserting data into tables ======"
  ./mysql_random_data_insert --no-progress -h $mysql_host-u $mysql_user -p$mysql_password --max-threads=10 $mysql_db default_init $mysql_batch_insert_row_num;
  echo "====== Data has been inserted successfully! ======"
  echo "====== Inserting data into tables ======"
  for ((i = 0; i < $mysql_table_num; i++)); do ./mysql_random_data_insert --no-progress -h $mysql_host -u $mysql_user -p$mysql_password --max-threads=10 $mysql_db random_table_$i $mysql_batch_insert_row_num; done
  echo "====== Data has been inserted successfully! ======"
}

# Sleep for a while to make sure the flink cdc task syncs mysql data to LakeSoul
function sleep_after_ddl_or_dml {
  echo "====== Flink cdc is synchronizing mysql data to lakesoul ======"
  sleep 3m
  echo "====== Flink cdc has synchronized mysql data to lakesoul ======"
}

# Verify data consistency between mysql and LakeSoul
function verify_mysql_cdc_data_consistency {
  echo "====== Verifying data consistency ======"
  docker run --cpus 4 -m 16000m --net lakesoul-docker-compose-env_default --rm -t -v ${PWD}/work-dir:/opt/spark/work-dir --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties bitnami/spark:3.3.1 spark-submit --driver-memory $spark_task_driver_memory --executor-memory $spark_task_executor_memory --conf spark.driver.memoryOverhead=$spark_task_driver_memoryOverhead --conf spark.executor.memoryOverhead=$spark_task_executor_memoryOverhead --jars /opt/spark/work-dir/$SPARK_JAR_NAME,/opt/spark/work-dir/mysql-connector-java-8.0.30.jar --class org.apache.spark.sql.lakesoul.benchmark.Benchmark --master local[4] /opt/spark/work-dir/$SPARK_TEST_JAR_NAME
  echo "====== Verifying has been completed! ======"
}

# Verify data consistency
function verify_mysql_cdc_data_consistency_pass_db_info {
  echo "====== Verifying data consistency ======"
  docker run --cpus 4 -m 16000m --net lakesoul-docker-compose-env_default --rm -t -v ${PWD}/work-dir:/opt/spark/work-dir --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties bitnami/spark:3.3.1 spark-submit --driver-memory 14G --executor-memory 14G --conf spark.driver.memoryOverhead=1500m --conf spark.executor.memoryOverhead=1500m --jars /opt/spark/work-dir/$SPARK_JAR_NAME,/opt/spark/work-dir/mysql-connector-java-8.0.30.jar --conf spark.hadoop.fs.s3.buffer.dir=/tmp --conf spark.hadoop.fs.s3a.buffer.dir=/tmp --conf spark.hadoop.fs.s3a.fast.upload.buffer=disk --conf spark.hadoop.fs.s3a.fast.upload=true --class org.apache.spark.sql.lakesoul.benchmark.Benchmark --master local[4] /opt/spark/work-dir/$SPARK_TEST_JAR_NAME --mysql.hostname $host --mysql.database.name $db --mysql.username $user --mysql.password $password --mysql.port $port --server.time.zone $timezone --cdc.contract true --single.table.contract false
  echo "====== Verifying has been completed! ======"
}

# Verify flink source to sink data consistency
function verify_flink_source_to_sink_data_consistency_pass_db_info {
  echo "====== Verifying data consistency ======"
  docker run --cpus 4 -m 16000m --net lakesoul-docker-compose-env_default --rm -t -v ${PWD}/work-dir:/opt/spark/work-dir --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties bitnami/spark:3.3.1 spark-submit --driver-memory 14G --executor-memory 14G --conf spark.driver.memoryOverhead=1500m --conf spark.executor.memoryOverhead=1500m --jars /opt/spark/work-dir/$SPARK_JAR_NAME,/opt/spark/work-dir/mysql-connector-java-8.0.30.jar --conf spark.hadoop.fs.s3.buffer.dir=/tmp --conf spark.hadoop.fs.s3a.buffer.dir=/tmp --conf spark.hadoop.fs.s3a.fast.upload.buffer=disk --conf spark.hadoop.fs.s3a.fast.upload=true --class org.apache.spark.sql.lakesoul.benchmark.Benchmark --master local[4] /opt/spark/work-dir/$SPARK_TEST_JAR_NAME --mysql.hostname $host --mysql.database.name $db --mysql.username $user --mysql.password $password --mysql.port $port --server.time.zone $timezone --cdc.contract false --single.table.contract true --lakesoul.database.name $flink_source_to_sink_db --lakesoul.table.name $flink_source_to_sink_table
  echo "====== Verifying has been completed! ======"
}

# Verify flink write table without primary keys data consistency
function verify_flink_write_table_without_primary_keys_data_consistency_pass_db_info {
  echo "====== Verifying data consistency ======"
  docker run --cpus 4 -m 16000m --net lakesoul-docker-compose-env_default --rm -t -v ${PWD}/work-dir:/opt/spark/work-dir --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties bitnami/spark:3.3.1 spark-submit --driver-memory 14G --executor-memory 14G --conf spark.driver.memoryOverhead=1500m --conf spark.executor.memoryOverhead=1500m --jars /opt/spark/work-dir/$SPARK_JAR_NAME,/opt/spark/work-dir/mysql-connector-java-8.0.30.jar --conf spark.hadoop.fs.s3.buffer.dir=/tmp --conf spark.hadoop.fs.s3a.buffer.dir=/tmp --conf spark.hadoop.fs.s3a.fast.upload.buffer=disk --conf spark.hadoop.fs.s3a.fast.upload=true --class org.apache.spark.sql.lakesoul.benchmark.FlinkWriteDataCheck --master local[4] /opt/spark/work-dir/$SPARK_TEST_JAR_NAME --csv.path $flink_datagen_sink_warehouse/csv --lakesoul.table.path $flink_datagen_sink_warehouse/$flink_datagen_sink_table --server.time.zone $timezone
  echo "====== Verifying has been completed! ======"
}

# Clean up metadata in lakesoul-meta-db
function clean_up_metadata {
  echo "====== Cleaning up metadata in lakesoul-meta-db ======"
  docker exec -ti lakesoul-docker-compose-env-lakesoul-meta-db-1 psql -h localhost -U lakesoul_test -d lakesoul_test -f /meta_cleanup.sql
  echo "====== Cleaning up metadata in lakesoul-meta-db has been completed! ======"
}

# Clean up data in minio
function clean_up_minio_data {
  echo "====== Cleaning up data in minio ======"
  docker run --net lakesoul-docker-compose-env_default --rm -t bitnami/spark:3.3.1 aws --no-sign-request --endpoint-url http://minio:9000 s3 rm --recursive s3://lakesoul-test-bucket/
  echo "====== Cleaning up data in minio has been completed! ======"
}

mysql_container_name=$(cat ./hadoop/hadoop_ci.conf | grep -v '^#' | grep mysql_container_name= | awk -F'=' '{print $2}')

mysql_table_num=$(cat ./properties | grep -v '^#' | grep table_num= | awk -F'=' '{print $2}')
mysql_host=$(cat ./properties | grep -v '^#' | grep host= | awk -F'=' '{print $2}')
mysql_port=$(cat ./properties | grep -v '^#' | grep port= | awk -F'=' '{print $2}')
mysql_db=$(cat ./properties | grep -v '^#' | grep db= | awk -F'=' '{print $2}')
mysql_user=$(cat ./properties | grep -v '^#' | grep user= | awk -F'=' '{print $2}')
mysql_password=$(cat ./properties | grep -v '^#' | grep password= | awk -F'=' '{print $2}')
mysql_timezone=$(cat ./properties | grep -v '^#' | grep timezone= | awk -F'=' '{print $2}')
mysql_batch_insert_row_num=$(cat ./properties | grep -v '^#' | grep row_num= | awk -F'=' '{print $2}')
mysql_batch_delete_num=$(cat ./properties | grep -v '^#' | grep delete_num= | awk -F'=' '{print $2}')

flink_container_name=$(cat ./hadoop/hadoop_ci.conf | grep -v '^#' | grep flink_container_name= | awk -F'=' '{print $2}')
flink_mysql_cdc_jobmanager_memory=$(cat ./hadoop/hadoop_ci.conf | grep -v '^#' | grep flink_mysql_cdc_jobmanager_memory= | awk -F'=' '{print $2}')
flink_mysql_cdc_taskmanager_memory=$(cat ./hadoop/hadoop_ci.conf | grep -v '^#' | grep flink_mysql_cdc_taskmanager_memory= | awk -F'=' '{print $2}')
flink_mysql_cdc_task_slot=$(cat ./hadoop/hadoop_ci.conf | grep -v '^#' | grep flink_mysql_cdc_task_slot= | awk -F'=' '{print $2}')
flink_mysql_cdc_application_name=$(cat ./hadoop/hadoop_ci.conf | grep -v '^#' | grep flink_mysql_cdc_application_name= | awk -F'=' '{print $2}')
flink_mysql_cdc_jar_path=$(cat ./hadoop/hadoop_ci.conf | grep -v '^#' | grep flink_mysql_cdc_jar_path= | awk -F'=' '{print $2}')

flink_mysql_cdc_source_parallelism=$(cat ./hadoop/hadoop_ci.conf | grep -v '^#' | grep flink_mysql_cdc_source_parallelism= | awk -F'=' '{print $2}')
flink_mysql_cdc_sink_parallelism=$(cat ./hadoop/hadoop_ci.conf | grep -v '^#' | grep flink_mysql_cdc_sink_parallelism= | awk -F'=' '{print $2}')
flink_mysql_cdc_warehouse=$(cat ./hadoop/hadoop_ci.conf | grep -v '^#' | grep flink_mysql_cdc_warehouse= | awk -F'=' '{print $2}')
flink_mysql_cdc_chk=$(cat ./hadoop/hadoop_ci.conf | grep -v '^#' | grep flink_mysql_cdc_chk= | awk -F'=' '{print $2}')
flink_mysql_cdc_checkpoint_interval=$(cat ./hadoop/hadoop_ci.conf | grep -v '^#' | grep flink_mysql_cdc_checkpoint_interval= | awk -F'=' '{print $2}')

flink_source_to_sink_db=$(cat ./properties | grep -v '^#' | grep flink_source_to_sink_db= | awk -F'=' '{print $2}')
flink_source_to_sink_table=$(cat ./properties | grep -v '^#' | grep flink_source_to_sink_table= | awk -F'=' '{print $2}')
flink_source_to_sink_hash_bucket_number=$(cat ./properties | grep -v '^#' | grep flink_source_to_sink_hash_bucket_number= | awk -F'=' '{print $2}')
flink_source_to_sink_warehouse=$(cat ./properties | grep -v '^#' | grep flink_source_to_sink_warehouse= | awk -F'=' '{print $2}')
flink_source_to_sink_chk=$(cat ./properties | grep -v '^#' | grep flink_source_to_sink_chk= | awk -F'=' '{print $2}')
flink_source_to_sink_checkpoint_interval=$(cat ./properties | grep -v '^#' | grep flink_source_to_sink_checkpoint_interval= | awk -F'=' '{print $2}')

flink_datagen_sink_db=$(cat ./properties | grep -v '^#' | grep flink_datagen_sink_db= | awk -F'=' '{print $2}')
flink_datagen_sink_table=$(cat ./properties | grep -v '^#' | grep flink_datagen_sink_table= | awk -F'=' '{print $2}')
flink_datagen_sink_parallelism=$(cat ./properties | grep -v '^#' | grep flink_datagen_sink_parallelism= | awk -F'=' '{print $2}')
flink_datagen_sink_warehouse=$(cat ./properties | grep -v '^#' | grep flink_datagen_sink_warehouse= | awk -F'=' '{print $2}')
flink_datagen_sink_chk=$(cat ./properties | grep -v '^#' | grep flink_datagen_sink_chk= | awk -F'=' '{print $2}')
flink_datagen_sink_checkpoint_interval=$(cat ./properties | grep -v '^#' | grep flink_datagen_sink_checkpoint_interval= | awk -F'=' '{print $2}')
flink_datagen_sink_batch_data_row_num=$(cat ./properties | grep -v '^#' | grep flink_datagen_sink_batch_data_row_num= | awk -F'=' '{print $2}')
flink_datagen_sink_batch_time=$(cat ./properties | grep -v '^#' | grep flink_datagen_sink_batch_time= | awk -F'=' '{print $2}')



FLINK_JAR_NAME=$(python3 ../get_jar_name.py ../../lakesoul-flink)
FLINK_TEST_JAR_NAME=$(python3 ../get_jar_name.py ../../lakesoul-flink | sed -e 's/.jar/-tests.jar/g')
SPARK_JAR_NAME=$(python3 ../get_jar_name.py ../../lakesoul-spark)
SPARK_TEST_JAR_NAME=$(python3 ../get_jar_name.py ../../lakesoul-spark | sed -e 's/.jar/-tests.jar/g')


cd ./work-dir


if [ ! -e mysql-connector-java-8.0.30.jar ]; then
  wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar
fi

cd ..

echo "====== Starting flink cdc job ======"
start_flink_cdc_job
sleep 30s

echo "====== Creating tables in mysql:test_cdc database ======"
python3 1_create_table.py # Create tables in mysql:test_cdc database
echo "====== Tables have been created! ======"

if [ !$IS_ON_K8S ]; then
  #docker exec -ti lakesoul-docker-compose-env-mysql-1 chmod 755 /2_insert_table_data.sh # Modify permissions for 2_insert_table_data.sh
  insert_data_into_tables
  sleep_after_ddl_or_dml
  verify_mysql_cdc_data_consistency
#  verify_flink_source_to_sink_data_consistency_pass_db_info
else
  insert_data_into_tables_pass_db_info
  sleep_after_ddl_or_dml
  verify_mysql_cdc_data_consistency_pass_db_info
#  verify_flink_source_to_sink_data_consistency_pass_db_info
fi

sleep 5s
echo "====== Adding columns for tables and deleting some data from tables ======"
python3 3_add_column.py # Add columns for tables in mysql:test_cdc database
python3 delete_data.py
echo "====== Adding columns and deleting some data have been completed! ======"

if [ !$IS_ON_K8S ]; then
  insert_data_into_tables
  sleep_after_ddl_or_dml
  verify_mysql_cdc_data_consistency
else
  insert_data_into_tables_pass_db_info
  sleep_after_ddl_or_dml
  verify_mysql_cdc_data_consistency_pass_db_info
fi

sleep 5s
echo "====== Updating data in tables ======"
python3 4_update_data.py # Update data in mysql:test_cdc tables
echo "====== Data in tables has been updated! ======"
sleep_after_ddl_or_dml

if [ !$IS_ON_K8S ]; then
  verify_mysql_cdc_data_consistency
#  verify_flink_source_to_sink_data_consistency_pass_db_info
else
  verify_mysql_cdc_data_consistency_pass_db_info
#  verify_flink_source_to_sink_data_consistency_pass_db_info
fi

sleep 5s
echo "====== Dropping columns and deleting some data in tables ======"
python3 6_drop_column.py # Drop columns in mysql:test_cdc tables
python3 delete_data.py
echo "====== Dropping columns and deleting some data have been completed! ======"

if [ !$IS_ON_K8S ]; then
  insert_data_into_tables
  sleep_after_ddl_or_dml
  verify_mysql_cdc_data_consistency
#  verify_flink_source_to_sink_data_consistency_pass_db_info
#  verify_flink_write_table_without_primary_keys_data_consistency_pass_db_info
else
  insert_data_into_tables_pass_db_info
  sleep_after_ddl_or_dml
  verify_mysql_cdc_data_consistency_pass_db_info
#  verify_flink_source_to_sink_data_consistency_pass_db_info
#  verify_flink_write_table_without_primary_keys_data_consistency_pass_db_info
fi

sleep 5s

if [ !$IS_ON_K8S ]; then
  echo "====== Dropping tables ======"
  python3 drop_table.py
  echo "====== Dropping tables has been completed! ======"
  clean_up_metadata
  clean_up_minio_data
  change_dir_from_script_benchmark_to_docker_compose
  echo "====== Stopping docker compose services ======"
  docker compose down # Use docker-compose to stop services
  echo "====== Docker compose services have been stopped! ======"
  echo "====== Congratulations! You have successfully tested LakeSoul for correctness! ======"
fi
