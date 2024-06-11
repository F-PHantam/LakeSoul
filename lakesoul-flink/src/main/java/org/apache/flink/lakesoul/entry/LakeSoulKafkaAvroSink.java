package org.apache.flink.lakesoul.entry;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.*;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.lakesoul.tool.JobOptions.*;
import static org.apache.flink.lakesoul.tool.LakeSoulKafkaSinkOptions.*;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SERVER_TIME_ZONE;

public class LakeSoulKafkaAvroSink {
    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        String kafkaServers = parameter.get(BOOTSTRAP_SERVERS.key());
        String kafkaTopic = parameter.get(TOPIC.key());
        String topicGroupID = parameter.get(GROUP_ID.key());

        int sourceParallelism = parameter.getInt(SOURCE_PARALLELISM.key(), 1);
        int sinkParallelism = parameter.getInt(BUCKET_PARALLELISM.key(), 1);
        int checkpointInterval = parameter.getInt(JOB_CHECKPOINT_INTERVAL.key(), JOB_CHECKPOINT_INTERVAL.defaultValue());
        String serverTimezone = parameter.get(SERVER_TIME_ZONE.key(), SERVER_TIME_ZONE.defaultValue());
        String schemaRegistryUrl = parameter.get(SCHEMA_REGISTRY_URL.key(), SCHEMA_REGISTRY_URL.defaultValue());
        boolean regularTopicName = parameter.getBoolean(REGULAR_TOPIC_NAME.key(), REGULAR_TOPIC_NAME.defaultValue());
        int maxPollRecords = parameter.getInt(MAX_POLL_RECORDS.key(), MAX_POLL_RECORDS.defaultValue());

        //about security
        String securityProtocol = parameter.get(SECURITY_PROTOCOL.key());
        String saslMechanism = parameter.get(SASL_MECHANISM.key());
        String saslJaasConfig = parameter.get(SASL_JAAS_CONFIG.key());
        String sslTrustStoreLocation = parameter.get(SSL_TRUSTSTORE_LOCATION.key());
        String sslTrustStorePasswd = parameter.get(SSL_TRUSTSTORE_PASSWORD.key());
        String sslKeyStoreLocation = parameter.get(SSL_KEYSTORE_LOCATION.key());
        String sslKeyStorePasswd = parameter.get(SSL_KEYSTORE_PASSWORD.key());

        Properties pro = new Properties();
        pro.put("bootstrap.servers", kafkaServers);
        pro.put("group.id", topicGroupID);
        pro.put("max.poll.records", maxPollRecords);
        pro.put("retries", 3);

        OffsetsInitializer offSet;
        if (securityProtocol != null) {
            pro.put("security.protocol", securityProtocol);
            if (securityProtocol.equals("SASL_PLAINTEXT")) {
                pro.put("sasl.mechanism", saslMechanism);
                pro.put("sasl.jaas.config", saslJaasConfig);
            } else if (securityProtocol.equals("SSL")) {
                // SSL configurations
                // Configure the path of truststore (CA) provided by the server
                pro.put("ssl.truststore.location", sslTrustStoreLocation);
                pro.put("ssl.truststore.password", sslTrustStorePasswd);
                // Configure the path of keystore (private key) if client authentication is required
                pro.put("ssl.keystore.location", sslKeyStoreLocation);
                pro.put("ssl.keystore.password", sslKeyStorePasswd);
                pro.put("ssl.endpoint.identification.algorithm", "");
            } else if (securityProtocol.equals("SASL_SSL")) {
                // SSL configurations
                // Configure the path of truststore (CA) provided by the server
                pro.put("ssl.truststore.location", sslTrustStoreLocation);
                pro.put("ssl.truststore.password", sslTrustStorePasswd);
                // Configure the path of keystore (private key) if client authentication is required
                pro.put("ssl.keystore.location", sslKeyStoreLocation);
                pro.put("ssl.keystore.password", sslKeyStorePasswd);
                // SASL configurations
                // Set SASL mechanism as SCRAM-SHA-256
                pro.put("sasl.mechanism", saslMechanism);
                // Set JAAS configurations
                pro.put("sasl.jaas.config", saslJaasConfig);
                pro.put("ssl.endpoint.identification.algorithm", "");
            }
        }

        Configuration conf = new Configuration();
        // parameters for mutil tables dml sink
        conf.set(LakeSoulSinkOptions.USE_CDC, true);
        conf.set(LakeSoulSinkOptions.SOURCE_PARALLELISM, sourceParallelism);
        conf.set(LakeSoulSinkOptions.BUCKET_PARALLELISM, sinkParallelism);
        conf.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.getConfig().registerTypeWithKryoSerializer(BinarySourceRecord.class, BinarySourceRecordSerializer.class);
        ParameterTool pt = ParameterTool.fromMap(conf.toMap());
        env.getConfig().setGlobalJobParameters(pt);
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4023);

        CheckpointingMode checkpointingMode = CheckpointingMode.EXACTLY_ONCE;
        if (parameter.get(JOB_CHECKPOINT_MODE.key(), JOB_CHECKPOINT_MODE.defaultValue()).equals("AT_LEAST_ONCE")) {
            checkpointingMode = CheckpointingMode.AT_LEAST_ONCE;
        }
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.getCheckpointConfig().setCheckpointingMode(checkpointingMode);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.getCheckpointConfig().setCheckpointStorage(parameter.get(FLINK_CHECKPOINT.key()));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, // max failures per interval
                Time.of(10, TimeUnit.MINUTES), //time interval for measuring failure rate
                Time.of(20, TimeUnit.SECONDS) // delay
        ));

        DataStreamSource<LakeSoulArrowWrapper> source = env.fromSource(
                LakeSoulArrowSource.create(
                        "default",
                        MockLakeSoulArrowSource.MockSourceFunction.tableName,
                        conf
                ),
                WatermarkStrategy.noWatermarks(),
                "LakeSoul Arrow Source"
        );

        LakeSoulDynamicKafkaRecordSerializationSchema lakeSoulDynamicKafkaRecordSerializationSchema =
                LakeSoulDynamicKafkaRecordSerializationSchema.builder()
                        .setTopic(kafkaTopic)
                        .setSchemaRegistryUrl(schemaRegistryUrl)
                        .setKafkaConfigs(Maps.fromProperties(pro))
                        .setLakeDBAndTable("", "")
                        .hsaMetaData(false)
                        .setMetadataPositions(null)
                        .isUpsertMode(false)
                        .build();

        KafkaSink<SinkKafkaRecord> kafkaSink = KafkaSink.<SinkKafkaRecord>builder()
                .setBootstrapServers(kafkaServers)
                .setRecordSerializer(lakeSoulDynamicKafkaRecordSerializationSchema)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        source.flatMap(new FlatMapFunction<LakeSoulArrowWrapper, SinkKafkaRecord>() {
            @Override
            public void flatMap(LakeSoulArrowWrapper lakeSoulArrowWrapper, Collector<SinkKafkaRecord> collector) throws Exception {
                TableSchemaIdentity tableSchemaIdentity = lakeSoulArrowWrapper.generateTableSchemaIdentity();
                RowType rowType = tableSchemaIdentity.rowType;
                lakeSoulArrowWrapper.withDecoded(ArrowUtils.getRootAllocator(), (tableInfo, recordBatch) -> {
                    ArrowReader arrowReader = ArrowUtils.createArrowReader(recordBatch, rowType);
                    int i = 0;
                    while (i < recordBatch.getRowCount()) {
                        RowData rowData = arrowReader.read(i);
                        SinkKafkaRecord sinkKafkaRecord = new SinkKafkaRecord(tableSchemaIdentity, rowData, rowType);
                        collector.collect(sinkKafkaRecord);
                        i++;
                    }
                });
            }
        }).sinkTo(kafkaSink);
        env.execute("LakeSoul CDC Sink From Kafka topic " + kafkaTopic);
    }
}
