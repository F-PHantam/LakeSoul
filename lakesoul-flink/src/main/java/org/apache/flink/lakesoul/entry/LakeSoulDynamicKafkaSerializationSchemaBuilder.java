package org.apache.flink.lakesoul.entry;

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.types.LakSoulKafkaPartitioner;
import org.apache.flink.lakesoul.types.LakeSoulDynamicKafkaRecordSerializationSchema;
import org.apache.flink.lakesoul.types.SinkKafkaRecord;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class LakeSoulDynamicKafkaSerializationSchemaBuilder {
    private String topic;
    private String schemaRegistryUrl;
    private Map<String, ?> kafkaConfigs;
    private FlinkKafkaPartitioner<SinkKafkaRecord> partitioner;
    private SerializationSchema<RowData> keySerialization;
    private RowData.FieldGetter[] keyFieldGetters;
    private boolean hasMetadata = false;
    private int[] metadataPositions;
    private boolean upsertMode = true;

    private final DBManager lakesoulDBManager = new DBManager();

    public LakeSoulDynamicKafkaSerializationSchemaBuilder setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public LakeSoulDynamicKafkaSerializationSchemaBuilder setSchemaRegistryUrl(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        return this;
    }

    public LakeSoulDynamicKafkaSerializationSchemaBuilder setKafkaConfigs(Map<String, ?> kafkaConfigs) {
        this.kafkaConfigs = kafkaConfigs;
        return this;
    }

    public LakeSoulDynamicKafkaSerializationSchemaBuilder setLakeDBAndTable(String dbName, String table) {
        TableInfo tableInfo = lakesoulDBManager.getTableInfoByNameAndNamespace(table, dbName);
        String tableSchema = tableInfo.getTableSchema();
        DBUtil.TablePartitionKeys partitionKeys = DBUtil.parseTableInfoPartitions(tableInfo.getPartitions());
        List<String> primaryKeys = partitionKeys.primaryKeys;

        RowType keyRowType;
        try {
            keyRowType = ArrowUtils.tablePrimaryArrowSchema(Schema.fromJSON(tableSchema), primaryKeys);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (primaryKeys.size() > 0) {

            this.keyFieldGetters =
                    IntStream.range(0, keyRowType.getFieldCount())
                            .mapToObj(
                                    i ->
                                            RowData.createFieldGetter(
                                                    keyRowType.getTypeAt(i), i))
                            .toArray(RowData.FieldGetter[]::new);

            ConfluentRegistryAvroSerializationSchema<GenericRecord> genericRecordConfluentRegistryAvroSerializationSchema =
                    ConfluentRegistryAvroSerializationSchema.forGeneric(
                            String.format("%s-key", topic),
                            AvroSchemaConverter.convertToSchema(keyRowType),
                            this.schemaRegistryUrl,
                            this.kafkaConfigs);
            AvroRowDataSerializationSchema avroRowDataSerializationSchema = new AvroRowDataSerializationSchema(
                    keyRowType,
                    genericRecordConfluentRegistryAvroSerializationSchema,
                    RowDataToAvroConverters.createConverter(keyRowType));
            this.keySerialization = avroRowDataSerializationSchema;
        } else {
            this.keyFieldGetters = null;
            this.keySerialization = null;
        }
        return this;
    }

    public LakeSoulDynamicKafkaSerializationSchemaBuilder hsaMetaData(boolean hasMetadata) {
        this.hasMetadata = hasMetadata;
        return this;
    }

    public LakeSoulDynamicKafkaSerializationSchemaBuilder setMetadataPositions(int[] metadataPositions) {
        this.metadataPositions = metadataPositions;
        return this;
    }

    public LakeSoulDynamicKafkaSerializationSchemaBuilder isUpsertMode(boolean isUpsertMode) {
        this.upsertMode = isUpsertMode;
        return this;
    }

    public LakeSoulDynamicKafkaRecordSerializationSchema build() {
        this.partitioner = new LakSoulKafkaPartitioner<>();
        return new LakeSoulDynamicKafkaRecordSerializationSchema(this.topic,
                this.schemaRegistryUrl,
                this.kafkaConfigs,
                this.partitioner,
                this.keySerialization,
                this.keyFieldGetters,
                this.hasMetadata,
                this.metadataPositions,
                this.upsertMode);
    }
}
