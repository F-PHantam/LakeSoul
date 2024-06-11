// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.lakesoul.entry.LakeSoulDynamicKafkaSerializationSchemaBuilder;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class LakeSoulDynamicKafkaRecordSerializationSchema implements KafkaRecordSerializationSchema<SinkKafkaRecord> {

    private final String topic;

    private final String schemaRegistryUrl;

    private final Map<String, ?> kafkaConfigs;
    private final FlinkKafkaPartitioner<SinkKafkaRecord> partitioner;
    @Nullable private final SerializationSchema<RowData> keySerialization;
    private SerializationSchema<RowData> valueSerialization;
    private final RowData.FieldGetter[] keyFieldGetters;
    private RowData.FieldGetter[] valueFieldGetters;
    private final boolean hasMetadata;
    private final int[] metadataPositions;
    private final boolean upsertMode;

    public LakeSoulDynamicKafkaRecordSerializationSchema(
            String topic,
            String schemaRegistryUrl,
            @Nullable Map<String, ?> kafkaConfigs,
            @Nullable FlinkKafkaPartitioner<SinkKafkaRecord> partitioner,
            @Nullable SerializationSchema<RowData> keySerialization,
//            SerializationSchema<LakeSoulArrowWrapper> valueSerialization,
            RowData.FieldGetter[] keyFieldGetters,
//            RowData.FieldGetter[] valueFieldGetters,
            boolean hasMetadata,
            int[] metadataPositions,
            boolean upsertMode) {
        if (upsertMode) {
            Preconditions.checkArgument(
                    keySerialization != null && keyFieldGetters.length > 0,
                    "Key must be set in upsert mode for serialization schema.");
        }
        this.topic = checkNotNull(topic);
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.kafkaConfigs = kafkaConfigs;
        this.partitioner = partitioner;
        this.keySerialization = keySerialization;
//        this.valueSerialization = checkNotNull(valueSerialization);
        this.keyFieldGetters = keyFieldGetters;
//        this.valueFieldGetters = valueFieldGetters;
        this.hasMetadata = hasMetadata;
        this.metadataPositions = metadataPositions;
        this.upsertMode = upsertMode;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        if (keySerialization != null) {
            keySerialization.open(context);
        }
        if (partitioner != null) {
            partitioner.open(
                    sinkContext.getParallelInstanceId(),
                    sinkContext.getNumberOfParallelInstances());
        }
        valueSerialization.open(context);
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(SinkKafkaRecord sinkKafkaRecord, KafkaSinkContext kafkaSinkContext, Long aLong) {

        TableSchemaIdentity tableSchemaIdentity = sinkKafkaRecord.getTableSchemaIdentity();
        RowType rowType = sinkKafkaRecord.getRowType();
        initSerializationSchema(rowType);

//        byte[] encodedBatch = lakeSoulArrowWrapper.getEncodedBatch();
//        BufferAllocator allocator = ArrowUtils.getRootAllocator().newChildAllocator("allocator", 0, Long.MAX_VALUE);
//        ArrowStreamReader arrowStreamReader = new ArrowStreamReader(new ByteArrayInputStream(encodedBatch), allocator);
//        VectorSchemaRoot vectorSchemaRoot;
//        try {
//            arrowStreamReader.loadNextBatch();
//            vectorSchemaRoot = arrowStreamReader.getVectorSchemaRoot();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        int count = vectorSchemaRoot.getRowCount();
//        ArrowReader arrowReader = ArrowUtils.createArrowReader(vectorSchemaRoot, rowType);

        RowData rowData = sinkKafkaRecord.getRowData();
        if (keySerialization == null && !hasMetadata) {
            final byte[] valueSerialized = valueSerialization.serialize(rowData);
            return new ProducerRecord<>(
                    topic,
                    extractPartition(
                            sinkKafkaRecord,
                            null,
                            valueSerialized,
                            kafkaSinkContext.getPartitionsForTopic(topic)),
                    null,
                    valueSerialized);
        }
        final byte[] keySerialized;
        if (keySerialization == null) {
            keySerialized = null;
        } else {
            final RowData keyRow = createProjectedRow(rowData, RowKind.INSERT, keyFieldGetters);
            keySerialized = keySerialization.serialize(keyRow);
        }

        final byte[] valueSerialized;
        final RowKind kind = rowData.getRowKind();
        if (upsertMode) {
            if (kind == RowKind.DELETE || kind == RowKind.UPDATE_BEFORE) {
                // transform the message as the tombstone message
                valueSerialized = null;
            } else {
                // make the message to be INSERT to be compliant with the INSERT-ONLY format
                final RowData valueRow =
                        LakeSoulDynamicKafkaRecordSerializationSchema.createProjectedRow(
                                rowData, kind, valueFieldGetters);
                valueRow.setRowKind(RowKind.INSERT);
                valueSerialized = valueSerialization.serialize(valueRow);
            }
        } else {
            final RowData valueRow =
                    LakeSoulDynamicKafkaRecordSerializationSchema.createProjectedRow(
                            rowData, kind, valueFieldGetters);
            valueSerialized = valueSerialization.serialize(valueRow);
        }

        return new ProducerRecord<>(
                topic,
                extractPartition(
                        sinkKafkaRecord,
                        keySerialized,
                        valueSerialized,
                        kafkaSinkContext.getPartitionsForTopic(topic)),
                readMetadata(rowData, WritableMetadata.TIMESTAMP),
                keySerialized,
                valueSerialized,
                readMetadata(rowData, WritableMetadata.HEADERS));
    }

    private void initSerializationSchema(RowType rowType) {
        this.valueFieldGetters = IntStream.range(0, rowType.getFieldCount())
                .mapToObj(
                        i ->
                                RowData.createFieldGetter(
                                        rowType.getTypeAt(i), i))
                .toArray(RowData.FieldGetter[]::new);

        ConfluentRegistryAvroSerializationSchema<GenericRecord> genericRecordConfluentRegistryAvroSerializationSchema =
                ConfluentRegistryAvroSerializationSchema.forGeneric(
                        String.format("%s-value", topic),
                        AvroSchemaConverter.convertToSchema(rowType),
                        this.schemaRegistryUrl,
                        this.kafkaConfigs);
        AvroRowDataSerializationSchema avroRowDataSerializationSchema = new AvroRowDataSerializationSchema(
                rowType,
                genericRecordConfluentRegistryAvroSerializationSchema,
                RowDataToAvroConverters.createConverter(rowType));
        this.valueSerialization = avroRowDataSerializationSchema;
    }

    private Integer extractPartition(
            SinkKafkaRecord consumedRow,
            @Nullable byte[] keySerialized,
            byte[] valueSerialized,
            int[] partitions) {
        if (partitioner != null) {
            return partitioner.partition(
                    consumedRow, keySerialized, valueSerialized, topic, partitions);
        }
        return null;
    }

    static RowData createProjectedRow(
            RowData consumedRow, RowKind kind, RowData.FieldGetter[] fieldGetters) {
        final int arity = fieldGetters.length;
        final GenericRowData genericRowData = new GenericRowData(kind, arity);
        for (int fieldPos = 0; fieldPos < arity; fieldPos++) {
            genericRowData.setField(fieldPos, fieldGetters[fieldPos].getFieldOrNull(consumedRow));
        }
        return genericRowData;
    }

    private <T> T readMetadata(RowData consumedRow, WritableMetadata metadata) {
        final int pos = metadataPositions[metadata.ordinal()];
        if (pos < 0) {
            return null;
        }
        return (T) metadata.converter.read(consumedRow, pos);
    }

    enum WritableMetadata {
        HEADERS(
                "headers",
                // key and value of the map are nullable to make handling easier in queries
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.BYTES().nullable())
                        .nullable(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(RowData row, int pos) {
                        if (row.isNullAt(pos)) {
                            return null;
                        }
                        final MapData map = row.getMap(pos);
                        final ArrayData keyArray = map.keyArray();
                        final ArrayData valueArray = map.valueArray();
                        final List<Header> headers = new ArrayList<>();
                        for (int i = 0; i < keyArray.size(); i++) {
                            if (!keyArray.isNullAt(i) && !valueArray.isNullAt(i)) {
                                final String key = keyArray.getString(i).toString();
                                final byte[] value = valueArray.getBinary(i);
                                headers.add(new KafkaHeader(key, value));
                            }
                        }
                        return headers;
                    }
                }),

        TIMESTAMP(
                "timestamp",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(),
                new MetadataConverter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(RowData row, int pos) {
                        if (row.isNullAt(pos)) {
                            return null;
                        }
                        return row.getTimestamp(pos, 3).getMillisecond();
                    }
                });

        final String key;

        final DataType dataType;

        final MetadataConverter converter;

        WritableMetadata(String key, DataType dataType, MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }

    interface MetadataConverter extends Serializable {
        Object read(RowData var1, int var2);
    }

    private static class KafkaHeader implements Header {
        private final String key;
        private final byte[] value;

        KafkaHeader(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public String key() {
            return this.key;
        }

        public byte[] value() {
            return this.value;
        }
    }

    public static LakeSoulDynamicKafkaSerializationSchemaBuilder builder() {
        return new LakeSoulDynamicKafkaSerializationSchemaBuilder();
    }

}
