package org.apache.flink.lakesoul.types;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.flink.lakesoul.tool.LakeSoulKeyGen;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

public class LakSoulKafkaPartitioner<T> extends FlinkKafkaPartitioner<T> {

    private static final long serialVersionUID = -3785320239953858771L;

    private int parallelInstanceId;

    @Override
    public void open(int parallelInstanceId, int parallelInstances) {
        Preconditions.checkArgument(
                parallelInstanceId >= 0, "Id of this subtask cannot be negative.");
        Preconditions.checkArgument(
                parallelInstances > 0, "Number of subtasks must be larger than 0.");

        this.parallelInstanceId = parallelInstanceId;
    }

    @Override
    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        Preconditions.checkArgument(
                partitions != null && partitions.length > 0,
                "Partitions of the target topic is empty.");

        if (record instanceof LakeSoulArrowWrapper) {

            TableSchemaIdentity tableSchemaIdentity = ((LakeSoulArrowWrapper) record).generateTableSchemaIdentity();
            RowType rowType = tableSchemaIdentity.rowType;

            byte[] encodedBatch = ((LakeSoulArrowWrapper) record).getEncodedBatch();
            BufferAllocator allocator = ArrowUtils.getRootAllocator().newChildAllocator("allocator", 0, Long.MAX_VALUE);
            ArrowStreamReader arrowStreamReader = new ArrowStreamReader(new ByteArrayInputStream(encodedBatch), allocator);
            VectorSchemaRoot vectorSchemaRoot;
            try {
                arrowStreamReader.loadNextBatch();
                vectorSchemaRoot = arrowStreamReader.getVectorSchemaRoot();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            ArrowReader arrowReader = ArrowUtils.createArrowReader(vectorSchemaRoot, rowType);
            RowData rowData = arrowReader.read(0);

            long hash = 42;
            List<String> primaryKeys = tableSchemaIdentity.primaryKeys;
            for (String pk : primaryKeys) {
                int typeIndex = rowType.getFieldIndex(pk);
                LogicalType type = rowType.getTypeAt(typeIndex);
                Object fieldOrNull = RowData.createFieldGetter(type, typeIndex).getFieldOrNull(rowData);
                hash = LakeSoulKeyGen.getHash(type, fieldOrNull, hash);
            }
            return partitions[(int) (hash % partitions.length)];
        } else if (record instanceof SinkKafkaRecord) {
            TableSchemaIdentity tableSchemaIdentity = ((SinkKafkaRecord) record).getTableSchemaIdentity();
            RowData rowData = ((SinkKafkaRecord) record).getRowData();
            RowType rowType = ((SinkKafkaRecord) record).getRowType();
            long hash = 42;
            List<String> primaryKeys = tableSchemaIdentity.primaryKeys;
            for (String pk : primaryKeys) {
                int typeIndex = rowType.getFieldIndex(pk);
                LogicalType type = rowType.getTypeAt(typeIndex);
                Object fieldOrNull = RowData.createFieldGetter(type, typeIndex).getFieldOrNull(rowData);
                hash = LakeSoulKeyGen.getHash(type, fieldOrNull, hash);
            }
        }

        return partitions[parallelInstanceId % partitions.length];
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof FlinkFixedPartitioner;
    }

    @Override
    public int hashCode() {
        return FlinkFixedPartitioner.class.hashCode();
    }
}
