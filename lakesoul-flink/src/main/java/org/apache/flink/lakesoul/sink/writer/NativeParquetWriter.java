// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer;

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOWriter;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.arrow.ArrowWriter;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.MAX_ROW_GROUP_SIZE;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SORT_FIELD;

public class NativeParquetWriter implements InProgressFileWriter<RowData, String> {

    private static final Logger LOG = LoggerFactory.getLogger(NativeParquetWriter.class);

    private final ArrowWriter<RowData> arrowWriter;

    private NativeIOWriter nativeWriter;

    private final int maxRowGroupRows;

    private final long creationTime;

    private final VectorSchemaRoot batch;

    private final String bucketID;

    private int rowsInBatch;

    long lastUpdateTime;

    Path path;

    private long totalRows = 0;

    public NativeParquetWriter(RowType rowType,
                               List<String> primaryKeys,
                               String bucketID,
                               Path path,
                               long creationTime,
                               Configuration conf) throws IOException {
        this.maxRowGroupRows = conf.getInteger(MAX_ROW_GROUP_SIZE);
        this.creationTime = creationTime;
        this.bucketID = bucketID;
        this.rowsInBatch = 0;

        ArrowUtils.setLocalTimeZone(FlinkUtil.getLocalTimeZone(conf));
        Schema arrowSchema = ArrowUtils.toArrowSchema(rowType);
        nativeWriter = new NativeIOWriter(arrowSchema);
        nativeWriter.setPrimaryKeys(primaryKeys);
        if (conf.getBoolean(LakeSoulSinkOptions.isMultiTableSource)) {
            nativeWriter.setAuxSortColumns(Collections.singletonList(SORT_FIELD));
        }
        nativeWriter.setRowGroupRowNumber(this.maxRowGroupRows);
        batch = VectorSchemaRoot.create(arrowSchema, nativeWriter.getAllocator());
        arrowWriter = ArrowUtils.createRowDataArrowWriter(batch, rowType);
        this.path = path.makeQualified(path.getFileSystem());
        nativeWriter.addFile(this.path.toUri().toString());

        FlinkUtil.setFSConfigs(conf, this.nativeWriter);
        this.nativeWriter.initializeWriter();
    }

    @Override
    public void write(RowData element, long currentTime) throws IOException {
        this.lastUpdateTime = currentTime;
        this.arrowWriter.write(element);
        this.rowsInBatch++;
        this.totalRows++;
        if (this.rowsInBatch >= this.maxRowGroupRows) {
            this.arrowWriter.finish();
            this.nativeWriter.write(this.batch);
            // in native writer, batch may be kept in memory for sorting,
            // so we have to release ownership in java
            this.batch.clear();
            this.arrowWriter.reset();
            this.rowsInBatch = 0;
        }
    }

    @Override
    public InProgressFileRecoverable persist() throws IOException {
        // we currently do not support persist
        return null;
    }

    public static class NativePendingFileRecoverableSerializer
            implements SimpleVersionedSerializer<PendingFileRecoverable> {

        public static final NativePendingFileRecoverableSerializer INSTANCE =
                new NativePendingFileRecoverableSerializer();

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(InProgressFileWriter.PendingFileRecoverable obj) throws IOException {
            if (!(obj instanceof NativeParquetWriter.NativeWriterPendingFileRecoverable)) {
                throw new UnsupportedOperationException(
                        "Only NativeParquetWriter.NativeWriterPendingFileRecoverable is supported.");
            }
            DataOutputSerializer out = new DataOutputSerializer(256);
            NativeParquetWriter.NativeWriterPendingFileRecoverable recoverable =
                    (NativeParquetWriter.NativeWriterPendingFileRecoverable) obj;
            out.writeUTF(recoverable.path);
            out.writeLong(recoverable.creationTime);
            return out.getCopyOfBuffer();
        }

        @Override
        public InProgressFileWriter.PendingFileRecoverable deserialize(int version, byte[] serialized)
                throws IOException {
            DataInputDeserializer in = new DataInputDeserializer(serialized);
            String path = in.readUTF();
            long time = in.readLong();
            return new NativeParquetWriter.NativeWriterPendingFileRecoverable(path, time);
        }
    }

    static public class NativeWriterPendingFileRecoverable implements PendingFileRecoverable, Serializable {
        public String path;

        public long creationTime;

        public NativeWriterPendingFileRecoverable(String path, long creationTime) {
            this.path = path;
            this.creationTime = creationTime;
        }

        @Override
        public String toString() {
            return "PendingFile(" +
                    path + ", " + creationTime + ")";
        }

        @Nullable
        @Override
        public Path getPath() {
            return new Path(path);
        }

        @Override
        public long getSize() {
            return 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, creationTime);
        }
    }

    @Override
    public PendingFileRecoverable closeForCommit() throws IOException {
        this.arrowWriter.finish();
        this.nativeWriter.write(this.batch);
        this.nativeWriter.flush();
        this.arrowWriter.reset();
        this.rowsInBatch = 0;
        this.batch.clear();
        this.batch.close();
        try {
            this.nativeWriter.close();
            this.nativeWriter = null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new NativeWriterPendingFileRecoverable(this.path.toString(), this.creationTime);
    }

    @Override
    public void dispose() {
        try {
            this.arrowWriter.finish();
            this.batch.close();
            this.nativeWriter.close();
            this.nativeWriter = null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getBucketId() {
        return this.bucketID;
    }

    @Override
    public long getCreationTime() {
        return this.creationTime;
    }

    @Override
    public long getSize() throws IOException {
        return totalRows;
    }

    @Override
    public long getLastUpdateTime() {
        return this.lastUpdateTime;
    }

    @Override public String toString() {
        return "NativeParquetWriter{" +
                "maxRowGroupRows=" + maxRowGroupRows +
                ", creationTime=" + creationTime +
                ", bucketID='" + bucketID + '\'' +
                ", rowsInBatch=" + rowsInBatch +
                ", lastUpdateTime=" + lastUpdateTime +
                ", path=" + path +
                ", totalRows=" + totalRows +
                '}';
    }
}
