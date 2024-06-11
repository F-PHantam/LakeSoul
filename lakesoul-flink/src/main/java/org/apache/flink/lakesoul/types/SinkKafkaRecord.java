package org.apache.flink.lakesoul.types;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class SinkKafkaRecord {

    private TableSchemaIdentity tableSchemaIdentity;

    private RowData rowData;

    private RowType rowType;

    public SinkKafkaRecord(TableSchemaIdentity tableSchemaIdentity, RowData rowData, RowType rowType) {
        this.tableSchemaIdentity = tableSchemaIdentity;
        this.rowData = rowData;
        this.rowType = rowType;
    }

    public TableSchemaIdentity getTableSchemaIdentity() {
        return tableSchemaIdentity;
    }

    public void setTableSchemaIdentity(TableSchemaIdentity tableSchemaIdentity) {
        this.tableSchemaIdentity = tableSchemaIdentity;
    }

    public RowData getRowData() {
        return rowData;
    }

    public void setRowData(RowData rowData) {
        this.rowData = rowData;
    }

    public RowType getRowType() {
        return rowType;
    }

    public void setRowType(RowType rowType) {
        this.rowType = rowType;
    }
}
