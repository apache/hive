package org.apache.hadoop.hive.cassandra.serde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.hive.cassandra.output.CassandraColumn;
import org.apache.hadoop.hive.cassandra.output.CassandraPut;
import org.apache.hadoop.hive.cassandra.output.CassandraSuperPut;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Writable;

public class TransposedMapping extends TableMapping {
  /* Track the index of :column, :subcolumn, and :value. This would only be set when it is a transposed table*/
  private int columnName = -1;
  private int columnValue = -1;
  private int subColumnName = -1;

  public TransposedMapping(
      String colFamily,
      List<String> columnNames,
      SerDeParameters serdeParams) throws SerDeException {
    super(colFamily, columnNames, serdeParams);
    init();
  }

  private void init() throws SerDeException {
    setTransposedTableIndex();
  }

  @Override
  public Writable write(
      byte[] keyBytes,
      List<? extends StructField> fields,
      List<Object> list,
      List<? extends StructField> declaredFields) throws IOException {
    assert columnName >= 0;
    assert columnValue >= 0;
    if (subColumnName == -1) {
      //A regular column family mapping
      CassandraPut put = new CassandraPut(ByteBuffer.wrap(keyBytes));
      CassandraColumn cc = new CassandraColumn();
      cc.setTimeStamp(System.currentTimeMillis());
      cc.setColumnFamily(cassandraColumnFamily);
      cc.setColumn(serializeToBytes(columnName, fields, list, declaredFields));
      cc.setValue(serializeToBytes(columnValue, fields, list, declaredFields));
      put.getColumns().add(cc);

      return put;
    } else {
      //A super column family mapping
      //This is a super column family mapping
      CassandraSuperPut put = new CassandraSuperPut(ByteBuffer.wrap(keyBytes));
      byte[] colName = serializeToBytes(
          fields.get(columnName).getFieldObjectInspector(),
          list.get(columnName),
          useJsonSerialize(columnName, declaredFields));
      CassandraPut column = new CassandraPut(ByteBuffer.wrap(colName));
      CassandraColumn cc = new CassandraColumn();
      cc.setTimeStamp(System.currentTimeMillis());
      cc.setColumnFamily(cassandraColumnFamily);
      cc.setColumn(serializeToBytes(subColumnName, fields, list, declaredFields));
      cc.setValue(serializeToBytes(columnValue, fields, list, declaredFields));
      column.getColumns().add(cc);
      put.getColumns().add(column);

      return put;
    }
  }

  /**
   * Save the index of :column, :value and :subcolumn and the transposedTable flag if and only if this is a transposed table.
   *
   * @throws SerDeException
   */
  private void setTransposedTableIndex() throws SerDeException {
    int key = -1;
    int columnName = -1;
    int columnValue = -1;
    int subColumnName = -1;
    for (int i = 0; i < cassandraColumnNames.size(); i++) {
      String str = cassandraColumnNames.get(i);
      if (str.equals(AbstractColumnSerDe.CASSANDRA_KEY_COLUMN)) {
        key = i;
      } else if  (str.equals(AbstractColumnSerDe.CASSANDRA_COLUMN_COLUMN)) {
        columnName = i;
      } else if (str.equals(AbstractColumnSerDe.CASSANDRA_VALUE_COLUMN)) {
        columnValue = i;
      } else if (str.equals(AbstractColumnSerDe.CASSANDRA_SUBCOLUMN_COLUMN)) {
        subColumnName = i;
      } else {
        throw new SerDeException("An expected mapping appears in the column mapping " + str);
      }
    }

    if (key >= 0 && columnName >= 0 && columnValue >= 0) {
      if (subColumnName != -1) {
        this.subColumnName = subColumnName;
      }

      this.columnName = columnName;
      this.columnValue = columnValue;
    }
  }


}
