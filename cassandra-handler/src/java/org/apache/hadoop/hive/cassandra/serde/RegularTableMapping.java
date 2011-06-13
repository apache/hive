package org.apache.hadoop.hive.cassandra.serde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.cassandra.output.CassandraColumn;
import org.apache.hadoop.hive.cassandra.output.CassandraPut;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Writable;

public class RegularTableMapping extends TableMapping {
  public RegularTableMapping(
      String colFamily,
      List<String> columnNames,
      SerDeParameters serdeParams) {
    super(colFamily, columnNames, serdeParams);
  }

  @Override
  public Writable write(
      byte[] keyBytes,
      List<? extends StructField> fields,
      List<Object> list,
      List<? extends StructField> declaredFields) throws IOException {
    CassandraPut put = new CassandraPut(ByteBuffer.wrap(keyBytes));

    for (int i = 0; i < cassandraColumnNames.size(); i++) {
      if (i == iKey) {
        //Skip since this is the row key.
        continue;
      }
      String cassandraColumn = cassandraColumnNames.get(i);

      // Get the field objectInspector and the field object.
      ObjectInspector foi = fields.get(i).getFieldObjectInspector();

      Object f = null;
      if (list == null) {
        return null;
      } else {
        assert i < list.size();
        f = list.get(i);
      }

      // If the field corresponds to a column family in cassandra
      // (when would someone ever need this?)
      if (cassandraColumn.endsWith(":")) {
        MapObjectInspector moi = (MapObjectInspector) foi;
        ObjectInspector koi = moi.getMapKeyObjectInspector();
        ObjectInspector voi = moi.getMapValueObjectInspector();

        Map<?, ?> map = moi.getMap(f);
        if (map == null) {
          return null;
        } else {
          for (Map.Entry<?, ?> entry : map.entrySet()) {
            // Get the Key
            serializeStream.reset();
            serialize(entry.getKey(), koi, 3);

            // Get the column-qualifier
            byte[] columnQualifier = new byte[serializeStream.getCount()];
            System.arraycopy(serializeStream.getData(), 0, columnQualifier, 0,
                serializeStream.getCount());

            // Get the Value
            serializeStream.reset();

            boolean isNotNull = serialize(entry.getValue(), voi, 3);
            if (!isNotNull) {
              continue;
            }
            byte[] value = new byte[serializeStream.getCount()];
            System.arraycopy(serializeStream.getData(), 0, value, 0, serializeStream.getCount());

            CassandraColumn cc = new CassandraColumn();
            cc.setTimeStamp(System.currentTimeMillis());
            cc.setColumnFamily(cassandraColumnFamily);
            cc.setColumn(columnQualifier);
            cc.setValue(value);
            put.getColumns().add(cc);

          }
        }
      } else {
        CassandraColumn cc = new CassandraColumn();
        cc.setTimeStamp(System.currentTimeMillis());
        cc.setColumnFamily(cassandraColumnFamily);
        cc.setColumn(cassandraColumn.getBytes());
        byte[] key = serializeToBytes(foi, f, useJsonSerialize(i, declaredFields));
        cc.setValue(key);
        put.getColumns().add(cc);
      }
    }

    return put;
  }

}
