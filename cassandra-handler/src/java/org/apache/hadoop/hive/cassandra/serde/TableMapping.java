package org.apache.hadoop.hive.cassandra.serde;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Writable;

public abstract class TableMapping {
  /* names of columns from SerdeParameters */
  protected final List<String> cassandraColumnNames;
  /* index of key column in results */
  protected final int iKey;
  protected final String cassandraColumnFamily;

  private boolean useJSONSerialize;
  protected final ByteStream.Output serializeStream = new ByteStream.Output();

  private final byte[] separators; // the separators array
  private final boolean escaped; // whether we need to escape the data when writing out
  private final byte escapeChar; // which char to use as the escape char, e.g. '\\'
  private final boolean[] needsEscape; // which chars need to be escaped. This array should have size


  TableMapping(String colFamily, List<String> columnNames, SerDeParameters serdeParams) {
    this.cassandraColumnFamily = colFamily;
    this.cassandraColumnNames = columnNames;
    this.iKey = cassandraColumnNames.indexOf(AbstractColumnSerDe.CASSANDRA_KEY_COLUMN);

    separators = serdeParams.getSeparators();
    escaped = serdeParams.isEscaped();
    escapeChar = serdeParams.getEscapeChar();
    needsEscape = serdeParams.getNeedsEscape();
  }

  public Writable getWritable(
      List<? extends StructField> fields,
      List<Object> list,
      List<? extends StructField> declaredFields) throws IOException {
    assert iKey >= 0;
    //First get the cassandra row key
    byte[] keyBytes = serializeToBytes(iKey, fields, list, declaredFields);

    return write(keyBytes, fields, list, declaredFields);
  }

  public abstract Writable write(
      byte[] keyBytes,
      List<? extends StructField> fields,
      List<Object> list,
      List<? extends StructField> declaredFields) throws IOException;

  /**
   * Serialize the index-th object into bytes array.
   *
   * @param index the index of the object to be seralized.
   * @param fields a list of fields
   * @param list a list of objects
   * @param declaredFields a list of declared fields
   * @return object serialized into bytes
   * @throws IOException
   */
  protected byte[] serializeToBytes(int index,
      List<? extends StructField> fields,
      List<Object> list,
      List<? extends StructField> declaredFields) throws IOException {
    return serializeToBytes(
        fields.get(index).getFieldObjectInspector(),
        list.get(index),
        useJsonSerialize(index, declaredFields));
  }

  /**
   * Return true if using json serialization. Otherwise, false;
   *
   * @param index the index of the field to be deserialized.
   * @param declaredFields a list of declared fields
   * @return true if using json serialization
   */
  protected boolean useJsonSerialize(int index, List<? extends StructField> declaredFields) {
    return (declaredFields == null ||
        declaredFields.get(index).getFieldObjectInspector().getCategory()
        .equals(Category.PRIMITIVE) || useJSONSerialize);
  }

  /**
   * Serialize a object into bytes.
   * @param foi object inspector
   * @param obj object to be serialized
   * @param useJsonSerialize true to use json serialization
   * @return object in serialized bytes
   * @throws IOException when error happens
   */
  protected byte[] serializeToBytes(ObjectInspector foi, Object obj, boolean useJsonSerialize) throws IOException {
    serializeStream.reset();
    boolean isNotNull;
    if (!foi.getCategory().equals(Category.PRIMITIVE)
                && useJsonSerialize) {
      isNotNull = serialize(SerDeUtils.getJSONString(obj, foi),
                  PrimitiveObjectInspectorFactory.javaStringObjectInspector, 1);
    } else {
      isNotNull = serialize(obj, foi, 1);
    }
    if (!isNotNull) {
      return null;
    }
    byte[] key = new byte[serializeStream.getCount()];
    System.arraycopy(serializeStream.getData(), 0, key, 0, serializeStream.getCount());

    return key;
  }

  protected boolean serialize(Object obj, ObjectInspector objInspector, int level)
  throws IOException {

    switch (objInspector.getCategory()) {
      case PRIMITIVE: {
        LazyUtils.writePrimitiveUTF8(
                serializeStream, obj,
                (PrimitiveObjectInspector) objInspector,
                escaped, escapeChar, needsEscape);
        return true;
      }
      case LIST: {
        char separator = (char) separators[level];
        ListObjectInspector loi = (ListObjectInspector) objInspector;
        List<?> list = loi.getList(obj);
        ObjectInspector eoi = loi.getListElementObjectInspector();
        if (list == null) {
          return false;
        } else {
          for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
              serializeStream.write(separator);
            }
            serialize(list.get(i), eoi, level + 1);
          }
        }
        return true;
      }
      case MAP: {
        char separator = (char) separators[level];
        char keyValueSeparator = (char) separators[level + 1];
        MapObjectInspector moi = (MapObjectInspector) objInspector;
        ObjectInspector koi = moi.getMapKeyObjectInspector();
        ObjectInspector voi = moi.getMapValueObjectInspector();

        Map<?, ?> map = moi.getMap(obj);
        if (map == null) {
          return false;
        } else {
          boolean first = true;
          for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (first) {
              first = false;
            } else {
              serializeStream.write(separator);
            }
            serialize(entry.getKey(), koi, level + 2);
            serializeStream.write(keyValueSeparator);
            serialize(entry.getValue(), voi, level + 2);
          }
        }
        return true;
      }
      case STRUCT: {
        char separator = (char) separators[level];
        StructObjectInspector soi = (StructObjectInspector) objInspector;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        List<Object> list = soi.getStructFieldsDataAsList(obj);
        if (list == null) {
          return false;
        } else {
          for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
              serializeStream.write(separator);
            }
            serialize(list.get(i), fields.get(i).getFieldObjectInspector(), level + 1);
          }
        }
        return true;
      }
    }
    throw new RuntimeException("Unknown category type: " + objInspector.getCategory());
  }
}
