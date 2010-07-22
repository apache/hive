/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;

/**
 * HBaseSerDe can be used to serialize object into an HBase table and
 * deserialize objects from an HBase table.
 */
public class HBaseSerDe implements SerDe {

  public static final String HBASE_COLUMNS_MAPPING = "hbase.columns.mapping";
  public static final String HBASE_TABLE_NAME = "hbase.table.name";
  public static final String HBASE_KEY_COL = ":key";
  public static final Log LOG = LogFactory.getLog(HBaseSerDe.class);

  private ObjectInspector cachedObjectInspector;
  private List<String> hbaseColumnNames;
  private List<byte []> hbaseColumnNamesBytes;
  private SerDeParameters serdeParams;
  private boolean useJSONSerialize;
  private LazyHBaseRow cachedHBaseRow;
  private final ByteStream.Output serializeStream = new ByteStream.Output();
  private int iKey;

  // used for serializing a field
  private byte [] separators;     // the separators array
  private boolean escaped;        // whether we need to escape the data when writing out
  private byte escapeChar;        // which char to use as the escape char, e.g. '\\'
  private boolean [] needsEscape; // which chars need to be escaped. This array should have size
                                  // of 128. Negative byte values (or byte values >= 128) are
                                  // never escaped.
  @Override
  public String toString() {
    return getClass().toString()
        + "["
        + hbaseColumnNames
        + ":"
        + ((StructTypeInfo) serdeParams.getRowTypeInfo())
            .getAllStructFieldNames()
        + ":"
        + ((StructTypeInfo) serdeParams.getRowTypeInfo())
            .getAllStructFieldTypeInfos() + "]";
  }

  public HBaseSerDe() throws SerDeException {
  }

  /**
   * Initialize the SerDe given parameters.
   * @see SerDe#initialize(Configuration, Properties)
   */
  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {

    initHBaseSerDeParameters(conf, tbl,
        getClass().getName());

    cachedObjectInspector = LazyFactory.createLazyStructInspector(
        serdeParams.getColumnNames(),
        serdeParams.getColumnTypes(),
        serdeParams.getSeparators(),
        serdeParams.getNullSequence(),
        serdeParams.isLastColumnTakesRest(),
        serdeParams.isEscaped(),
        serdeParams.getEscapeChar());

    cachedHBaseRow = new LazyHBaseRow(
      (LazySimpleStructObjectInspector) cachedObjectInspector);

    if (LOG.isDebugEnabled()) {
      LOG.debug("HBaseSerDe initialized with : columnNames = "
        + serdeParams.getColumnNames()
        + " columnTypes = "
        + serdeParams.getColumnTypes()
        + " hbaseColumnMapping = "
        + hbaseColumnNames);
    }
  }

  public static List<String> parseColumnMapping(String columnMapping) {
    String [] columnArray = columnMapping.split(",");
    List<String> columnList = Arrays.asList(columnArray);
    int iKey = columnList.indexOf(HBASE_KEY_COL);
    if (iKey == -1) {
      columnList = new ArrayList<String>(columnList);
      columnList.add(0, HBASE_KEY_COL);
    }
    return columnList;
  }

  public static List<byte []> initColumnNamesBytes(List<String> columnNames) {
    List<byte []> columnBytes = new ArrayList<byte []>();
    String column = null;

    for (int i = 0; i < columnNames.size(); i++) {
      column = columnNames.get(i);

      if (column.endsWith(":")) {
        columnBytes.add(Bytes.toBytes(column.split(":")[0]));
      } else {
        columnBytes.add(Bytes.toBytes(column));
      }
    }

    return columnBytes;
  }

  public static boolean isSpecialColumn(String hbaseColumnName) {
    return hbaseColumnName.equals(HBASE_KEY_COL);
  }

  private void initHBaseSerDeParameters(
      Configuration job, Properties tbl, String serdeName)
    throws SerDeException {

    // Read configuration parameters
    String hbaseColumnNameProperty =
      tbl.getProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING);
    String columnTypeProperty =
      tbl.getProperty(Constants.LIST_COLUMN_TYPES);

    // Initialize the HBase column list
    hbaseColumnNames = parseColumnMapping(hbaseColumnNameProperty);
    iKey = hbaseColumnNames.indexOf(HBASE_KEY_COL);

    // initialize the byte [] corresponding to each column name
    hbaseColumnNamesBytes = initColumnNamesBytes(hbaseColumnNames);

    // Build the type property string if not supplied
    if (columnTypeProperty == null) {
      StringBuilder sb = new StringBuilder();

      for (int i = 0; i < hbaseColumnNames.size(); i++) {
        if (sb.length() > 0) {
          sb.append(":");
        }
        String colName = hbaseColumnNames.get(i);
        if (isSpecialColumn(colName)) {
            // a special column becomes a STRING
            sb.append(Constants.STRING_TYPE_NAME);
        } else if (colName.endsWith(":"))  {
          // a column family become a MAP
          sb.append(
            Constants.MAP_TYPE_NAME + "<"
            + Constants.STRING_TYPE_NAME
            + "," + Constants.STRING_TYPE_NAME + ">");
        } else {
          // an individual column becomes a STRING
          sb.append(Constants.STRING_TYPE_NAME);
        }
      }
      tbl.setProperty(Constants.LIST_COLUMN_TYPES, sb.toString());
    }

    serdeParams = LazySimpleSerDe.initSerdeParams(job, tbl, serdeName);

    if (hbaseColumnNames.size() != serdeParams.getColumnNames().size()) {
      throw new SerDeException(serdeName + ": columns has " +
        serdeParams.getColumnNames().size() +
        " elements while hbase.columns.mapping has " +
        hbaseColumnNames.size() + " elements" +
        " (counting the key if implicit)");
    }

    separators = serdeParams.getSeparators();
    escaped = serdeParams.isEscaped();
    escapeChar = serdeParams.getEscapeChar();
    needsEscape = serdeParams.getNeedsEscape();

    // check that the mapping schema is right;
    // we just can make sure that "column-family:" is mapped to MAP<String,?>
    for (int i = 0; i < hbaseColumnNames.size(); i++) {
      String hbaseColName = hbaseColumnNames.get(i);
      if (hbaseColName.endsWith(":")) {
        TypeInfo typeInfo = serdeParams.getColumnTypes().get(i);
        if ((typeInfo.getCategory() != Category.MAP) ||
          (((MapTypeInfo) typeInfo).getMapKeyTypeInfo().getTypeName()
            !=  Constants.STRING_TYPE_NAME)) {

          throw new SerDeException(
            serdeName + ": hbase column family '"
            + hbaseColName
            + "' should be mapped to map<string,?> but is mapped to "
            + typeInfo.getTypeName());
        }
      }
    }
  }

  /**
   * Deserialize a row from the HBase Result writable to a LazyObject
   * @param result the HBase Result Writable containing the row
   * @return the deserialized object
   * @see SerDe#deserialize(Writable)
   */
  @Override
  public Object deserialize(Writable result) throws SerDeException {

    if (!(result instanceof Result)) {
      throw new SerDeException(getClass().getName() + ": expects Result!");
    }

    Result r = (Result)result;
    cachedHBaseRow.init(r, hbaseColumnNames, hbaseColumnNamesBytes);
    return cachedHBaseRow;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Put.class;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
      throws SerDeException {
    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString()
          + " can only serialize struct types, but we got: "
          + objInspector.getTypeName());
    }

    // Prepare the field ObjectInspectors
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> list = soi.getStructFieldsDataAsList(obj);
    List<? extends StructField> declaredFields =
      (serdeParams.getRowTypeInfo() != null &&
        ((StructTypeInfo) serdeParams.getRowTypeInfo())
        .getAllStructFieldNames().size() > 0) ?
      ((StructObjectInspector)getObjectInspector()).getAllStructFieldRefs()
      : null;

    Put put = null;

    try {
      byte [] key = serializeField(iKey, null, fields, list, declaredFields);

      if (key == null) {
        throw new SerDeException("HBase row key cannot be NULL");
      }

      put = new Put(key);

      // Serialize each field
      for (int i = 0; i < fields.size(); i++) {
        if (i == iKey) {
          // already processed the key above
          continue;
        }
        serializeField(i, put, fields, list, declaredFields);
      }
    } catch (IOException e) {
      throw new SerDeException(e);
    }

    return put;
  }

  private byte [] serializeField(
    int i,
    Put put,
    List<? extends StructField> fields,
    List<Object> list,
    List<? extends StructField> declaredFields) throws IOException {

    // column name
    String hbaseColumn = hbaseColumnNames.get(i);

    // Get the field objectInspector and the field object.
    ObjectInspector foi = fields.get(i).getFieldObjectInspector();
    Object f = (list == null ? null : list.get(i));

    if (f == null) {
      // a null object, we do not serialize it
      return null;
    }

    // If the field corresponds to a column family in hbase
    if (hbaseColumn.endsWith(":")) {
      MapObjectInspector moi = (MapObjectInspector)foi;
      ObjectInspector koi = moi.getMapKeyObjectInspector();
      ObjectInspector voi = moi.getMapValueObjectInspector();

      Map<?, ?> map = moi.getMap(f);
      if (map == null) {
        return null;
      } else {
        for (Map.Entry<?, ?> entry: map.entrySet()) {
          // Get the Key
          serializeStream.reset();
          serialize(entry.getKey(), koi, 3);

          // Get the column-qualifier
          byte [] columnQualifier = new byte[serializeStream.getCount()];
          System.arraycopy(serializeStream.getData(), 0, columnQualifier, 0, serializeStream.getCount());

          // Get the Value
          serializeStream.reset();
          boolean isNotNull = serialize(entry.getValue(), voi, 3);
          if (!isNotNull) {
            continue;
          }
          byte [] value = new byte[serializeStream.getCount()];
          System.arraycopy(serializeStream.getData(), 0, value, 0, serializeStream.getCount());
          put.add(hbaseColumnNamesBytes.get(i), columnQualifier, value);
        }
      }
    } else {
      // If the field that is passed in is NOT a primitive, and either the
      // field is not declared (no schema was given at initialization), or
      // the field is declared as a primitive in initialization, serialize
      // the data to JSON string.  Otherwise serialize the data in the
      // delimited way.
      serializeStream.reset();
      boolean isNotNull;
      if (!foi.getCategory().equals(Category.PRIMITIVE)
          && (declaredFields == null ||
              declaredFields.get(i).getFieldObjectInspector().getCategory()
              .equals(Category.PRIMITIVE) || useJSONSerialize)) {

        isNotNull = serialize(
            SerDeUtils.getJSONString(f, foi),
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            1);
      } else {
        isNotNull = serialize(f, foi, 1);
      }
      if (!isNotNull) {
        return null;
      }
      byte [] key = new byte[serializeStream.getCount()];
      System.arraycopy(serializeStream.getData(), 0, key, 0, serializeStream.getCount());
      if (i == iKey) {
        return key;
      }
      put.add(hbaseColumnNamesBytes.get(i), 0, key);
    }

    return null;
  }

  /**
   * Serialize the row into a ByteStream.
   *
   * @param obj           The object for the current field.
   * @param objInspector  The ObjectInspector for the current Object.
   * @param level         The current level of separator.
   * @throws IOException
   * @return true, if serialize is a not-null object; otherwise false.
   */
  private boolean serialize(Object obj, ObjectInspector objInspector, int level)
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
        ListObjectInspector loi = (ListObjectInspector)objInspector;
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
        char keyValueSeparator = (char) separators[level+1];
        MapObjectInspector moi = (MapObjectInspector) objInspector;
        ObjectInspector koi = moi.getMapKeyObjectInspector();
        ObjectInspector voi = moi.getMapValueObjectInspector();

        Map<?, ?> map = moi.getMap(obj);
        if (map == null) {
          return false;
        } else {
          boolean first = true;
          for (Map.Entry<?, ?> entry: map.entrySet()) {
            if (first) {
              first = false;
            } else {
              serializeStream.write(separator);
            }
            serialize(entry.getKey(), koi, level+2);
            serializeStream.write(keyValueSeparator);
            serialize(entry.getValue(), voi, level+2);
          }
        }
        return true;
      }
      case STRUCT: {
        char separator = (char)separators[level];
        StructObjectInspector soi = (StructObjectInspector)objInspector;
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


  /**
   * @return the useJSONSerialize
   */
  public boolean isUseJSONSerialize() {
    return useJSONSerialize;
  }

  /**
   * @param useJSONSerialize the useJSONSerialize to set
   */
  public void setUseJSONSerialize(boolean useJSONSerialize) {
    this.useJSONSerialize = useJSONSerialize;
  }
}
