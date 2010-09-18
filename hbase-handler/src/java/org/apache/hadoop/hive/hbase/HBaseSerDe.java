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
  private String hbaseColumnsMapping;
  private List<String> hbaseColumnFamilies;
  private List<byte []> hbaseColumnFamiliesBytes;
  private List<String> hbaseColumnQualifiers;
  private List<byte []> hbaseColumnQualifiersBytes;
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
        + hbaseColumnsMapping
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
        + hbaseColumnsMapping);
    }
  }

  /**
   * Parses the HBase columns mapping to identify the column families, qualifiers
   * and also caches the byte arrays corresponding to them. One of the Hive table
   * columns maps to the HBase row key, by default the first column.
   *
   * @param columnMapping - the column mapping specification to be parsed
   * @param colFamilies - the list of HBase column family names
   * @param colFamiliesBytes - the corresponding byte array
   * @param colQualifiers - the list of HBase column qualifier names
   * @param colQualifiersBytes - the corresponding byte array
   * @return the row key index in the column names list
   * @throws SerDeException
   */
  public static int parseColumnMapping(
      String columnMapping,
      List<String> colFamilies,
      List<byte []> colFamiliesBytes,
      List<String> colQualifiers,
      List<byte []> colQualifiersBytes) throws SerDeException {

    int rowKeyIndex = -1;

    if (colFamilies == null || colQualifiers == null) {
      throw new SerDeException("Error: caller must pass in lists for the column families " +
          "and qualifiers.");
    }

    colFamilies.clear();
    colQualifiers.clear();

    if (columnMapping == null) {
      throw new SerDeException("Error: hbase.columns.mapping missing for this HBase table.");
    }

    if (columnMapping.equals("") || columnMapping.equals(HBASE_KEY_COL)) {
      throw new SerDeException("Error: hbase.columns.mapping specifies only the HBase table"
          + " row key. A valid Hive-HBase table must specify at least one additional column.");
    }

    String [] mapping = columnMapping.split(",");

    for (int i = 0; i < mapping.length; i++) {
      String elem = mapping[i];
      int idxFirst = elem.indexOf(":");
      int idxLast = elem.lastIndexOf(":");

      if (idxFirst < 0 || !(idxFirst == idxLast)) {
        throw new SerDeException("Error: the HBase columns mapping contains a badly formed " +
            "column family, column qualifier specification.");
      }

      if (elem.equals(HBASE_KEY_COL)) {
        rowKeyIndex = i;
        colFamilies.add(elem);
        colQualifiers.add(null);
      } else {
        String [] parts = elem.split(":");
        assert(parts.length > 0 && parts.length <= 2);
        colFamilies.add(parts[0]);

        if (parts.length == 2) {
          colQualifiers.add(parts[1]);
        } else {
          colQualifiers.add(null);
        }
      }
    }

    if (rowKeyIndex == -1) {
      colFamilies.add(0, HBASE_KEY_COL);
      colQualifiers.add(0, null);
      rowKeyIndex = 0;
    }

    if (colFamilies.size() != colQualifiers.size()) {
      throw new SerDeException("Error in parsing the hbase columns mapping.");
    }

    // populate the corresponding byte [] if the client has passed in a non-null list
    if (colFamiliesBytes != null) {
      colFamiliesBytes.clear();

      for (String fam : colFamilies) {
        colFamiliesBytes.add(Bytes.toBytes(fam));
      }
    }

    if (colQualifiersBytes != null) {
      colQualifiersBytes.clear();

      for (String qual : colQualifiers) {
        if (qual == null) {
          colQualifiersBytes.add(null);
        } else {
          colQualifiersBytes.add(Bytes.toBytes(qual));
        }
      }
    }

    if (colFamiliesBytes != null && colQualifiersBytes != null) {
      if (colFamiliesBytes.size() != colQualifiersBytes.size()) {
        throw new SerDeException("Error in caching the bytes for the hbase column families " +
            "and qualifiers.");
      }
    }

    return rowKeyIndex;
  }

  public static boolean isSpecialColumn(String hbaseColumnName) {
    return hbaseColumnName.equals(HBASE_KEY_COL);
  }

  private void initHBaseSerDeParameters(
      Configuration job, Properties tbl, String serdeName)
    throws SerDeException {

    // Read configuration parameters
    hbaseColumnsMapping = tbl.getProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING);
    String columnTypeProperty = tbl.getProperty(Constants.LIST_COLUMN_TYPES);

    // Parse the HBase columns mapping and initialize the col family & qualifiers
    hbaseColumnFamilies = new ArrayList<String>();
    hbaseColumnFamiliesBytes = new ArrayList<byte []>();
    hbaseColumnQualifiers = new ArrayList<String>();
    hbaseColumnQualifiersBytes = new ArrayList<byte []>();
    iKey = parseColumnMapping(hbaseColumnsMapping, hbaseColumnFamilies,
        hbaseColumnFamiliesBytes, hbaseColumnQualifiers, hbaseColumnQualifiersBytes);

    // Build the type property string if not supplied
    if (columnTypeProperty == null) {
      StringBuilder sb = new StringBuilder();

      for (int i = 0; i < hbaseColumnFamilies.size(); i++) {
        if (sb.length() > 0) {
          sb.append(":");
        }
        String colFamily = hbaseColumnFamilies.get(i);
        String colQualifier = hbaseColumnQualifiers.get(i);
        if (isSpecialColumn(colFamily)) {
            // the row key column becomes a STRING
            sb.append(Constants.STRING_TYPE_NAME);
        } else if (colQualifier == null)  {
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

    if (hbaseColumnFamilies.size() != serdeParams.getColumnNames().size()) {
      throw new SerDeException(serdeName + ": columns has " +
        serdeParams.getColumnNames().size() +
        " elements while hbase.columns.mapping has " +
        hbaseColumnFamilies.size() + " elements" +
        " (counting the key if implicit)");
    }

    separators = serdeParams.getSeparators();
    escaped = serdeParams.isEscaped();
    escapeChar = serdeParams.getEscapeChar();
    needsEscape = serdeParams.getNeedsEscape();

    // check that the mapping schema is right;
    // check that the "column-family:" is mapped to MAP<String,?>
    for (int i = 0; i < hbaseColumnFamilies.size(); i++) {
      String colFamily = hbaseColumnFamilies.get(i);
      String colQualifier = hbaseColumnQualifiers.get(i);
      if (colQualifier == null && !isSpecialColumn(colFamily)) {
        TypeInfo typeInfo = serdeParams.getColumnTypes().get(i);
        if ((typeInfo.getCategory() != Category.MAP) ||
          (((MapTypeInfo) typeInfo).getMapKeyTypeInfo().getTypeName()
            !=  Constants.STRING_TYPE_NAME)) {

          throw new SerDeException(
            serdeName + ": hbase column family '"
            + colFamily
            + "' should be mapped to Map<String,?> but is mapped to "
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

    cachedHBaseRow.init((Result) result, hbaseColumnFamilies, hbaseColumnFamiliesBytes,
        hbaseColumnQualifiers, hbaseColumnQualifiersBytes);

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
    String hbaseColumnFamily = hbaseColumnFamilies.get(i);
    String hbaseColumnQualifier = hbaseColumnQualifiers.get(i);

    // Get the field objectInspector and the field object.
    ObjectInspector foi = fields.get(i).getFieldObjectInspector();
    Object f = (list == null ? null : list.get(i));

    if (f == null) {
      // a null object, we do not serialize it
      return null;
    }

    // If the field corresponds to a column family in HBase
    if (hbaseColumnQualifier == null && !isSpecialColumn(hbaseColumnFamily)) {
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
          byte [] columnQualifierBytes = new byte[serializeStream.getCount()];
          System.arraycopy(
              serializeStream.getData(), 0, columnQualifierBytes, 0, serializeStream.getCount());

          // Get the Value
          serializeStream.reset();
          boolean isNotNull = serialize(entry.getValue(), voi, 3);
          if (!isNotNull) {
            continue;
          }
          byte [] value = new byte[serializeStream.getCount()];
          System.arraycopy(serializeStream.getData(), 0, value, 0, serializeStream.getCount());
          put.add(hbaseColumnFamiliesBytes.get(i), columnQualifierBytes, value);
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
      put.add(hbaseColumnFamiliesBytes.get(i), hbaseColumnQualifiersBytes.get(i), key);
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

  /**
   * @return 0-based offset of the key column within the table
   */
  int getKeyColumnOffset() {
    return iKey;
  }
}
