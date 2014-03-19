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
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * HBaseSerDe can be used to serialize object into an HBase table and
 * deserialize objects from an HBase table.
 */
public class HBaseSerDe extends AbstractSerDe {
  public static final Log LOG = LogFactory.getLog(HBaseSerDe.class);

  public static final String HBASE_COLUMNS_MAPPING = "hbase.columns.mapping";
  public static final String HBASE_TABLE_NAME = "hbase.table.name";
  public static final String HBASE_TABLE_DEFAULT_STORAGE_TYPE = "hbase.table.default.storage.type";
  public static final String HBASE_KEY_COL = ":key";
  public static final String HBASE_PUT_TIMESTAMP = "hbase.put.timestamp";
  public static final String HBASE_COMPOSITE_KEY_CLASS = "hbase.composite.key.class";
  public static final String HBASE_SCAN_CACHE = "hbase.scan.cache";
  public static final String HBASE_SCAN_CACHEBLOCKS = "hbase.scan.cacheblock";
  public static final String HBASE_SCAN_BATCH = "hbase.scan.batch";
  /**
   *  Determines whether a regex matching should be done on the columns or not. Defaults to true.
   *  <strong>WARNING: Note that currently this only supports the suffix wildcard .*</strong>
   */
  public static final String HBASE_COLUMNS_REGEX_MATCHING = "hbase.columns.mapping.regex.matching";

  private ObjectInspector cachedObjectInspector;
  private LazyHBaseRow cachedHBaseRow;
  private Object compositeKeyObj;

  private HBaseSerDeParameters serdeParams;

  @Override
  public String toString() {
    return getClass().toString()
        + "["
        + serdeParams.getColumnMappingString()
        + ":"
        + serdeParams.getRowTypeInfo().getAllStructFieldNames()
        + ":"
        + serdeParams.getRowTypeInfo().getAllStructFieldTypeInfos() + "]";
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
    serdeParams = new HBaseSerDeParameters();
    serdeParams.init(conf, tbl, getClass().getName());

    cachedObjectInspector = LazyFactory.createLazyStructInspector(
        serdeParams.getColumnNames(),
        serdeParams.getColumnTypes(),
        serdeParams.getSeparators(),
        serdeParams.getNullSequence(),
        serdeParams.isLastColumnTakesRest(),
        serdeParams.isEscaped(),
        serdeParams.getEscapeChar());

    cachedHBaseRow = new LazyHBaseRow((LazySimpleStructObjectInspector) cachedObjectInspector);

    if (serdeParams.getCompositeKeyClass() != null) {
      // initialize the constructor of the composite key class with its object inspector
      initCompositeKeyClass(conf,tbl);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("HBaseSerDe initialized with : columnNames = "
        + serdeParams.getColumnNames()
        + " columnTypes = "
        + serdeParams.getColumnTypes()
        + " hbaseColumnMapping = "
        + serdeParams.getColumnMappingString());
    }
  }

  /**
   * Parses the HBase columns mapping specifier to identify the column families, qualifiers
   * and also caches the byte arrays corresponding to them. One of the Hive table
   * columns maps to the HBase row key, by default the first column.
   *
   * @param columnsMappingSpec string hbase.columns.mapping specified when creating table
   * @param doColumnRegexMatching whether to do a regex matching on the columns or not
   * @return List<ColumnMapping> which contains the column mapping information by position
   * @throws SerDeException
   */
  public static List<ColumnMapping> parseColumnsMapping(String columnsMappingSpec, boolean doColumnRegexMatching)
      throws SerDeException {

    if (columnsMappingSpec == null) {
      throw new SerDeException("Error: hbase.columns.mapping missing for this HBase table.");
    }

    if (columnsMappingSpec.equals("") || columnsMappingSpec.equals(HBASE_KEY_COL)) {
      throw new SerDeException("Error: hbase.columns.mapping specifies only the HBase table"
          + " row key. A valid Hive-HBase table must specify at least one additional column.");
    }

    int rowKeyIndex = -1;
    List<ColumnMapping> columnsMapping = new ArrayList<ColumnMapping>();
    String [] columnSpecs = columnsMappingSpec.split(",");
    ColumnMapping columnMapping = null;

    for (int i = 0; i < columnSpecs.length; i++) {
      String mappingSpec = columnSpecs[i].trim();
      String [] mapInfo = mappingSpec.split("#");
      String colInfo = mapInfo[0];

      int idxFirst = colInfo.indexOf(":");
      int idxLast = colInfo.lastIndexOf(":");

      if (idxFirst < 0 || !(idxFirst == idxLast)) {
        throw new SerDeException("Error: the HBase columns mapping contains a badly formed " +
            "column family, column qualifier specification.");
      }

      columnMapping = new ColumnMapping();

      if (colInfo.equals(HBASE_KEY_COL)) {
        rowKeyIndex = i;
        columnMapping.familyName = colInfo;
        columnMapping.familyNameBytes = Bytes.toBytes(colInfo);
        columnMapping.qualifierName = null;
        columnMapping.qualifierNameBytes = null;
        columnMapping.hbaseRowKey = true;
      } else {
        String [] parts = colInfo.split(":");
        assert(parts.length > 0 && parts.length <= 2);
        columnMapping.familyName = parts[0];
        columnMapping.familyNameBytes = Bytes.toBytes(parts[0]);
        columnMapping.hbaseRowKey = false;

        if (parts.length == 2) {

          if (doColumnRegexMatching && parts[1].endsWith(".*")) {
            // we have a prefix with a wildcard
            columnMapping.qualifierPrefix = parts[1].substring(0, parts[1].length() - 2);
            columnMapping.qualifierPrefixBytes = Bytes.toBytes(columnMapping.qualifierPrefix);
            // we weren't provided any actual qualifier name. Set these to
            // null.
            columnMapping.qualifierName = null;
            columnMapping.qualifierNameBytes = null;
          } else {
            // set the regular provided qualifier names
            columnMapping.qualifierName = parts[1];
            columnMapping.qualifierNameBytes = Bytes.toBytes(parts[1]);
            ;
          }
        } else {
          columnMapping.qualifierName = null;
          columnMapping.qualifierNameBytes = null;
        }
      }

      columnMapping.mappingSpec = mappingSpec;

      columnsMapping.add(columnMapping);
    }

    if (rowKeyIndex == -1) {
      columnMapping = new ColumnMapping();
      columnMapping.familyName = HBASE_KEY_COL;
      columnMapping.familyNameBytes = Bytes.toBytes(HBASE_KEY_COL);
      columnMapping.qualifierName = null;
      columnMapping.qualifierNameBytes = null;
      columnMapping.hbaseRowKey = true;
      columnMapping.mappingSpec = HBASE_KEY_COL;
      columnsMapping.add(0, columnMapping);
    }

    return columnsMapping;
  }

  static class ColumnMapping {

    ColumnMapping() {
      binaryStorage = new ArrayList<Boolean>(2);
    }

    String familyName;
    String qualifierName;
    byte [] familyNameBytes;
    byte [] qualifierNameBytes;
    List<Boolean> binaryStorage;
    boolean hbaseRowKey;
    String mappingSpec;
    String qualifierPrefix;
    byte[] qualifierPrefixBytes;
  }

  /**
   * Deserialize a row from the HBase Result writable to a LazyObject
   * @param result the HBase Result Writable containing the row
   * @return the deserialized object
   * @see SerDe#deserialize(Writable)
   */
  @Override
  public Object deserialize(Writable result) throws SerDeException {
    if (!(result instanceof ResultWritable)) {
      throw new SerDeException(getClass().getName() + ": expects ResultWritable!");
    }

    cachedHBaseRow.init(((ResultWritable) result).getResult(), serdeParams.getColumnMapping(),
        compositeKeyObj);
    return cachedHBaseRow;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return PutWritable.class;
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
        ((StructObjectInspector)getObjectInspector()).getAllStructFieldRefs();

    int iKey = serdeParams.getKeyIndex();
    StructField field = fields.get(iKey);
    Object value = list.get(iKey);
    StructField declaredField = declaredFields.get(iKey);
    byte[] key;
    try {
      key = serializeKeyField(field, value, declaredField, serdeParams);
    } catch (IOException ex) {
      throw new SerDeException(ex);
    }

    if (key == null) {
      throw new SerDeException("HBase row key cannot be NULL");
    }

    Put put = null;
    long putTimestamp = serdeParams.getPutTimestamp();
    if(putTimestamp >= 0) {
      put = new Put(key, putTimestamp);
    } else {
      put = new Put(key);
    }

    try {
      // Serialize each field
      for (int i = 0; i < fields.size(); i++) {
        if (i == iKey) {
          continue;
        }

        field = fields.get(i);
        value = list.get(i);
        declaredField = declaredFields.get(i);
        ColumnMapping colMap = serdeParams.getColumnMapping().get(i);
        serializeField(put, field, value, declaredField, colMap);
      }
    } catch (IOException e) {
      throw new SerDeException(e);
    }

    return new PutWritable(put);
  }

  private static byte[] serializeKeyField(StructField keyField, Object keyValue,
      StructField declaredKeyField, HBaseSerDeParameters serdeParams) throws IOException {
	  if (keyValue == null) {
		  // a null object, we do not serialize it
		  return null;
	  }

    boolean writeBinary = serdeParams.getKeyColumnMapping().binaryStorage.get(0);
	  ObjectInspector keyFieldOI = keyField.getFieldObjectInspector();

	  if (!keyFieldOI.getCategory().equals(Category.PRIMITIVE) &&
			  declaredKeyField.getFieldObjectInspector().getCategory().equals(Category.PRIMITIVE)) {
		  // we always serialize the String type using the escaped algorithm for LazyString
	    return serialize(SerDeUtils.getJSONString(keyValue, keyFieldOI),
	        PrimitiveObjectInspectorFactory.javaStringObjectInspector, 1, false, serdeParams);
	  } else {
		  // use the serialization option switch to write primitive values as either a variable
		  // length UTF8 string or a fixed width bytes if serializing in binary format
	    return serialize(keyValue, keyFieldOI, 1, writeBinary, serdeParams);
	  }

  }

  private void serializeField(Put put, StructField field, Object value,
    StructField declaredField, ColumnMapping colMap) throws IOException {
    if (value == null) {
      // a null object, we do not serialize it
      return;
    }

    // Get the field objectInspector and the field object.
    ObjectInspector foi = field.getFieldObjectInspector();

    // If the field corresponds to a column family in HBase
    if (colMap.qualifierName == null) {
      MapObjectInspector moi = (MapObjectInspector) foi;
      ObjectInspector koi = moi.getMapKeyObjectInspector();
      ObjectInspector voi = moi.getMapValueObjectInspector();

      Map<?, ?> map = moi.getMap(value);
      if (map == null) {
        return;
      } else {
        for (Map.Entry<?, ?> entry: map.entrySet()) {
          // Get the Key
          // Map keys are required to be primitive and may be serialized in binary format
          byte[] columnQualifierBytes = serialize(entry.getKey(), koi, 3, colMap.binaryStorage.get(0), serdeParams);
          if (columnQualifierBytes == null) {
            continue;
          }

          // Map values may be serialized in binary format when they are primitive and binary
          // serialization is the option selected
          byte[] bytes = serialize(entry.getValue(), voi, 3, colMap.binaryStorage.get(1), serdeParams);
          if (bytes == null) {
            continue;
          }

          put.add(colMap.familyNameBytes, columnQualifierBytes, bytes);
        }
      }
    } else {
      byte[] bytes = null;
      // If the field that is passed in is NOT a primitive, and either the
      // field is not declared (no schema was given at initialization), or
      // the field is declared as a primitive in initialization, serialize
      // the data to JSON string.  Otherwise serialize the data in the
      // delimited way.
      if (!foi.getCategory().equals(Category.PRIMITIVE)
          && declaredField.getFieldObjectInspector().getCategory().equals(Category.PRIMITIVE)) {
        // we always serialize the String type using the escaped algorithm for LazyString
        bytes = serialize(SerDeUtils.getJSONString(value, foi),
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, 1, false, serdeParams);
      } else {
        // use the serialization option switch to write primitive values as either a variable
        // length UTF8 string or a fixed width bytes if serializing in binary format
        bytes = serialize(value, foi, 1, colMap.binaryStorage.get(0), serdeParams);
      }

      if (bytes == null) {
        return;
      }

      put.add(colMap.familyNameBytes, colMap.qualifierNameBytes, bytes);
    }
  }

  private static byte[] getBytesFromStream(ByteStream.Output ss) {
    byte [] buf = new byte[ss.getCount()];
    System.arraycopy(ss.getData(), 0, buf, 0, ss.getCount());
    return buf;
  }

  /*
   * Serialize the row into a ByteStream.
   *
   * @param obj           The object for the current field.
   * @param objInspector  The ObjectInspector for the current Object.
   * @param level         The current level of separator.
   * @param writeBinary   Whether to write a primitive object as an UTF8 variable length string or
   *                      as a fixed width byte array onto the byte stream.
   * @throws IOException  On error in writing to the serialization stream.
   * @return true         On serializing a non-null object, otherwise false.
   */
  private static byte[] serialize(Object obj, ObjectInspector objInspector, int level,
      boolean writeBinary, HBaseSerDeParameters serdeParams) throws IOException {
    ByteStream.Output ss = new ByteStream.Output();
    if (objInspector.getCategory() == Category.PRIMITIVE && writeBinary) {
      LazyUtils.writePrimitive(ss, obj, (PrimitiveObjectInspector) objInspector);
    } else {
      if (false == serialize(obj, objInspector, level, serdeParams, ss)) {
        return null;
      }
    }

    return getBytesFromStream(ss);
  }

  private static boolean serialize(
      Object obj,
      ObjectInspector objInspector,
      int level, HBaseSerDeParameters serdeParams, ByteStream.Output ss) throws IOException {

    byte[] separators = serdeParams.getSeparators();
    boolean escaped = serdeParams.isEscaped();
    byte escapeChar = serdeParams.getEscapeChar();
    boolean[] needsEscape = serdeParams.getNeedsEscape();
    switch (objInspector.getCategory()) {
      case PRIMITIVE:
        LazyUtils.writePrimitiveUTF8(ss, obj,
            (PrimitiveObjectInspector) objInspector, escaped, escapeChar, needsEscape);
        return true;
      case LIST:
        char separator = (char) separators[level];
        ListObjectInspector loi = (ListObjectInspector)objInspector;
        List<?> list = loi.getList(obj);
        ObjectInspector eoi = loi.getListElementObjectInspector();
        if (list == null) {
          return false;
        } else {
          for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
              ss.write(separator);
            }
            serialize(list.get(i), eoi, level + 1, serdeParams, ss);
          }
        }
        return true;
      case MAP:
        char sep = (char) separators[level];
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
              ss.write(sep);
            }
            serialize(entry.getKey(), koi, level+2, serdeParams, ss);
            ss.write(keyValueSeparator);
            serialize(entry.getValue(), voi, level+2, serdeParams, ss);
          }
        }
        return true;
      case STRUCT:
        sep = (char)separators[level];
        StructObjectInspector soi = (StructObjectInspector)objInspector;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        list = soi.getStructFieldsDataAsList(obj);
        if (list == null) {
          return false;
        } else {
          for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
              ss.write(sep);
            }

            serialize(list.get(i), fields.get(i).getFieldObjectInspector(),
                level + 1, serdeParams, ss);
          }
        }
        return true;
      default:
        throw new RuntimeException("Unknown category type: " + objInspector.getCategory());
    }
  }

  /**
   * Initialize the composite key class with the objectinspector for the key
   *
   * @throws SerDeException
   * */
  private void initCompositeKeyClass(Configuration conf,Properties tbl) throws SerDeException {

    int i = 0;

    // find the hbase row key
    for (ColumnMapping colMap : serdeParams.getColumnMapping()) {
      if (colMap.hbaseRowKey) {
        break;
      }
      i++;
    }

    ObjectInspector keyObjectInspector = ((LazySimpleStructObjectInspector) cachedObjectInspector)
        .getAllStructFieldRefs().get(i).getFieldObjectInspector();

    try {
      compositeKeyObj = serdeParams.getCompositeKeyClass().getDeclaredConstructor(
            LazySimpleStructObjectInspector.class, Properties.class, Configuration.class)
            .newInstance(
                ((LazySimpleStructObjectInspector) keyObjectInspector), tbl, conf);
    } catch (IllegalArgumentException e) {
      throw new SerDeException(e);
    } catch (SecurityException e) {
      throw new SerDeException(e);
    } catch (InstantiationException e) {
      throw new SerDeException(e);
    } catch (IllegalAccessException e) {
      throw new SerDeException(e);
    } catch (InvocationTargetException e) {
      throw new SerDeException(e);
    } catch (NoSuchMethodException e) {
      // the constructor wasn't defined in the implementation class. Flag error
      throw new SerDeException("Constructor not defined in composite key class ["
          + serdeParams.getCompositeKeyClass().getName() + "]", e);
    }
  }

  /**
   * @return 0-based offset of the key column within the table
   */
  int getKeyColumnOffset() {
    return serdeParams.getKeyIndex();
  }

  List<Boolean> getStorageFormatOfCol(int colPos){
    return serdeParams.getColumnMapping().get(colPos).binaryStorage;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }

  public static int getRowKeyColumnOffset(List<ColumnMapping> columnsMapping)
      throws SerDeException {

    for (int i = 0; i < columnsMapping.size(); i++) {
      ColumnMapping colMap = columnsMapping.get(i);

      if (colMap.hbaseRowKey && colMap.familyName.equals(HBASE_KEY_COL)) {
        return i;
      }
    }

    throw new SerDeException("HBaseSerDe Error: columns mapping list does not contain" +
      " row key column.");
  }
}
