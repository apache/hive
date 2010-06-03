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
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.RowResult;
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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * HBaseSerDe can be used to serialize object into an HBase table and
 * deserialize objects from an HBase table.
 */
public class HBaseSerDe implements SerDe {
  
  public static final String HBASE_COL_MAPPING = "hbase.columns.mapping";
  
  public static final String HBASE_TABLE_NAME = "hbase.table.name";
  
  public static final String HBASE_KEY_COL = ":key";
  
  public static final Log LOG = LogFactory.getLog(
      HBaseSerDe.class.getName());

  private ObjectInspector cachedObjectInspector;
  private List<String> hbaseColumnNames;
  private SerDeParameters serdeParams;
  private boolean useJSONSerialize;
  private LazyHBaseRow cachedHBaseRow;
  private ByteStream.Output serializeStream = new ByteStream.Output();
  private int iKey;

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
  
  public static boolean isSpecialColumn(String hbaseColumnName) {
    return hbaseColumnName.equals(HBASE_KEY_COL);
  }
  
  private void initHBaseSerDeParameters(
      Configuration job, Properties tbl, String serdeName) 
    throws SerDeException {

    // Read configuration parameters
    String hbaseColumnNameProperty =
      tbl.getProperty(HBaseSerDe.HBASE_COL_MAPPING);
    String columnTypeProperty =
      tbl.getProperty(Constants.LIST_COLUMN_TYPES);
    
    // Initialize the hbase column list
    hbaseColumnNames = parseColumnMapping(hbaseColumnNameProperty);
    iKey = hbaseColumnNames.indexOf(HBASE_KEY_COL);
      
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
    
    serdeParams = LazySimpleSerDe.initSerdeParams(
      job, tbl, serdeName);
    
    if (hbaseColumnNames.size() != serdeParams.getColumnNames().size()) {
      throw new SerDeException(serdeName + ": columns has " + 
        serdeParams.getColumnNames().size() + 
        " elements while hbase.columns.mapping has " + 
        hbaseColumnNames.size() + " elements" +
        " (counting the key if implicit)");
    }
    
    // check that the mapping schema is right;
    // we just can make sure that "columnfamily:" is mapped to MAP<String,?> 
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
   * Deserialize a row from the HBase RowResult writable to a LazyObject
   * @param rowResult the HBase RowResult Writable contain a row
   * @return the deserialized object
   * @see SerDe#deserialize(Writable) 
   */
  public Object deserialize(Writable rowResult) throws SerDeException {
    
    if (!(rowResult instanceof RowResult)) {
      throw new SerDeException(getClass().getName() + ": expects RowResult!");
    }
    
    RowResult rr = (RowResult)rowResult;
    cachedHBaseRow.init(rr, hbaseColumnNames);
    return cachedHBaseRow;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return BatchUpdate.class;
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
        
    BatchUpdate batchUpdate;

    try {
      byte [] key =
        serializeField(
          iKey, HBASE_KEY_COL, null, fields, list, declaredFields);
      if (key == null) {
        throw new SerDeException("HBase row key cannot be NULL");
      }
      batchUpdate = new BatchUpdate(key);
      // Serialize each field
      for (int i = 0; i < fields.size(); i++) {
        if (i == iKey) {
          // already processed the key above
          continue;
        }
        String hbaseColumn = hbaseColumnNames.get(i);
        serializeField(
          i, hbaseColumn, batchUpdate, fields, list, declaredFields);
      }
    } catch (IOException e) {
      throw new SerDeException(e);
    }
    
    return batchUpdate;
  }

  private byte [] serializeField(
    int i, String hbaseColumn, BatchUpdate batchUpdate,
    List<? extends StructField> fields,
    List<Object> list,
    List<? extends StructField> declaredFields) throws IOException {

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
          serialize(serializeStream, entry.getKey(), koi, 
            serdeParams.getSeparators(), 3,
            serdeParams.getNullSequence(),
            serdeParams.isEscaped(),
            serdeParams.getEscapeChar(),
            serdeParams.getNeedsEscape());
              
          // generate a column name (column_family:column_name)
          String hbaseSparseColumn =
            hbaseColumn + Bytes.toString(
              serializeStream.getData(), 0, serializeStream.getCount());

          // Get the Value
          serializeStream.reset();

          boolean isNotNull = serialize(serializeStream, entry.getValue(), voi, 
            serdeParams.getSeparators(), 3,
            serdeParams.getNullSequence(),
            serdeParams.isEscaped(),
            serdeParams.getEscapeChar(),
            serdeParams.getNeedsEscape());
          if (!isNotNull) {
            continue;
          }
          byte [] key = new byte[serializeStream.getCount()];
          System.arraycopy(
            serializeStream.getData(), 0, key, 0, serializeStream.getCount());
          batchUpdate.put(hbaseSparseColumn, key);
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
          serializeStream, SerDeUtils.getJSONString(f, foi),
          PrimitiveObjectInspectorFactory.javaStringObjectInspector,
          serdeParams.getSeparators(), 1,
          serdeParams.getNullSequence(),
          serdeParams.isEscaped(),
          serdeParams.getEscapeChar(),
          serdeParams.getNeedsEscape());
      } else {
        isNotNull = serialize(
          serializeStream, f, foi, 
          serdeParams.getSeparators(), 1,
          serdeParams.getNullSequence(),
          serdeParams.isEscaped(),
          serdeParams.getEscapeChar(),
          serdeParams.getNeedsEscape());
      }
      if (!isNotNull) {
        return null;
      }
      byte [] key = new byte[serializeStream.getCount()];
      System.arraycopy(
        serializeStream.getData(), 0, key, 0, serializeStream.getCount());
      if (hbaseColumn.equals(HBASE_KEY_COL)) {
        return key;
      }
      batchUpdate.put(hbaseColumn, key);
    }

    return null;
  }
  
  /**
   * Serialize the row into a ByteStream.
   *
   * @param out  The ByteStream.Output to store the serialized data.
   * @param obj The object for the current field.
   * @param objInspector  The ObjectInspector for the current Object.
   * @param separators    The separators array.
   * @param level         The current level of separator.
   * @param nullSequence  The byte sequence representing the NULL value.
   * @param escaped       Whether we need to escape the data when writing out
   * @param escapeChar    Which char to use as the escape char, e.g. '\\'     
   * @param needsEscape   Which chars needs to be escaped.
   *                      This array should have size of 128.
   *                      Negative byte values (or byte values >= 128)
   *                      are never escaped.
   * @throws IOException 
   * @return true, if serialize a not-null object; otherwise false.
   */
  public static boolean serialize(ByteStream.Output out, Object obj, 
    ObjectInspector objInspector, byte[] separators, int level,
    Text nullSequence, boolean escaped, byte escapeChar,
    boolean[] needsEscape) throws IOException {
    
    switch (objInspector.getCategory()) {
      case PRIMITIVE: {
        LazyUtils.writePrimitiveUTF8(
          out, obj,
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
              out.write(separator);
            }
            serialize(out, list.get(i), eoi, separators, level + 1,
                nullSequence, escaped, escapeChar, needsEscape);
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
              out.write(separator);
            }
            serialize(out, entry.getKey(), koi, separators, level+2, 
                nullSequence, escaped, escapeChar, needsEscape);
            out.write(keyValueSeparator);
            serialize(out, entry.getValue(), voi, separators, level+2, 
                nullSequence, escaped, escapeChar, needsEscape);
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
              out.write(separator);
            }
            serialize(out, list.get(i),
                fields.get(i).getFieldObjectInspector(), separators, level + 1,
                nullSequence, escaped, escapeChar, needsEscape);
          }
        }
        return true;
      }
    }
    
    throw new RuntimeException("Unknown category type: "
        + objInspector.getCategory());
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
