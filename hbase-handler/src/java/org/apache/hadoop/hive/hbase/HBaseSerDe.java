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
  
  public static final Log LOG = LogFactory.getLog(
      HBaseSerDe.class.getName());

  private ObjectInspector cachedObjectInspector;
  private HBaseSerDeParameters hbSerDeParams;
  private boolean useJSONSerialize;
  private LazyHBaseRow cachedHBaseRow;
  private BatchUpdate serializeCache;
  private ByteStream.Output serializeStream = new ByteStream.Output();

  /**
   * HBaseSerDeParameters defines the parameters used to
   * instantiate HBaseSerDe.
   */
  public static class HBaseSerDeParameters {
    private List<String> hbaseColumnNames;
    private SerDeParameters serdeParams;
    
    public List<String> getHBaseColumnNames() {
      return hbaseColumnNames;
    }
    
    public SerDeParameters getSerDeParameters() {
      return serdeParams;
    }
  }
  
  public String toString() {
    return getClass().toString()
        + "["
        + hbSerDeParams.hbaseColumnNames
        + ":"
        + ((StructTypeInfo) hbSerDeParams.serdeParams.getRowTypeInfo())
            .getAllStructFieldNames()
        + ":"
        + ((StructTypeInfo) hbSerDeParams.serdeParams.getRowTypeInfo())
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
    hbSerDeParams = HBaseSerDe.initHBaseSerDeParameters(conf, tbl, 
        getClass().getName());
    
    // We just used columnNames & columnTypes these two parameters
    cachedObjectInspector = LazyFactory.createLazyStructInspector(
        hbSerDeParams.serdeParams.getColumnNames(), 
        hbSerDeParams.serdeParams.getColumnTypes(), 
        hbSerDeParams.serdeParams.getSeparators(),
        hbSerDeParams.serdeParams.getNullSequence(),
        hbSerDeParams.serdeParams.isLastColumnTakesRest(),
        hbSerDeParams.serdeParams.isEscaped(),
        hbSerDeParams.serdeParams.getEscapeChar()); 
    
    cachedHBaseRow = new LazyHBaseRow(
      (LazySimpleStructObjectInspector) cachedObjectInspector);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("HBaseSerDe initialized with : columnNames = "
        + hbSerDeParams.serdeParams.getColumnNames()
        + " columnTypes = "
        + hbSerDeParams.serdeParams.getColumnTypes()
        + " hbaseColumnMapping = "
        + hbSerDeParams.hbaseColumnNames);
    }
  }
  
  public static HBaseSerDeParameters initHBaseSerDeParameters(
      Configuration job, Properties tbl, String serdeName) 
    throws SerDeException {

    HBaseSerDeParameters serdeParams = new HBaseSerDeParameters();
    
    // Read Configuration Parameter
    String hbaseColumnNameProperty =
      tbl.getProperty(HBaseSerDe.HBASE_COL_MAPPING);
    String columnTypeProperty =
      tbl.getProperty(Constants.LIST_COLUMN_TYPES);
    
    // Initial the hbase column list
    if (hbaseColumnNameProperty != null
      && hbaseColumnNameProperty.length() > 0) {

      serdeParams.hbaseColumnNames =
        Arrays.asList(hbaseColumnNameProperty.split(","));
    } else {
      serdeParams.hbaseColumnNames = new ArrayList<String>();
    }
    
    // Add the hbase key to the columnNameList and columnTypeList
    
    // Build the type property string
    if (columnTypeProperty == null) {
      StringBuilder sb = new StringBuilder();
      sb.append(Constants.STRING_TYPE_NAME);
      
      for (int i = 0; i < serdeParams.hbaseColumnNames.size(); i++) {
        String colName = serdeParams.hbaseColumnNames.get(i);
        if (colName.endsWith(":"))  {
          sb.append(":").append(
            Constants.MAP_TYPE_NAME + "<"
            + Constants.STRING_TYPE_NAME
            + "," + Constants.STRING_TYPE_NAME + ">");
        } else {
          sb.append(":").append(Constants.STRING_TYPE_NAME);
        }
      }
      tbl.setProperty(Constants.LIST_COLUMN_TYPES, sb.toString());
    }
    
    serdeParams.serdeParams = LazySimpleSerDe.initSerdeParams(
      job, tbl, serdeName);
    
    if (serdeParams.hbaseColumnNames.size() + 1
      != serdeParams.serdeParams.getColumnNames().size()) {

      throw new SerDeException(serdeName + ": columns has " + 
          serdeParams.serdeParams.getColumnNames().size() + 
          " elements while hbase.columns.mapping has " + 
          serdeParams.hbaseColumnNames.size() + " elements!");
    }
    
    // check that the mapping schema is right;
    // we just can make sure that "columnfamily:" is mapped to MAP<String,?> 
    for (int i = 0; i < serdeParams.hbaseColumnNames.size(); i++) {
      String hbaseColName = serdeParams.hbaseColumnNames.get(i);
      if (hbaseColName.endsWith(":")) {    
        TypeInfo typeInfo = serdeParams.serdeParams.getColumnTypes().get(i + 1);
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
    
    return serdeParams;
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
    cachedHBaseRow.init(rr, hbSerDeParams.hbaseColumnNames);
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
      (hbSerDeParams.serdeParams.getRowTypeInfo() != null && 
        ((StructTypeInfo) hbSerDeParams.serdeParams.getRowTypeInfo())
        .getAllStructFieldNames().size() > 0) ? 
      ((StructObjectInspector)getObjectInspector()).getAllStructFieldRefs()
      : null;
        
    boolean isNotNull = false;
    String hbaseColumn = "";

    try {
      // Serialize each field
      for (int i = 0; i < fields.size(); i++) {
        serializeStream.reset();
        // Get the field objectInspector and the field object.
        ObjectInspector foi = fields.get(i).getFieldObjectInspector();
        Object f = (list == null ? null : list.get(i));

        if (declaredFields != null && i >= declaredFields.size()) {
          throw new SerDeException(
              "Error: expecting " + declaredFields.size() 
              + " but asking for field " + i + "\n" + "data=" + obj + "\n"
              + "tableType="
              + hbSerDeParams.serdeParams.getRowTypeInfo().toString()
              + "\n"
              + "dataType=" 
              + TypeInfoUtils.getTypeInfoFromObjectInspector(objInspector));
        }
        
        if (f == null) {
          // a null object, we do not serialize it
          continue;
        }
        
        if (i > 0) {
          hbaseColumn = hbSerDeParams.hbaseColumnNames.get(i-1);
        }
        
        // If the field that is column family in hbase
        if (i > 0 && hbaseColumn.endsWith(":")) {
          MapObjectInspector moi = (MapObjectInspector)foi;
          ObjectInspector koi = moi.getMapKeyObjectInspector();
          ObjectInspector voi = moi.getMapValueObjectInspector();

          Map<?, ?> map = moi.getMap(f);
          if (map == null) {
            continue;
          } else {
            for (Map.Entry<?, ?> entry: map.entrySet()) {
              // Get the Key
              serialize(serializeStream, entry.getKey(), koi, 
                  hbSerDeParams.serdeParams.getSeparators(), 3,
                  hbSerDeParams.serdeParams.getNullSequence(),
                  hbSerDeParams.serdeParams.isEscaped(),
                  hbSerDeParams.serdeParams.getEscapeChar(),
                  hbSerDeParams.serdeParams.getNeedsEscape());
              
              // generate a column name (column_family:column_name)
              hbaseColumn += Bytes.toString(
                serializeStream.getData(), 0, serializeStream.getCount());

              // Get the Value
              serializeStream.reset();

              isNotNull = serialize(serializeStream, entry.getValue(), voi, 
                  hbSerDeParams.serdeParams.getSeparators(), 3,
                  hbSerDeParams.serdeParams.getNullSequence(),
                  hbSerDeParams.serdeParams.isEscaped(),
                  hbSerDeParams.serdeParams.getEscapeChar(),
                  hbSerDeParams.serdeParams.getNeedsEscape());
            }
          }
        } else {        
          // If the field that is passed in is NOT a primitive, and either the 
          // field is not declared (no schema was given at initialization), or 
          // the field is declared as a primitive in initialization, serialize 
          // the data to JSON string.  Otherwise serialize the data in the 
          // delimited way.
          if (!foi.getCategory().equals(Category.PRIMITIVE)
              && (declaredFields == null || 
                  declaredFields.get(i).getFieldObjectInspector().getCategory()
                  .equals(Category.PRIMITIVE) || useJSONSerialize)) {
            isNotNull = serialize(
              serializeStream, SerDeUtils.getJSONString(f, foi),
              PrimitiveObjectInspectorFactory.javaStringObjectInspector,
              hbSerDeParams.serdeParams.getSeparators(), 1,
              hbSerDeParams.serdeParams.getNullSequence(),
              hbSerDeParams.serdeParams.isEscaped(),
              hbSerDeParams.serdeParams.getEscapeChar(),
              hbSerDeParams.serdeParams.getNeedsEscape());
          } else {
            isNotNull = serialize(
              serializeStream, f, foi, 
              hbSerDeParams.serdeParams.getSeparators(), 1,
              hbSerDeParams.serdeParams.getNullSequence(),
              hbSerDeParams.serdeParams.isEscaped(),
              hbSerDeParams.serdeParams.getEscapeChar(),
              hbSerDeParams.serdeParams.getNeedsEscape());
          }
        }
        
        byte [] key = new byte[serializeStream.getCount()];
        System.arraycopy(
          serializeStream.getData(), 0, key, 0, serializeStream.getCount());
        if (i == 0) {
          // the first column is the hbase key
          serializeCache = new BatchUpdate(key);
        } else {
          if (isNotNull) {
            serializeCache.put(hbaseColumn, key);
          }
        }
      }
    } catch (IOException e) {
      throw new SerDeException(e);
    }
    
    return serializeCache;
  }
  
  /**
   * Serialize the row into the StringBuilder.
   * @param out  The StringBuilder to store the serialized data.
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
          for (int i = 0; i<list.size(); i++) {
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
