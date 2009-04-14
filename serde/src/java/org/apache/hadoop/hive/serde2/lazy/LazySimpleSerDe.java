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

package org.apache.hadoop.hive.serde2.lazy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * LazySimpleSerDe can be used to read the same data format as 
 * MetadataTypedColumnsetSerDe and TCTLSeparatedProtocol.
 * 
 * However, LazySimpleSerDe creates Objects in a lazy way, to 
 * provide better performance.
 * 
 * Also LazySimpleSerDe outputs typed columns instead of treating
 * all columns as String like MetadataTypedColumnsetSerDe.
 */
public class LazySimpleSerDe implements SerDe {

  public static final Log LOG = LogFactory.getLog(
      LazySimpleSerDe.class.getName());

  final public static byte[] DefaultSeparators = {(byte)1, (byte)2, (byte)3};
  // We need some initial values in case user don't call initialize()
  private byte[] separators = DefaultSeparators;

  private String nullString;
  private Text nullSequence;
  private boolean lastColumnTakesRest;
  
  private TypeInfo rowTypeInfo;
  private ObjectInspector cachedObjectInspector;

  public String toString() {
    return getClass().toString() + "[" + Arrays.asList(separators) + ":" 
        + ((StructTypeInfo)rowTypeInfo).getAllStructFieldNames()
        + ":" + ((StructTypeInfo)rowTypeInfo).getAllStructFieldTypeInfos() + "]";
  }

  public LazySimpleSerDe() throws SerDeException {
  }

  /**
   * Return the byte value of the number string.
   * @param altValue   The string containing a number.
   * @param defaultVal If the altValue does not represent a number, 
   *                   return the defaultVal.
   */
  private byte getByte(String altValue, byte defaultVal) {
    if (altValue != null && altValue.length() > 0) {
      try {
        return Byte.valueOf(altValue).byteValue();
      } catch(NumberFormatException e) {
        return (byte)altValue.charAt(0);
      }
    }
    return defaultVal;
  }

  /**
   * Initialize the SerDe given the parameters.
   * serialization.format: separator char or byte code (only supports 
   * byte-value up to 127)
   * columns:  ","-separated column names 
   * columns.types:  ",", ":", or ";"-separated column types
   * @see SerDe#initialize(Configuration, Properties) 
   */
  public void initialize(Configuration job, Properties tbl) 
  throws SerDeException {

    // Read the separators: We use 10 levels of separators by default, but we 
    // should change this when we allow users to specify more than 10 levels 
    // of separators through DDL.
    separators = new byte[10];
    separators[0] = getByte(tbl.getProperty(Constants.FIELD_DELIM, 
                              tbl.getProperty(Constants.SERIALIZATION_FORMAT)),
                              DefaultSeparators[0]);
    separators[1] = getByte(tbl.getProperty(Constants.COLLECTION_DELIM),
        DefaultSeparators[1]);
    separators[2] = getByte(tbl.getProperty(Constants.MAPKEY_DELIM),
        DefaultSeparators[2]);
    for (int i=3; i<separators.length; i++) {
      separators[i] = (byte)(i+1);
    }
    
    nullString = tbl.getProperty(Constants.SERIALIZATION_NULL_FORMAT, "\\N");
    nullSequence = new Text(nullString);
    
    String lastColumnTakesRestString = tbl.getProperty(
        Constants.SERIALIZATION_LAST_COLUMN_TAKES_REST);
    lastColumnTakesRest = (lastColumnTakesRestString != null 
        && lastColumnTakesRestString.equalsIgnoreCase("true"));


    // Read the configuration parameters
    String columnNameProperty = tbl.getProperty("columns");
    // NOTE: if "columns.types" is missing, all columns will be of String type
    String columnTypeProperty = tbl.getProperty("columns.types");
    
    // Parse the configuration parameters
    List<String> columnNames;
    if (columnNameProperty != null && columnNameProperty.length()>0) {
      columnNames = Arrays.asList(columnNameProperty.split(","));
    } else {
      columnNames = new ArrayList<String>();
    }
    if (columnTypeProperty == null) {
      // Default type: all string
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < columnNames.size(); i++) {
        if (i>0) sb.append(":");
        sb.append(Constants.STRING_TYPE_NAME);
      }
      columnTypeProperty = sb.toString();
    }
    
    List<TypeInfo> columnTypes = 
      TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    
    if (columnNames.size() != columnTypes.size()) {
      throw new SerDeException(getClass().toString() 
          + ": columns has " + columnNames.size() 
          + " elements while columns.types has " + columnTypes.size()
          + " elements!");
    }
    
    // Create the LazyObject for storing the rows
    rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    cachedLazyStruct = (LazyStruct)LazyFactory.createLazyObject(rowTypeInfo);

    // Create the ObjectInspectors for the fields
    cachedObjectInspector = LazyFactory.createLazyStructInspector(columnNames,
            columnTypes, separators, nullSequence, lastColumnTakesRest);
    
    
    LOG.debug("LazySimpleSerDe initialized with: columnNames=" + columnNames
        + " columnTypes=" + columnTypes + " separator=" 
        + Arrays.asList(separators) + " nullstring=" + nullString 
        + " lastColumnTakesRest=" + lastColumnTakesRest);
  }
  
  // The object for storing row data
  LazyStruct cachedLazyStruct;
  
  // The wrapper for byte array
  ByteArrayRef byteArrayRef;
  
  /**
   * Deserialize a row from the Writable to a LazyObject.
   * @param field the Writable that contains the data
   * @return  The deserialized row Object.
   * @see SerDe#deserialize(Writable)
   */
  public Object deserialize(Writable field) throws SerDeException {
    if (byteArrayRef == null) {
      byteArrayRef = new ByteArrayRef();
    }
    if (field instanceof BytesWritable) {
      BytesWritable b = (BytesWritable)field;
      // For backward-compatibility with hadoop 0.17
      byteArrayRef.setData(b.get());
      cachedLazyStruct.init(byteArrayRef, 0, b.getSize());
    } else if (field instanceof Text) {
      Text t = (Text)field;
      byteArrayRef.setData(t.getBytes());
      cachedLazyStruct.init(byteArrayRef, 0, t.getLength());
    } else {
      throw new SerDeException(getClass().toString()  
          + ": expects either BytesWritable or Text object!");
    }
    return cachedLazyStruct;
  }
  
  
  /**
   * Returns the ObjectInspector for the row.
   */
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  /**
   * Returns the Writable Class after serialization.
   * @see SerDe#getSerializedClass()
   */
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }
  
  Text serializeCache = new Text();
  ByteStream.Output serializeStream = new ByteStream.Output();
  /**
   * Serialize a row of data.
   * @param obj          The row object
   * @param objInspector The ObjectInspector for the row object
   * @return             The serialized Writable object
   * @throws IOException 
   * @see SerDe#serialize(Object, ObjectInspector)  
   */
  public Writable serialize(Object obj, ObjectInspector objInspector) 
      throws SerDeException {

    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString() 
          + " can only serialize struct types, but we got: " 
          + objInspector.getTypeName());
    }

    // Prepare the field ObjectInspectors
    StructObjectInspector soi = (StructObjectInspector)objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> list = soi.getStructFieldsDataAsList(obj);
    List<? extends StructField> declaredFields = 
        (rowTypeInfo != null && ((StructTypeInfo)rowTypeInfo).getAllStructFieldNames().size()>0)
        ? ((StructObjectInspector)getObjectInspector()).getAllStructFieldRefs()
        : null;
        
    serializeStream.reset();

    try {
      // Serialize each field
      for (int i=0; i<fields.size(); i++) {
        // Append the separator if needed.
        if (i>0) {
          serializeStream.write(separators[0]);
        }
        // Get the field objectInspector and the field object.
        ObjectInspector foi = fields.get(i).getFieldObjectInspector();
        Object f = (list == null ? null : list.get(i));
  
        if (declaredFields != null && i >= declaredFields.size()) {
          throw new SerDeException(
              "Error: expecting " + declaredFields.size() 
              + " but asking for field " + i + "\n" + "data=" + obj + "\n"
              + "tableType=" + rowTypeInfo.toString() + "\n"
              + "dataType=" 
              + TypeInfoUtils.getTypeInfoFromObjectInspector(objInspector));
        }
        
        // If the field that is passed in is NOT a primitive, and either the 
        // field is not declared (no schema was given at initialization), or 
        // the field is declared as a primitive in initialization, serialize 
        // the data to JSON string.  Otherwise serialize the data in the 
        // delimited way.
        if (!foi.getCategory().equals(Category.PRIMITIVE)
            && (declaredFields == null || 
                declaredFields.get(i).getFieldObjectInspector().getCategory()
                .equals(Category.PRIMITIVE))) {
          serialize(serializeStream, SerDeUtils.getJSONString(f, foi), 
              PrimitiveObjectInspectorFactory.javaStringObjectInspector,
              separators, 1, nullSequence);
        } else {
          serialize(serializeStream, f, foi, separators, 1, nullSequence);
        }
      }
    } catch (IOException e) {
      throw new SerDeException(e);
    }
    // TODO: The copy of data is unnecessary, but there is no work-around
    // since we cannot directly set the private byte[] field inside Text.
    serializeCache.set(serializeStream.getData(), 0,
        serializeStream.getCount());
    return serializeCache;
  }

  /**
   * Serialize the row into the StringBuilder.
   * @param out  The StringBuilder to store the serialized data.
   * @param obj The object for the current field.
   * @param objInspector  The ObjectInspector for the current Object.
   * @param separators    The separators array.
   * @param level         The current level of separator.
   * @param nullSequence    The byte sequence representing the NULL value.
   * @throws IOException 
   */
  private void serialize(ByteStream.Output out, Object obj, 
      ObjectInspector objInspector, byte[] separators, int level,
      Text nullSequence) throws IOException {
    
    if (obj == null) {
      out.write(nullSequence.getBytes(), 0, nullSequence.getLength());
      return;
    }
    
    switch (objInspector.getCategory()) {
      case PRIMITIVE: {
        LazyUtils.writePrimitiveUTF8(out, obj, (PrimitiveObjectInspector)objInspector);
        return;
      }
      case LIST: {
        char separator = (char)separators[level];
        ListObjectInspector loi = (ListObjectInspector)objInspector;
        List<?> list = loi.getList(obj);
        ObjectInspector eoi = loi.getListElementObjectInspector();
        if (list == null) {
          out.write(nullSequence.getBytes(), 0, nullSequence.getLength());
        } else {
          for (int i=0; i<list.size(); i++) {
            if (i>0) {
              out.write(separator);
            }
            serialize(out, list.get(i), eoi, separators, level+1,
                nullSequence);
          }
        }
        return;
      }
      case MAP: {
        char separator = (char)separators[level];
        char keyValueSeparator = (char)separators[level+1];
        MapObjectInspector moi = (MapObjectInspector)objInspector;
        ObjectInspector koi = moi.getMapKeyObjectInspector();
        ObjectInspector voi = moi.getMapValueObjectInspector();
        
        Map<?, ?> map = moi.getMap(obj);
        if (map == null) {
          out.write(nullSequence.getBytes(), 0, nullSequence.getLength());
        } else {
          boolean first = true;
          for (Map.Entry<?, ?> entry: map.entrySet()) {
            if (first) {
              first = false;
            } else {
              out.write(separator);
            }
            serialize(out, entry.getKey(), koi, separators, level+2, 
                nullSequence);
            out.write(keyValueSeparator);
            serialize(out, entry.getValue(), voi, separators, level+2, 
                nullSequence);
          }
        }
        return;
      }
      case STRUCT: {
        char separator = (char)separators[level];
        StructObjectInspector soi = (StructObjectInspector)objInspector;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        List<Object> list = soi.getStructFieldsDataAsList(obj);
        if (list == null) {
          out.write(nullSequence.getBytes(), 0, nullSequence.getLength());
        } else {
          for (int i=0; i<list.size(); i++) {
            if (i>0) {
              out.write(separator);
            }
            serialize(out, list.get(i),
                fields.get(i).getFieldObjectInspector(), separators, level+1,
                nullSequence);
          }
        }
        return;
      }
    }
    
    throw new RuntimeException("Unknown category type: "
        + objInspector.getCategory());
  }
}
