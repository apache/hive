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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
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

  public static final Log LOG = LogFactory.getLog(LazySimpleSerDe.class.getName());

  final public static byte DefaultSeparator = 1;
  private byte separator;


  private List<String> columnNames;
  private List<String> columnTypes;
  private ObjectInspector cachedObjectInspector;

  private String nullString;
  private boolean lastColumnTakesRest;
  
  public String toString() {
    return getClass().toString() + "[" + separator + ":" 
        + columnNames + ":" + columnTypes + "]";
  }

  public LazySimpleSerDe() throws SerDeException {
    separator = DefaultSeparator;
  }

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
   * serialization.format: separator char or byte code (only supports byte-value up to 127)
   * columns:  ,-separated column naems 
   * columns.types:  :-separated column types 
   */
  public void initialize(Configuration job, Properties tbl) throws SerDeException {
    // Read the separator
    String alt_sep = tbl.getProperty(Constants.SERIALIZATION_FORMAT);
    separator = getByte(alt_sep, DefaultSeparator);

    // Read the configuration parameters
    String columnNameProperty = tbl.getProperty("columns");
    // NOTE: if "columns.types" is missing, all columns will be of String type
    String columnTypeProperty = tbl.getProperty("columns.types");
    
    nullString = tbl.getProperty(Constants.SERIALIZATION_NULL_FORMAT);
    if (nullString == null) {
      nullString = "\\N";
    }
    
    String lastColumnTakesRestString = tbl.getProperty(Constants.SERIALIZATION_LAST_COLUMN_TAKES_REST);
    lastColumnTakesRest = (lastColumnTakesRestString != null && lastColumnTakesRestString.equalsIgnoreCase("true"));
    
    // Parse the configuration parameters
    if (columnNameProperty != null) {
      columnNames = Arrays.asList(columnNameProperty.split(","));
    } else {
      columnNames = new ArrayList<String>();
    }
    if (columnTypeProperty != null) {
      columnTypes = Arrays.asList(columnTypeProperty.split(":"));
    } else {
      // Default type: all string
      columnTypes = new ArrayList<String>();
      for (int i = 0; i < columnNames.size(); i++) {
        columnTypes.add(Constants.STRING_TYPE_NAME);
      }
    }
    if (columnNames.size() != columnTypes.size()) {
      throw new SerDeException(getClass().toString() 
          + ": columns has " + columnNames.size() 
          + " elements while columns.types has " + columnTypes.size() + " elements!");
    }
    
    // Create the LazyObject for storing the rows
    LazyObject[] lazyPrimitives = new LazyObject[columnNames.size()];
    // Create the ObjectInspectors for the fields
    ArrayList<ObjectInspector> columnObjectInspectors
        = new ArrayList<ObjectInspector>(columnNames.size());  
    for (int i=0; i<columnTypes.size(); i++) {
      Class<?> primitiveClass = ObjectInspectorUtils.typeNameToClass.get( columnTypes.get(i) );
      if (primitiveClass == null) {
        throw new SerDeException(getClass().toString() 
            + ": type " + columnTypes.get(i) + " not supported!");
      }
      columnObjectInspectors.add(ObjectInspectorFactory.
          getStandardPrimitiveObjectInspector(primitiveClass));
      lazyPrimitives[i] = LazyUtils.createLazyPrimitiveClass(primitiveClass);
    }
    
    cachedObjectInspector = 
        ObjectInspectorFactory.getLazySimpleStructObjectInspector(columnNames,
            columnObjectInspectors);
    
    cachedLazyStruct = new LazyStruct(lazyPrimitives, separator, 
        new Text(nullString), lastColumnTakesRest);
    
    LOG.debug("LazySimpleSerDe initialized with: columnNames=" + columnNames + " columnTypes=" 
        + columnTypes + " separator=" + separator + " nullstring=" + nullString 
        + " lastColumnTakesRest=" + lastColumnTakesRest);
  }
  
  // The object for storing row data
  LazyStruct cachedLazyStruct;
  
  /**
   * Deserialize a row from the Writable to a LazyObject.
   */
  public Object deserialize(Writable field) throws SerDeException {
    if (field instanceof BytesWritable) {
      BytesWritable b = (BytesWritable)field;
      // For backward-compatibility with hadoop 0.17
      cachedLazyStruct.setAll(b.get(), 0, b.getSize());
    } else if (field instanceof Text) {
      Text t = (Text)field;
      cachedLazyStruct.setAll(t.getBytes(), 0, t.getLength());
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
   */
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }
  
  Text serializeCache = new Text();
  /**
   * Serialize a row of data.
   */
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {

    // TODO: We can switch the serialization to be directly based on 
    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString() 
          + " can only serialize struct types, but we got: " + objInspector.getTypeName());
    }
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    
    StringBuilder sb = new StringBuilder();
    for(int i=0; i<fields.size(); i++) {
      if (i>0) {
        sb.append((char)separator);
      }
      StructField field = fields.get(i);
      Object fieldData = soi.getStructFieldData(obj, field);
      if (field.getFieldObjectInspector().getCategory() == Category.PRIMITIVE) {
        // For primitive object, serialize to plain string
        sb.append(fieldData == null ? nullString : fieldData.toString());
      } else {
        // For complex object, serialize to JSON format
        sb.append(SerDeUtils.getJSONString(fieldData, field.getFieldObjectInspector()));
      }
    }
    serializeCache.set(sb.toString());
    return serializeCache;
  }

}
