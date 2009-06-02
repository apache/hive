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

package org.apache.hadoop.hive.serde2.objectinspector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * ObjectInspectorFactory is the primary way to create new ObjectInspector instances.
 * 
 * SerDe classes should call the static functions in this library to create an ObjectInspector
 * to return to the caller of SerDe2.getObjectInspector(). 
 */
public class ObjectInspectorUtils {

  private static Log LOG = LogFactory.getLog(ObjectInspectorUtils.class.getName());
  
  
  static ArrayList<ArrayList<String>> integerArrayCache = new ArrayList<ArrayList<String>>();
  /**
   * Returns an array of Integer strings, starting from "0".
   * This function caches the arrays to provide a better performance. 
   */
  public static ArrayList<String> getIntegerArray(int size) {
    while (integerArrayCache.size() <= size) {
      integerArrayCache.add(null);
    }
    ArrayList<String> result = integerArrayCache.get(size);
    if (result == null) {
      result = new ArrayList<String>();
      for (int i=0; i<size; i++) {
        result.add(Integer.valueOf(i).toString());
      }
      integerArrayCache.set(size, result);
    }
    return result;
  }

  static ArrayList<String> integerCSVCache = new ArrayList<String>(); 
  public static String getIntegerCSV(int size) {
    while (integerCSVCache.size() <= size) {
      integerCSVCache.add(null);
    }
    String result = integerCSVCache.get(size);
    if (result == null) {
      StringBuilder sb = new StringBuilder();
      for(int i=0; i<size; i++) {
        if (i>0) sb.append(",");
        sb.append("" + i);
      }
      result = sb.toString();
      integerCSVCache.set(size, result);
    }
    return result;
  }
  

  /**
   * This enum controls how we copy primitive objects.
   * 
   * KEEP means keeping the original format of the primitive object. This is usually the most efficient. 
   * JAVA means converting all primitive objects to java primitive objects.
   * WRITABLE means converting all primitive objects to writable objects. 
   *
   */
  public enum ObjectInspectorCopyOption {
    KEEP,
    JAVA,
    WRITABLE
  }
  
  /**
   * Get the corresponding standard ObjectInspector for an ObjectInspector.
   * 
   * The returned ObjectInspector can be used to inspect the standard object.
   */
  public static ObjectInspector getStandardObjectInspector(ObjectInspector oi) {
    return getStandardObjectInspector(oi, ObjectInspectorCopyOption.KEEP);
  }
  
  public static ObjectInspector getStandardObjectInspector(ObjectInspector oi, ObjectInspectorCopyOption objectInspectorOption) {
    ObjectInspector result = null;
    switch (oi.getCategory()) {
      case PRIMITIVE: {
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector)oi;
        switch (objectInspectorOption) {
          case KEEP: {
            result = poi;
            break;
          }
          case JAVA: {
            result = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(poi.getPrimitiveCategory());
            break;
          }
          case WRITABLE: {
            result = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(poi.getPrimitiveCategory());
            break;
          }
        }
        break;
      }
      case LIST: {
        ListObjectInspector loi = (ListObjectInspector)oi;
        result = ObjectInspectorFactory.getStandardListObjectInspector(
            getStandardObjectInspector(loi.getListElementObjectInspector(), objectInspectorOption));
        break;
      }
      case MAP: {
        MapObjectInspector moi = (MapObjectInspector)oi;
        result = ObjectInspectorFactory.getStandardMapObjectInspector(
            getStandardObjectInspector(moi.getMapKeyObjectInspector(), objectInspectorOption),
            getStandardObjectInspector(moi.getMapValueObjectInspector(), objectInspectorOption));
        break;
      }
      case STRUCT: {
        StructObjectInspector soi = (StructObjectInspector)oi;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        List<String> fieldNames = new ArrayList<String>(fields.size());
        List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(fields.size());
        for(StructField f : fields) {
          fieldNames.add(f.getFieldName());
          fieldObjectInspectors.add(getStandardObjectInspector(f.getFieldObjectInspector(), objectInspectorOption));
        }
        result = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldObjectInspectors);
        break;
      }
      default: {
        throw new RuntimeException("Unknown ObjectInspector category!");
      }
    }
    return result;
  }
  
  /**
   * Returns a deep copy of the Object o that can be scanned by a
   * StandardObjectInspector returned by getStandardObjectInspector(oi).
   */
  public static Object copyToStandardObject(Object o, ObjectInspector oi) {
    return copyToStandardObject(o, oi, ObjectInspectorCopyOption.KEEP);
  }
  
  public static Object copyToStandardObject(Object o, ObjectInspector oi, ObjectInspectorCopyOption objectInspectorOption) {
    if (o == null) {
      return null;
    }
    
    Object result = null;
    switch (oi.getCategory()) {
      case PRIMITIVE: {
        PrimitiveObjectInspector loi = (PrimitiveObjectInspector)oi;
        switch (objectInspectorOption) {
          case KEEP: {
            result = loi.copyObject(o);
            break;
          }
          case JAVA: {
            result = loi.getPrimitiveJavaObject(o);
            break;
          }
          case WRITABLE: {
            result = loi.getPrimitiveWritableObject(o);
            break;
          }
        }
        break;
      }
      case LIST: {
        ListObjectInspector loi = (ListObjectInspector)oi;
        int length = loi.getListLength(o);
        ArrayList<Object> list = new ArrayList<Object>(length);
        for(int i=0; i<length; i++) {
          list.add(copyToStandardObject(
              loi.getListElement(o, i),
              loi.getListElementObjectInspector(),
              objectInspectorOption));
        }
        result = list;
        break;
      }
      case MAP: {
        MapObjectInspector moi = (MapObjectInspector)oi;
        HashMap<Object, Object> map = new HashMap<Object, Object>();
        Map<? extends Object, ? extends Object> omap = moi.getMap(o);
        for(Map.Entry<? extends Object, ? extends Object> entry: omap.entrySet()) {
          map.put(copyToStandardObject(entry.getKey(), moi.getMapKeyObjectInspector(), objectInspectorOption),
              copyToStandardObject(entry.getValue(), moi.getMapValueObjectInspector(), objectInspectorOption));
        }
        result = map;
        break;
      }
      case STRUCT: {
        StructObjectInspector soi = (StructObjectInspector)oi;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        ArrayList<Object> struct = new ArrayList<Object>(fields.size()); 
        for(StructField f : fields) {
          struct.add(copyToStandardObject(soi.getStructFieldData(o, f), f.getFieldObjectInspector(), objectInspectorOption));
        }
        result = struct;
        break;
      }
      default: {
        throw new RuntimeException("Unknown ObjectInspector category!");
      }
    }
    return result;
  }  
  
  public static String getStandardStructTypeName(StructObjectInspector soi) {
    StringBuilder sb = new StringBuilder();
    sb.append("struct<");
    List<? extends StructField> fields = soi.getAllStructFieldRefs(); 
    for(int i=0; i<fields.size(); i++) {
      if (i>0) sb.append(",");
      sb.append(fields.get(i).getFieldName());
      sb.append(":");
      sb.append(fields.get(i).getFieldObjectInspector().getTypeName());
    }
    sb.append(">");
    return sb.toString();
  }
  
  public static StructField getStandardStructFieldRef(String fieldName, List<? extends StructField> fields) {
    fieldName = fieldName.toLowerCase();
    for(int i=0; i<fields.size(); i++) {
      if (fields.get(i).getFieldName().equals(fieldName)) {
        return fields.get(i);
      }
    }
    // For backward compatibility: fieldNames can also be integer Strings.
    try {
      int i = Integer.parseInt(fieldName);
      if (i>=0 && i<fields.size()) {
        return fields.get(i);
      }
    } catch (NumberFormatException e) {
      // ignore
    }
    throw new RuntimeException("cannot find field " + fieldName + " from " + fields); 
    // return null;
  }
  
  /**
   * Get the class names of the ObjectInspector hierarchy. Mainly used for debugging. 
   */
  public static String getObjectInspectorName(ObjectInspector oi) {
    switch (oi.getCategory()) {
      case PRIMITIVE: {
        return oi.getClass().getSimpleName();
      }
      case LIST: {
        ListObjectInspector loi = (ListObjectInspector)oi;
        return oi.getClass().getSimpleName() + "<" + getObjectInspectorName(loi.getListElementObjectInspector()) + ">";
      }
      case MAP: {
        MapObjectInspector moi = (MapObjectInspector)oi;
        return oi.getClass().getSimpleName() + "<" + getObjectInspectorName(moi.getMapKeyObjectInspector()) 
            + "," + getObjectInspectorName(moi.getMapValueObjectInspector()) + ">";
      }
      case STRUCT: {
        StringBuffer result = new StringBuffer();
        result.append(oi.getClass().getSimpleName() + "<");
        StructObjectInspector soi = (StructObjectInspector)oi;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        for(int i = 0; i<fields.size(); i++) {
          result.append(fields.get(i).getFieldName());
          result.append(":");
          result.append(getObjectInspectorName(fields.get(i).getFieldObjectInspector()));
          if (i == fields.size() - 1) {
            result.append(">");
          } else {
            result.append(",");
          }
        }
        return result.toString();
      }
      default: {
        throw new RuntimeException("Unknown ObjectInspector category!");
      }
    }
  }

  public static int hashCode(Object o, ObjectInspector objIns) {
    if (o == null) {
      return 0;
    }
    switch (objIns.getCategory()) {
      case PRIMITIVE: {
        PrimitiveObjectInspector poi = ((PrimitiveObjectInspector)objIns);
        switch (poi.getPrimitiveCategory()) {
          case VOID: return 0;
          case BOOLEAN: return ((BooleanObjectInspector)poi).get(o) ? 1 : 0;
          case BYTE: return ((ByteObjectInspector)poi).get(o);
          case SHORT: return ((ShortObjectInspector)poi).get(o);
          case INT: return ((IntObjectInspector)poi).get(o);
          case LONG: {
            long a = ((LongObjectInspector)poi).get(o);
            return (int)((a >>> 32) ^ a);
          }
          case FLOAT: return Float.floatToIntBits(((FloatObjectInspector)poi).get(o));
          case DOUBLE: {
            // This hash function returns the same result as Double.hashCode()
            // while DoubleWritable.hashCode returns a different result.
            long a = Double.doubleToLongBits(((DoubleObjectInspector)poi).get(o));
            return (int)((a >>> 32) ^ a);
          }
          case STRING: {
            // This hash function returns the same result as String.hashCode() when
            // all characters are ASCII, while Text.hashCode() always returns a
            // different result.
            Text t = ((StringObjectInspector)poi).getPrimitiveWritableObject(o);
            int r = 0;
            for (int i=0; i<t.getLength(); i++) {
              r = r * 31 + (int)t.getBytes()[i];
            }
            return r;
          }
          default: {
            throw new RuntimeException("Unknown type: " + poi.getPrimitiveCategory());
          }
        }
      }
      case STRUCT:
      case LIST: 
      case MAP: 
      default:  
        throw new RuntimeException("Hash code on complex types not supported yet.");
    }
  }
  
}
