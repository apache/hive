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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.TextConverter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;

/**
 * ObjectInspectorFactory is the primary way to create new ObjectInspector instances.
 * 
 * SerDe classes should call the static functions in this library to create an ObjectInspector
 * to return to the caller of SerDe2.getObjectInspector(). 
 */
public class ObjectInspectorUtils {

  private static Log LOG = LogFactory.getLog(ObjectInspectorUtils.class.getName());
  
  
  static ArrayList<ArrayList<String>> integerArrayCache = new ArrayList<ArrayList<String>>();

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
   * DEFAULT means choosing the most efficient way between JAVA and WRITABLE. 
   * JAVA means converting all primitive objects to java primitive objects.
   * WRITABLE means converting all primitive objects to writable objects. 
   *
   */
  public enum ObjectInspectorCopyOption {
    DEFAULT,
    JAVA,
    WRITABLE
  }
  
  /**
   * Get the corresponding standard ObjectInspector for an ObjectInspector.
   * 
   * The returned ObjectInspector can be used to inspect the standard object.
   */
  public static ObjectInspector getStandardObjectInspector(ObjectInspector oi) {
    return getStandardObjectInspector(oi, ObjectInspectorCopyOption.DEFAULT);
  }
  
  public static ObjectInspector getStandardObjectInspector(ObjectInspector oi, ObjectInspectorCopyOption objectInspectorOption) {
    ObjectInspector result = null;
    switch (oi.getCategory()) {
      case PRIMITIVE: {
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector)oi;
        switch (objectInspectorOption) {
          case DEFAULT: {
            if (poi.preferWritable()) {
              result = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(poi.getPrimitiveCategory());
            } else {
              result = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(poi.getPrimitiveCategory());
            }
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
    return copyToStandardObject(o, oi, ObjectInspectorCopyOption.DEFAULT);
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
          case DEFAULT: {
            if (loi.preferWritable()) {
              result = loi.getPrimitiveWritableObject(loi.copyObject(o));
            } else {
              result = loi.getPrimitiveJavaObject(o);
            }
            break;
          }
          case JAVA: {
            result = loi.getPrimitiveJavaObject(o);
            break;
          }
          case WRITABLE: {
            result = loi.getPrimitiveWritableObject(loi.copyObject(o));
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

  public static int compare(Object o1, ObjectInspector oi1, Object o2, ObjectInspector oi2) {
    if (oi1.getCategory() != oi2.getCategory()) {
      return oi1.getCategory().compareTo(oi2.getCategory());
    }

    if (o1 == null) {
      return o2 == null ? 0 : -1;
    } else if (o2 == null) {
      return 1;
    }
    
    switch (oi1.getCategory()) {
      case PRIMITIVE: {
        PrimitiveObjectInspector poi1 = ((PrimitiveObjectInspector)oi1);
        PrimitiveObjectInspector poi2 = ((PrimitiveObjectInspector)oi2);
        if (poi1.getPrimitiveCategory() != poi2.getPrimitiveCategory()) {
          return poi1.getPrimitiveCategory().compareTo(poi2.getPrimitiveCategory());
        }
        switch (poi1.getPrimitiveCategory()) {
          case VOID: return 0;
          case BOOLEAN: {
            int v1 = ((BooleanObjectInspector)poi1).get(o1) ? 1 : 0;
            int v2 = ((BooleanObjectInspector)poi2).get(o2) ? 1 : 0;
            return v1 - v2;
          }
          case BYTE: {
            int v1 = ((ByteObjectInspector)poi1).get(o1);
            int v2 = ((ByteObjectInspector)poi2).get(o2);
            return v1 - v2;
          }
          case SHORT: {
            int v1 = ((ShortObjectInspector)poi1).get(o1);
            int v2 = ((ShortObjectInspector)poi2).get(o2);
            return v1 - v2;
          }
          case INT: {
            int v1 = ((IntObjectInspector)poi1).get(o1);
            int v2 = ((IntObjectInspector)poi2).get(o2);
            return v1 > v2 ? 1 : (v1 < v2 ? -1 : 0);
          }
          case LONG: {
            long v1 = ((LongObjectInspector)poi1).get(o1);
            long v2 = ((LongObjectInspector)poi2).get(o2);
            return v1 > v2 ? 1 : (v1 < v2 ? -1 : 0);
          }
          case FLOAT: {
            float v1 = ((FloatObjectInspector)poi1).get(o1);
            float v2 = ((FloatObjectInspector)poi2).get(o2);
            return v1 > v2 ? 1 : (v1 < v2 ? -1 : 0);
          }
          case DOUBLE: {
            double v1 = ((DoubleObjectInspector)poi1).get(o1);
            double v2 = ((DoubleObjectInspector)poi2).get(o2);
            return v1 > v2 ? 1 : (v1 < v2 ? -1 : 0);
          }
          case STRING: {
            if (poi1.preferWritable() || poi2.preferWritable()) {
              Text t1 = (Text)poi1.getPrimitiveWritableObject(o1);
              Text t2 = (Text)poi2.getPrimitiveWritableObject(o2);
              return t1 == null
                     ? (t2 == null ? 0 : -1)
                     : (t2 == null ? 1 : t1.compareTo(t2));
            } else {
              String s1 = (String)poi1.getPrimitiveJavaObject(o1);
              String s2 = (String)poi2.getPrimitiveJavaObject(o2);
              return s1 == null
                     ? (s2 == null ? 0 : -1)
                     : (s2 == null ? 1 : s1.compareTo(s2));
            }
          }
          default: {
            throw new RuntimeException("Unknown type: " + poi1.getPrimitiveCategory());
          }
        }
      }
      case STRUCT: {
        StructObjectInspector soi1 = (StructObjectInspector)oi1;
        StructObjectInspector soi2 = (StructObjectInspector)oi2;
        List<? extends StructField> fields1 = soi1.getAllStructFieldRefs();
        List<? extends StructField> fields2 = soi2.getAllStructFieldRefs();
        int minimum = Math.min(fields1.size(), fields2.size());
        for (int i=0; i<minimum; i++) {
          int r = compare(
              soi1.getStructFieldData(o1, fields1.get(i)),
              fields1.get(i).getFieldObjectInspector(),
              soi2.getStructFieldData(o2, fields2.get(i)),
              fields2.get(i).getFieldObjectInspector());
          if (r != 0) return r;
        }
        return fields1.size() - fields2.size();
      }
      case LIST: {
        ListObjectInspector loi1 = (ListObjectInspector)oi1;
        ListObjectInspector loi2 = (ListObjectInspector)oi2;
        int minimum = Math.min(loi1.getListLength(o1), loi1.getListLength(o2));
        for (int i=0; i<minimum; i++) {
          int r = compare(
              loi1.getListElement(o1, i),
              loi1.getListElementObjectInspector(),
              loi2.getListElement(o2, i),
              loi2.getListElementObjectInspector());
          if (r != 0) return r;
        }
        return loi1.getListLength(o1) - loi1.getListLength(o2);
      }
      case MAP:
      default:  
        throw new RuntimeException("Hash code on map type not supported yet.");
    }
  }

  /**
   * A converter which will convert objects with one ObjectInspector to another.
   * We can optimize this class by reusing the same returning object, etc, in the future.
   *
   * Note that the outputOI has to be a standard Writable ObjectInspector for now.
   * In the future, we can allow standard Java ObjectInspector as well.
   */
  public static class Converter {
    boolean typeConversionNeeded;
    ObjectInspector inputOI;
    ObjectInspector outputOI;
    TextConverter textConverter;
    
    Converter(ObjectInspector inputOI, ObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
      
      if (outputOI != 
        TypeInfoUtils.getStandardObjectInspectorFromTypeInfo(
            TypeInfoUtils.getTypeInfoFromObjectInspector(outputOI))) {
        throw new RuntimeException("Hive internal error: ObjectInspectorUtils.Converter"
           + " is called with non writable outputOI!");
      }
      
      typeConversionNeeded = 
        !TypeInfoUtils.getTypeInfoFromObjectInspector(inputOI)
          .equals(TypeInfoUtils.getTypeInfoFromObjectInspector(outputOI));
      
      if (outputOI instanceof StringObjectInspector) {
        textConverter = new TextConverter();
      }
    }
    
    /**
     * Returns an object that can be inspected by outputOI.
     * @param o  an object that can be inspected by inputOI.
     */
    public Object convert(Object o) {

      if (o == null) {
        return null;
      }
      
      if (!typeConversionNeeded) {
        return ObjectInspectorUtils.copyToStandardObject(
            o, inputOI, ObjectInspectorCopyOption.WRITABLE);
      }
      
      PrimitiveObjectInspector pInputOI = (PrimitiveObjectInspector)inputOI;
      PrimitiveObjectInspector pOutputOI = (PrimitiveObjectInspector)outputOI;
      
      try {
        switch (pOutputOI.getPrimitiveCategory()) {
          case VOID:
          case BOOLEAN:
          case BYTE:
            return PrimitiveObjectInspectorUtils.getByte(o, pInputOI);
          case SHORT:
            return PrimitiveObjectInspectorUtils.getShort(o, pInputOI);
          case INT:
            return PrimitiveObjectInspectorUtils.getInt(o, pInputOI);
          case LONG:
            return PrimitiveObjectInspectorUtils.getLong(o, pInputOI);
          case FLOAT:
            return PrimitiveObjectInspectorUtils.getFloat(o, pInputOI);
          case DOUBLE:
            return PrimitiveObjectInspectorUtils.getDouble(o, pInputOI);
          case STRING:
            return textConverter.convert(o, pInputOI);
          default: throw new RuntimeException("Hive internal error: unknown category:"
              + pOutputOI.getPrimitiveCategory());
        }
      } catch (NumberFormatException e) {
        return null;
      } 
      
    }
  }

  /**
   * Returns a converter that converts objects from one OI to another OI.
   * The returned (converted) object belongs to this converter, so that it can be reused
   * across different calls.
   */
  public static Converter getConverter(ObjectInspector inputOI, ObjectInspector outputOI) {
    return new Converter(inputOI, outputOI);
  }
}
