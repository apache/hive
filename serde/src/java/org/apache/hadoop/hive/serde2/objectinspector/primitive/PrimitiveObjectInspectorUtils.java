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

package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
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
public class PrimitiveObjectInspectorUtils {

  private static Log LOG = LogFactory.getLog(PrimitiveObjectInspectorUtils.class.getName());
  
  /**
   * TypeEntry stores information about a Hive Primitive TypeInfo.
   */
  public static class PrimitiveTypeEntry {
    
    /**
     * The category of the PrimitiveType.
     */
    public PrimitiveObjectInspector.PrimitiveCategory primitiveCategory;
    
    /**
     * primitiveJavaType refers to java types like int, double, etc. 
     */
    public Class<?> primitiveJavaType;
    /**
     * primitiveJavaClass refers to java classes like Integer, Double, String etc.
     */
    public Class<?> primitiveJavaClass;
    /**
     * writableClass refers to hadoop Writable classes like IntWritable, DoubleWritable, Text etc. 
     */
    public Class<?> primitiveWritableClass;
    /**
     * typeName is the name of the type as in DDL.
     */
    public String typeName;
    
    PrimitiveTypeEntry(PrimitiveObjectInspector.PrimitiveCategory primitiveCategory, 
        String typeName, Class<?> primitiveType, Class<?> javaClass, Class<?> hiveClass) {
      this.primitiveCategory = primitiveCategory;
      this.primitiveJavaType = primitiveType;
      this.primitiveJavaClass = javaClass;
      this.primitiveWritableClass = hiveClass;
      this.typeName = typeName;
    }
  }
  
  static final Map<PrimitiveCategory, PrimitiveTypeEntry> primitiveCategoryToTypeEntry = new HashMap<PrimitiveCategory, PrimitiveTypeEntry>();
  static final Map<Class<?>, PrimitiveTypeEntry> primitiveJavaTypeToTypeEntry = new HashMap<Class<?>, PrimitiveTypeEntry>();
  static final Map<Class<?>, PrimitiveTypeEntry> primitiveJavaClassToTypeEntry = new HashMap<Class<?>, PrimitiveTypeEntry>();
  static final Map<Class<?>, PrimitiveTypeEntry> primitiveWritableClassToTypeEntry = new HashMap<Class<?>, PrimitiveTypeEntry>();
  static final Map<String, PrimitiveTypeEntry> typeNameToTypeEntry = new HashMap<String, PrimitiveTypeEntry>();
  
  static void registerType(PrimitiveTypeEntry t) {
    if (t.primitiveCategory != PrimitiveCategory.UNKNOWN) {
      primitiveCategoryToTypeEntry.put(t.primitiveCategory, t);
    }
    if (t.primitiveJavaType != null) {
      primitiveJavaTypeToTypeEntry.put(t.primitiveJavaType, t);
    }
    if (t.primitiveJavaClass != null) {
      primitiveJavaClassToTypeEntry.put(t.primitiveJavaClass, t);
    }
    if (t.primitiveWritableClass != null) {
      primitiveWritableClassToTypeEntry.put(t.primitiveWritableClass, t);
    }
    if (t.typeName != null) {
      typeNameToTypeEntry.put(t.typeName, t);
    }
  }
  
  public static final PrimitiveTypeEntry stringTypeEntry = new PrimitiveTypeEntry(PrimitiveCategory.STRING, Constants.STRING_TYPE_NAME, null, String.class, Text.class);
  public static final PrimitiveTypeEntry booleanTypeEntry = new PrimitiveTypeEntry(PrimitiveCategory.BOOLEAN, Constants.BOOLEAN_TYPE_NAME, Boolean.TYPE, Boolean.class, BooleanWritable.class);
  public static final PrimitiveTypeEntry intTypeEntry = new PrimitiveTypeEntry(PrimitiveCategory.INT, Constants.INT_TYPE_NAME, Integer.TYPE, Integer.class, IntWritable.class);
  public static final PrimitiveTypeEntry longTypeEntry = new PrimitiveTypeEntry(PrimitiveCategory.LONG, Constants.BIGINT_TYPE_NAME, Long.TYPE, Long.class, LongWritable.class);
  public static final PrimitiveTypeEntry floatTypeEntry = new PrimitiveTypeEntry(PrimitiveCategory.FLOAT, Constants.FLOAT_TYPE_NAME, Float.TYPE, Float.class, FloatWritable.class);
  public static final PrimitiveTypeEntry voidTypeEntry = new PrimitiveTypeEntry(PrimitiveCategory.VOID, Constants.VOID_TYPE_NAME, Void.TYPE, Void.class, NullWritable.class);

  // No corresponding Writable classes for the following 3 in hadoop 0.17.0
  public static final PrimitiveTypeEntry doubleTypeEntry = new PrimitiveTypeEntry(PrimitiveCategory.DOUBLE, Constants.DOUBLE_TYPE_NAME, Double.TYPE, Double.class, DoubleWritable.class);
  public static final PrimitiveTypeEntry byteTypeEntry = new PrimitiveTypeEntry(PrimitiveCategory.BYTE, Constants.TINYINT_TYPE_NAME, Byte.TYPE, Byte.class, ByteWritable.class);
  public static final PrimitiveTypeEntry shortTypeEntry = new PrimitiveTypeEntry(PrimitiveCategory.SHORT, Constants.SMALLINT_TYPE_NAME, Short.TYPE, Short.class, ShortWritable.class);

  // Following 3 are complex types for special handling
  public static final PrimitiveTypeEntry unknownTypeEntry = new PrimitiveTypeEntry(PrimitiveCategory.UNKNOWN, "unknown", null, Object.class, null);
  public static final PrimitiveTypeEntry unknownMapTypeEntry = new PrimitiveTypeEntry(PrimitiveCategory.UNKNOWN, Constants.MAP_TYPE_NAME, null, Map.class, null);
  public static final PrimitiveTypeEntry unknownListTypeEntry = new PrimitiveTypeEntry(PrimitiveCategory.UNKNOWN, Constants.LIST_TYPE_NAME, null, List.class, null);
  
  static {
    registerType(stringTypeEntry);
    registerType(booleanTypeEntry);
    registerType(intTypeEntry);
    registerType(longTypeEntry);
    registerType(floatTypeEntry);
    registerType(voidTypeEntry);
    registerType(doubleTypeEntry);
    registerType(byteTypeEntry);
    registerType(shortTypeEntry);
    registerType(unknownTypeEntry);
    registerType(unknownMapTypeEntry);
    registerType(unknownListTypeEntry);
  }

  /**
   * Return Whether the class is a Java Primitive type or a Java Primitive class. 
   */
  public static Class<?> primitiveJavaTypeToClass(Class<?> clazz) {
    PrimitiveTypeEntry t = primitiveJavaTypeToTypeEntry.get(clazz);
    return t == null ? clazz : t.primitiveJavaClass;
  }

  /**
   * Whether the class is a Java Primitive type or a Java Primitive class. 
   */
  public static boolean isPrimitiveJava(Class<?> clazz) {
    return primitiveJavaTypeToTypeEntry.get(clazz) != null
           || primitiveJavaClassToTypeEntry.get(clazz) != null;
  }
  
  /**
   * Whether the class is a Java Primitive type. 
   */
  public static boolean isPrimitiveJavaType(Class<?> clazz) {
    return primitiveJavaTypeToTypeEntry.get(clazz) != null;
  }

  /**
   * Whether the class is a Java Primitive class. 
   */
  public static boolean isPrimitiveJavaClass(Class<?> clazz) {
    return primitiveJavaClassToTypeEntry.get(clazz) != null;
  }

  /**
   * Whether the class is a Hive Primitive Writable class. 
   */
  public static boolean isPrimitiveWritableClass(Class<?> clazz) {
    return primitiveWritableClassToTypeEntry.get(clazz) != null;
  }
  
  /**
   * Get the typeName from a Java Primitive Type or Java PrimitiveClass. 
   */
  public static String getTypeNameFromPrimitiveJava(Class<?> clazz) {
    PrimitiveTypeEntry t = primitiveJavaTypeToTypeEntry.get(clazz);
    if (t == null) {
      t = primitiveJavaClassToTypeEntry.get(clazz);
    }
    return t == null ? null : t.typeName;
  }
  
  /**
   * Get the typeName from a Primitive Writable Class. 
   */
  public static String getTypeNameFromPrimitiveWritable(Class<?> clazz) {
    PrimitiveTypeEntry t = primitiveWritableClassToTypeEntry.get(clazz);
    return t == null ? null : t.typeName;
  }

  /**
   * Get the typeName from a Java Primitive Type or Java PrimitiveClass. 
   */
  public static PrimitiveTypeEntry getTypeEntryFromPrimitiveCategory(PrimitiveCategory category) {
    return primitiveCategoryToTypeEntry.get(category);
  }
  
  /**
   * Get the TypeEntry for a Java Primitive Type or Java PrimitiveClass. 
   */
  public static PrimitiveTypeEntry getTypeEntryFromPrimitiveJava(Class<?> clazz) {
    PrimitiveTypeEntry t = primitiveJavaTypeToTypeEntry.get(clazz);
    if (t == null) {
      t = primitiveJavaClassToTypeEntry.get(clazz);
    }
    return t;
  }
  
  /**
   * Get the TypeEntry for a Java Primitive Type or Java PrimitiveClass. 
   */
  public static PrimitiveTypeEntry getTypeEntryFromPrimitiveJavaType(Class<?> clazz) {
    return primitiveJavaTypeToTypeEntry.get(clazz);
  }
  
  /**
   * Get the TypeEntry for a Java Primitive Type or Java PrimitiveClass. 
   */
  public static PrimitiveTypeEntry getTypeEntryFromPrimitiveJavaClass(Class<?> clazz) {
    return primitiveJavaClassToTypeEntry.get(clazz);
  }
  
  /**
   * Get the TypeEntry for a Primitive Writable Class. 
   */
  public static PrimitiveTypeEntry getTypeEntryFromPrimitiveWritableClass(Class<?> clazz) {
    return primitiveWritableClassToTypeEntry.get(clazz);
  }
  
  /**
   * Get the TypeEntry for a Primitive Writable Class. 
   */
  public static PrimitiveTypeEntry getTypeEntryFromTypeName(String typeName) {
    return typeNameToTypeEntry.get(typeName);
  }
  
  /**
   * Compare 2 primitive objects. Conversion not allowed.
   * Note that NULL does not equal to NULL according to SQL standard.
   */
  public static boolean comparePrimitiveObjects(Object o1, PrimitiveObjectInspector oi1,
      Object o2, PrimitiveObjectInspector oi2) {
    if (o1 == null || o2 == null) return false;
    
    if (oi1.getPrimitiveCategory() != oi2.getPrimitiveCategory()) {
      return false;
    }
    switch (oi1.getPrimitiveCategory()) {
      case BOOLEAN: {
        return ((BooleanObjectInspector)oi1).get(o1) == ((BooleanObjectInspector)oi2).get(o2);
      }
      case BYTE: {
        return ((ByteObjectInspector)oi1).get(o1) == ((ByteObjectInspector)oi2).get(o2);
      }
      case SHORT: {
        return ((ShortObjectInspector)oi1).get(o1) == ((ShortObjectInspector)oi2).get(o2);
      }
      case INT: {
        return ((IntObjectInspector)oi1).get(o1) == ((IntObjectInspector)oi2).get(o2);
      }
      case LONG: {
        return ((LongObjectInspector)oi1).get(o1) == ((LongObjectInspector)oi2).get(o2);
      }
      case FLOAT: {
        return ((FloatObjectInspector)oi1).get(o1) == ((FloatObjectInspector)oi2).get(o2);
      }
      case DOUBLE: {
        return ((DoubleObjectInspector)oi1).get(o1) == ((DoubleObjectInspector)oi2).get(o2);
      }
      case STRING: {
        Writable t1 = ((StringObjectInspector)oi1).getPrimitiveWritableObject(o1);
        Writable t2 = ((StringObjectInspector)oi2).getPrimitiveWritableObject(o2);
        return t1.equals(t2);
      }
      default:
        return false;
    }
  }
  

  /** Convert a primitive object to double.
   */
  public static double convertPrimitiveToDouble(Object o, PrimitiveObjectInspector oi) 
      throws NumberFormatException {
    switch (oi.getPrimitiveCategory()) {
      case BOOLEAN: {
        return ((BooleanObjectInspector)oi).get(o) ? 1 : 0;
      }
      case BYTE: {
        return ((ByteObjectInspector)oi).get(o);
      }
      case SHORT: {
        return ((ShortObjectInspector)oi).get(o);
      }
      case INT: {
        return ((IntObjectInspector)oi).get(o);
      }
      case LONG: {
        return ((LongObjectInspector)oi).get(o);
      }
      case FLOAT: {
        return ((FloatObjectInspector)oi).get(o);
      }
      case DOUBLE: {
        return ((DoubleObjectInspector)oi).get(o);
      }
      case STRING: {
        return Double.valueOf(((StringObjectInspector)oi).getPrimitiveJavaObject(o));
      }
      default:
        throw new NumberFormatException();
    }
  }
  
  /**
   * Compare 2 Primitive Objects with their Object Inspector, conversions allowed.
   * Note that NULL does not equal to NULL according to SQL standard.
   */
  public static boolean comparePrimitiveObjectsWithConversion(Object o1, PrimitiveObjectInspector oi1,
      Object o2, PrimitiveObjectInspector oi2) {
    if (o1 == null || o2 == null) return false;
    
    if (oi1.getPrimitiveCategory() == oi2.getPrimitiveCategory()) {
      return comparePrimitiveObjects(o1, oi1, o2, oi2);
    }
    
    // If not equal, convert all to double and compare
    try {
      return convertPrimitiveToDouble(o1, oi1) == convertPrimitiveToDouble(o2, oi2);
    } catch (NumberFormatException e) {
      return false;
    }
  }
  
  /**
   * Get the integer number out of a primitive object. 
   * Note that NullPointerException will be thrown if o is null.
   * Note that NumberFormatException will be thrown if o is not a valid number.
   */
  public static byte getByte(Object o, PrimitiveObjectInspector oi) throws NumberFormatException {
    return (byte)getInt(o, oi);
  }
  
  /**
   * Get the integer number out of a primitive object. 
   * Note that NullPointerException will be thrown if o is null.
   * Note that NumberFormatException will be thrown if o is not a valid number.
   */
  public static short getShort(Object o, PrimitiveObjectInspector oi) throws NumberFormatException {
    return (short)getInt(o, oi);
  }
  
  /**
   * Get the integer number out of a primitive object.
   * Note that NullPointerException will be thrown if o is null.
   * Note that NumberFormatException will be thrown if o is not a valid number.
   */
  public static int getInt(Object o, PrimitiveObjectInspector oi) throws NumberFormatException {
    int result = 0;
    switch (oi.getPrimitiveCategory()) {
      case VOID: {
        result = 0;
        break;
      }
      case BOOLEAN: {
        result = (((BooleanObjectInspector)oi).get(o) ? 1 : 0);
        break;
      }
      case BYTE: {
        result = ((ByteObjectInspector)oi).get(o);
        break;
      }
      case SHORT: {
        result = ((ShortObjectInspector)oi).get(o);
        break;
      }
      case INT: {
        result = ((IntObjectInspector)oi).get(o);
        break;
      }
      case LONG: {
        result = (int)((LongObjectInspector)oi).get(o);
        break;
      }
      case FLOAT: {
        result = (int)((FloatObjectInspector)oi).get(o);
        break;
      }
      case DOUBLE: {
        result = (int)((DoubleObjectInspector)oi).get(o);
        break;
      }
      case STRING: {
        StringObjectInspector soi = (StringObjectInspector)oi;
        if (soi.preferWritable()) {
          Text t = soi.getPrimitiveWritableObject(o);
          result = LazyInteger.parseInt(t.getBytes(), 0, t.getLength());
        } else {
          String s = soi.getPrimitiveJavaObject(o);
          result = Integer.parseInt(s);
        }
        break;
      }
      default: {
        // Should never happen because we checked this in SemanticAnalyzer.getXpathOrFuncExprNodeDesc
        throw new RuntimeException("Hive 2 Internal error: index expression is not ordinal types: "
            + oi.getTypeName());
      }
    }
    return result;
  }
  
  /**
   * Get the long number out of a primitive object. 
   * Note that NullPointerException will be thrown if o is null.
   * Note that NumberFormatException will be thrown if o is not a valid number.
   */
  public static long getLong(Object o, PrimitiveObjectInspector oi) throws NumberFormatException {
    long result = 0;
    switch (oi.getPrimitiveCategory()) {
      case VOID: {
        result = 0;
        break;
      }
      case BOOLEAN: {
        result = (((BooleanObjectInspector)oi).get(o) ? 1 : 0);
        break;
      }
      case BYTE: {
        result = ((ByteObjectInspector)oi).get(o);
        break;
      }
      case SHORT: {
        result = ((ShortObjectInspector)oi).get(o);
        break;
      }
      case INT: {
        result = ((IntObjectInspector)oi).get(o);
        break;
      }
      case LONG: {
        result = ((LongObjectInspector)oi).get(o);
        break;
      }
      case FLOAT: {
        result = (long)((FloatObjectInspector)oi).get(o);
        break;
      }
      case DOUBLE: {
        result = (long)((DoubleObjectInspector)oi).get(o);
        break;
      }
      case STRING: {
        StringObjectInspector soi = (StringObjectInspector)oi;
        if (soi.preferWritable()) {
          Text t = soi.getPrimitiveWritableObject(o);
          result = LazyLong.parseLong(t.getBytes(), 0, t.getLength());
        } else {
          String s = soi.getPrimitiveJavaObject(o);
          result = Long.parseLong(s);
        }
        break;
      }
      default: {
        // Should never happen because we checked this in SemanticAnalyzer.getXpathOrFuncExprNodeDesc
        throw new RuntimeException("Hive 2 Internal error: index expression is not ordinal types: "
            + oi.getTypeName());
      }
    }
    return result;
  }
  
  /**
   * Get the double number out of a primitive object. 
   * Note that NullPointerException will be thrown if o is null.
   * Note that NumberFormatException will be thrown if o is not a valid number.
   */
  public static double getDouble(Object o, PrimitiveObjectInspector oi) throws NumberFormatException {
    double result = 0;
    switch (oi.getPrimitiveCategory()) {
      case VOID: {
        result = 0;
        break;
      }
      case BOOLEAN: {
        result = (((BooleanObjectInspector)oi).get(o) ? 1 : 0);
        break;
      }
      case BYTE: {
        result = ((ByteObjectInspector)oi).get(o);
        break;
      }
      case SHORT: {
        result = ((ShortObjectInspector)oi).get(o);
        break;
      }
      case INT: {
        result = ((IntObjectInspector)oi).get(o);
        break;
      }
      case LONG: {
        result = ((LongObjectInspector)oi).get(o);
        break;
      }
      case FLOAT: {
        result = ((FloatObjectInspector)oi).get(o);
        break;
      }
      case DOUBLE: {
        result = ((DoubleObjectInspector)oi).get(o);
        break;
      }
      case STRING: {
        StringObjectInspector soi = (StringObjectInspector)oi;
        String s = soi.getPrimitiveJavaObject(o);
        result = Double.parseDouble(s);
        break;
      }
      default: {
        // Should never happen because we checked this in SemanticAnalyzer.getXpathOrFuncExprNodeDesc
        throw new RuntimeException("Hive 2 Internal error: index expression is not ordinal types: "
            + oi.getTypeName());
      }
    }
    return result;
  }    

  /**
   * Get the float number out of a primitive object. 
   * Note that NullPointerException will be thrown if o is null.
   * Note that NumberFormatException will be thrown if o is not a valid number.
   */
  public static float getFloat(Object o, PrimitiveObjectInspector oi) throws NumberFormatException {
    return (float)getDouble(o, oi);
  }
  
  /**
   * A helper class to convert any primitive to Text. 
   */
  public static class TextConverter {
    Text t = new Text();
    ByteStream.Output out = new ByteStream.Output();
    
    static byte[] trueBytes = {'T', 'R', 'U', 'E'};
    static byte[] falseBytes = {'F', 'A', 'L', 'S', 'E'};
    
    
    public Text convert(Object o, PrimitiveObjectInspector oi)  {
      if (o == null) {
        return null;
      }
      
      switch(oi.getPrimitiveCategory()) {
        case VOID: {
          return null;
        }
        case BOOLEAN: {
          t.set(((BooleanObjectInspector)oi).get(o) ? trueBytes : falseBytes);
          return t;
        }
        case BYTE: {
          out.reset();
          LazyInteger.writeUTF8NoException(out, ((ByteObjectInspector)oi).get(o));
          t.set(out.getData(), 0, out.getCount());
          return t;
        }
        case SHORT: {
          out.reset();
          LazyInteger.writeUTF8NoException(out, ((ShortObjectInspector)oi).get(o));
          t.set(out.getData(), 0, out.getCount());
          return t;
        }
        case INT: {
          out.reset();
          LazyInteger.writeUTF8NoException(out, ((IntObjectInspector)oi).get(o));
          t.set(out.getData(), 0, out.getCount());
          return t;
        }
        case LONG:{
          out.reset();
          LazyLong.writeUTF8NoException(out, ((LongObjectInspector)oi).get(o));
          t.set(out.getData(), 0, out.getCount());
          return t;
        }
        case FLOAT: {
          t.set(String.valueOf(((FloatObjectInspector)oi).get(o)));
          return t;
        }
        case DOUBLE: {
          t.set(String.valueOf(((DoubleObjectInspector)oi).get(o)));
          return t;
        }
        case STRING: {
          t.set(((StringObjectInspector)oi).getPrimitiveJavaObject(o));
          return t;
        }
        default: {
          throw new RuntimeException("Hive 2 Internal error: type = "
              + oi.getTypeName());
        }
      }
    }

    public Text evaluate(FloatWritable i)  {
      if (i == null) {
        return null;
      } else {
        t.set(i.toString());
        return t;
      }
    }
    
    public Text evaluate(DoubleWritable i)  {
      if (i == null) {
        return null;
      } else {
        t.set(i.toString());
        return t;
      }
    }

  }
}
