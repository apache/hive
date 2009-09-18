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

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.apache.hadoop.io.Writable;


/**
 * PrimitiveObjectInspectorFactory is the primary way to create new PrimitiveObjectInspector 
 * instances.
 * 
 * The reason of having caches here is that ObjectInspector is because ObjectInspectors do
 * not have an internal state - so ObjectInspectors with the same construction parameters should
 * result in exactly the same ObjectInspector.
 */
public class PrimitiveObjectInspectorFactory {

  public final static JavaBooleanObjectInspector javaBooleanObjectInspector= new JavaBooleanObjectInspector();
  public final static JavaByteObjectInspector javaByteObjectInspector= new JavaByteObjectInspector();
  public final static JavaShortObjectInspector javaShortObjectInspector= new JavaShortObjectInspector();
  public final static JavaIntObjectInspector javaIntObjectInspector= new JavaIntObjectInspector();
  public final static JavaLongObjectInspector javaLongObjectInspector= new JavaLongObjectInspector();
  public final static JavaFloatObjectInspector javaFloatObjectInspector= new JavaFloatObjectInspector();
  public final static JavaDoubleObjectInspector javaDoubleObjectInspector= new JavaDoubleObjectInspector();
  public final static JavaStringObjectInspector javaStringObjectInspector= new JavaStringObjectInspector();
  public final static JavaVoidObjectInspector javaVoidObjectInspector= new JavaVoidObjectInspector();

  public final static WritableBooleanObjectInspector writableBooleanObjectInspector= new WritableBooleanObjectInspector();
  public final static WritableByteObjectInspector writableByteObjectInspector= new WritableByteObjectInspector();
  public final static WritableShortObjectInspector writableShortObjectInspector= new WritableShortObjectInspector();
  public final static WritableIntObjectInspector writableIntObjectInspector= new WritableIntObjectInspector();
  public final static WritableLongObjectInspector writableLongObjectInspector= new WritableLongObjectInspector();
  public final static WritableFloatObjectInspector writableFloatObjectInspector= new WritableFloatObjectInspector();
  public final static WritableDoubleObjectInspector writableDoubleObjectInspector= new WritableDoubleObjectInspector();
  public final static WritableStringObjectInspector writableStringObjectInspector= new WritableStringObjectInspector();
  public final static WritableVoidObjectInspector writableVoidObjectInspector= new WritableVoidObjectInspector();
  
  
  private static HashMap<PrimitiveCategory, AbstractPrimitiveWritableObjectInspector> 
      cachedPrimitiveWritableInspectorCache = 
        new HashMap<PrimitiveCategory, AbstractPrimitiveWritableObjectInspector>();
  static {
    cachedPrimitiveWritableInspectorCache.put(PrimitiveCategory.BOOLEAN, 
        writableBooleanObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(PrimitiveCategory.BYTE, 
        writableByteObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(PrimitiveCategory.SHORT, 
        writableShortObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(PrimitiveCategory.INT, 
        writableIntObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(PrimitiveCategory.LONG, 
        writableLongObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(PrimitiveCategory.FLOAT, 
        writableFloatObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(PrimitiveCategory.DOUBLE, 
        writableDoubleObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(PrimitiveCategory.STRING, 
        writableStringObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(PrimitiveCategory.VOID, 
        writableVoidObjectInspector);
  }
  

  private static HashMap<PrimitiveCategory, AbstractPrimitiveJavaObjectInspector> 
      cachedPrimitiveJavaInspectorCache = 
        new HashMap<PrimitiveCategory, AbstractPrimitiveJavaObjectInspector>();
  static {
    cachedPrimitiveJavaInspectorCache.put(PrimitiveCategory.BOOLEAN, 
        javaBooleanObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(PrimitiveCategory.BYTE, 
        javaByteObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(PrimitiveCategory.SHORT, 
        javaShortObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(PrimitiveCategory.INT, 
        javaIntObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(PrimitiveCategory.LONG, 
        javaLongObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(PrimitiveCategory.FLOAT, 
        javaFloatObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(PrimitiveCategory.DOUBLE, 
        javaDoubleObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(PrimitiveCategory.STRING, 
        javaStringObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(PrimitiveCategory.VOID, 
        javaVoidObjectInspector);
  }
  
  
  /**
   * Returns the PrimitiveWritableObjectInspector for the PrimitiveCategory.  
   * @param primitiveCategory
   */
  public static AbstractPrimitiveWritableObjectInspector 
      getPrimitiveWritableObjectInspector(
          PrimitiveCategory primitiveCategory) {
    AbstractPrimitiveWritableObjectInspector result = 
      cachedPrimitiveWritableInspectorCache.get(primitiveCategory);
    if (result == null) {
      throw new RuntimeException("Internal error: Cannot find ObjectInspector "
          + " for " + primitiveCategory);
    }
    return result;
  }
  
  /**
   * Returns the PrimitiveJavaObjectInspector for the PrimitiveCategory.  
   * @param primitiveCategory
   */
  public static AbstractPrimitiveJavaObjectInspector 
      getPrimitiveJavaObjectInspector(
          PrimitiveCategory primitiveCategory) {
    AbstractPrimitiveJavaObjectInspector result = 
      cachedPrimitiveJavaInspectorCache.get(primitiveCategory);
    if (result == null) {
      throw new RuntimeException("Internal error: Cannot find ObjectInspector "
          + " for " + primitiveCategory);
    }
    return result;
  }

  /**
   * Returns an ObjectInspector for a primitive Class.
   * The Class can be a Hive Writable class, or a Java Primitive Class.
   * 
   * A runtimeException will be thrown if the class is not recognized
   * as a primitive type by Hive.
   */
  public static PrimitiveObjectInspector 
      getPrimitiveObjectInspectorFromClass(Class<?> c) {
    if (Writable.class.isAssignableFrom(c)) {
      // It is a writable class
      PrimitiveTypeEntry te = PrimitiveObjectInspectorUtils
          .getTypeEntryFromPrimitiveWritableClass(c);
      if (te == null) {
        throw new RuntimeException("Internal error: Cannot recognize " + c);
      }
      return PrimitiveObjectInspectorFactory
          .getPrimitiveWritableObjectInspector(te.primitiveCategory);
    } else {
      // It is a Java class
      PrimitiveTypeEntry te = PrimitiveObjectInspectorUtils
          .getTypeEntryFromPrimitiveJavaClass(c);
      if (te == null) {
        throw new RuntimeException("Internal error: Cannot recognize " + c);
      }
      return PrimitiveObjectInspectorFactory
          .getPrimitiveJavaObjectInspector(te.primitiveCategory);
    }
  }
  
  
}
