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

import org.apache.hadoop.hive.serde2.io.BigDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * PrimitiveObjectInspectorFactory is the primary way to create new
 * PrimitiveObjectInspector instances.
 *
 * The reason of having caches here is that ObjectInspector is because
 * ObjectInspectors do not have an internal state - so ObjectInspectors with the
 * same construction parameters should result in exactly the same
 * ObjectInspector.
 */
public final class PrimitiveObjectInspectorFactory {

  public static final JavaBooleanObjectInspector javaBooleanObjectInspector =
      new JavaBooleanObjectInspector();
  public static final JavaByteObjectInspector javaByteObjectInspector =
      new JavaByteObjectInspector();
  public static final JavaShortObjectInspector javaShortObjectInspector =
      new JavaShortObjectInspector();
  public static final JavaIntObjectInspector javaIntObjectInspector =
      new JavaIntObjectInspector();
  public static final JavaLongObjectInspector javaLongObjectInspector =
      new JavaLongObjectInspector();
  public static final JavaFloatObjectInspector javaFloatObjectInspector =
      new JavaFloatObjectInspector();
  public static final JavaDoubleObjectInspector javaDoubleObjectInspector =
      new JavaDoubleObjectInspector();
  public static final JavaStringObjectInspector javaStringObjectInspector =
      new JavaStringObjectInspector();
  public static final JavaVoidObjectInspector javaVoidObjectInspector =
      new JavaVoidObjectInspector();
  public static final JavaTimestampObjectInspector javaTimestampObjectInspector =
      new JavaTimestampObjectInspector();
  public static final JavaBinaryObjectInspector javaByteArrayObjectInspector =
      new JavaBinaryObjectInspector();
  public static final JavaBigDecimalObjectInspector javaBigDecimalObjectInspector =
      new JavaBigDecimalObjectInspector();

  public static final WritableBooleanObjectInspector writableBooleanObjectInspector =
      new WritableBooleanObjectInspector();
  public static final WritableByteObjectInspector writableByteObjectInspector =
      new WritableByteObjectInspector();
  public static final WritableShortObjectInspector writableShortObjectInspector =
      new WritableShortObjectInspector();
  public static final WritableIntObjectInspector writableIntObjectInspector =
      new WritableIntObjectInspector();
  public static final WritableLongObjectInspector writableLongObjectInspector =
      new WritableLongObjectInspector();
  public static final WritableFloatObjectInspector writableFloatObjectInspector =
      new WritableFloatObjectInspector();
  public static final WritableDoubleObjectInspector writableDoubleObjectInspector =
      new WritableDoubleObjectInspector();
  public static final WritableStringObjectInspector writableStringObjectInspector =
      new WritableStringObjectInspector();
  public static final WritableVoidObjectInspector writableVoidObjectInspector =
      new WritableVoidObjectInspector();
  public static final WritableTimestampObjectInspector writableTimestampObjectInspector =
      new WritableTimestampObjectInspector();
  public static final WritableBinaryObjectInspector writableBinaryObjectInspector =
      new WritableBinaryObjectInspector();
  public static final WritableBigDecimalObjectInspector writableBigDecimalObjectInspector =
      new WritableBigDecimalObjectInspector();

  private static HashMap<PrimitiveCategory, AbstractPrimitiveWritableObjectInspector> cachedPrimitiveWritableInspectorCache =
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
    cachedPrimitiveWritableInspectorCache.put(PrimitiveCategory.TIMESTAMP,
        writableTimestampObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(PrimitiveCategory.BINARY,
        writableBinaryObjectInspector);
    cachedPrimitiveWritableInspectorCache.put(PrimitiveCategory.DECIMAL,
        writableBigDecimalObjectInspector);
  }

  private static HashMap<PrimitiveCategory, AbstractPrimitiveJavaObjectInspector> cachedPrimitiveJavaInspectorCache =
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
    cachedPrimitiveJavaInspectorCache.put(PrimitiveCategory.TIMESTAMP,
        javaTimestampObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(PrimitiveCategory.BINARY,
        javaByteArrayObjectInspector);
    cachedPrimitiveJavaInspectorCache.put(PrimitiveCategory.DECIMAL,
        javaBigDecimalObjectInspector);
  }

  /**
   * Returns the PrimitiveWritableObjectInspector for the PrimitiveCategory.
   *
   * @param primitiveCategory
   */
  public static AbstractPrimitiveWritableObjectInspector getPrimitiveWritableObjectInspector(
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
   * Returns a PrimitiveWritableObjectInspector which implements ConstantObjectInspector
   * for the PrimitiveCategory.
   *
   * @param primitiveCategory
   * @param value
   */
  public static ConstantObjectInspector getPrimitiveWritableConstantObjectInspector(
      PrimitiveCategory primitiveCategory, Object value) {
    switch (primitiveCategory) {
    case BOOLEAN:
      return new WritableConstantBooleanObjectInspector((BooleanWritable)value);
    case BYTE:
      return new WritableConstantByteObjectInspector((ByteWritable)value);
    case SHORT:
      return new WritableConstantShortObjectInspector((ShortWritable)value);
    case INT:
      return new WritableConstantIntObjectInspector((IntWritable)value);
    case LONG:
      return new WritableConstantLongObjectInspector((LongWritable)value);
    case FLOAT:
      return new WritableConstantFloatObjectInspector((FloatWritable)value);
    case DOUBLE:
      return new WritableConstantDoubleObjectInspector((DoubleWritable)value);
    case STRING:
      return new WritableConstantStringObjectInspector((Text)value);
    case TIMESTAMP:
      return new WritableConstantTimestampObjectInspector((TimestampWritable)value);
    case DECIMAL:
      return new WritableConstantBigDecimalObjectInspector((BigDecimalWritable)value);
    case BINARY:
      return new WritableConstantBinaryObjectInspector((BytesWritable)value);
    case VOID:
      return new WritableVoidObjectInspector();
    default:
      throw new RuntimeException("Internal error: Cannot find "
        + "ConstantObjectInspector for " + primitiveCategory);
    }
  }

  /**
   * Returns the PrimitiveJavaObjectInspector for the PrimitiveCategory.
   *
   * @param primitiveCategory
   */
  public static AbstractPrimitiveJavaObjectInspector getPrimitiveJavaObjectInspector(
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
   * Returns an ObjectInspector for a primitive Class. The Class can be a Hive
   * Writable class, or a Java Primitive Class.
   *
   * A runtimeException will be thrown if the class is not recognized as a
   * primitive type by Hive.
   */
  public static PrimitiveObjectInspector getPrimitiveObjectInspectorFromClass(
      Class<?> c) {
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

  private PrimitiveObjectInspectorFactory() {
    // prevent instantiation
  }
}
