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
package org.apache.hadoop.hive.serde2.lazybinary;

import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryListObjectInspector;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;

public class LazyBinaryFactory {

  /**
   * Create a lazy binary primitive class given the type name.
   */
  public static LazyBinaryPrimitive<?, ?> createLazyBinaryPrimitiveClass(
      PrimitiveObjectInspector oi) {
    PrimitiveCategory p = oi.getPrimitiveCategory();
    switch (p) {
    case BOOLEAN: {
      return new LazyBinaryBoolean((WritableBooleanObjectInspector) oi);
    }
    case BYTE: {
      return new LazyBinaryByte((WritableByteObjectInspector) oi);
    }
    case SHORT: {
      return new LazyBinaryShort((WritableShortObjectInspector) oi);
    }
    case INT: {
      return new LazyBinaryInteger((WritableIntObjectInspector) oi);
    }
    case LONG: {
      return new LazyBinaryLong((WritableLongObjectInspector) oi);
    }
    case FLOAT: {
      return new LazyBinaryFloat((WritableFloatObjectInspector) oi);
    }
    case DOUBLE: {
      return new LazyBinaryDouble((WritableDoubleObjectInspector) oi);
    }
    case STRING: {
      return new LazyBinaryString((WritableStringObjectInspector) oi);
    }
    default: {
      throw new RuntimeException("Internal error: no LazyBinaryObject for " + p);
    }
    }
  }

  /**
   * Create a hierarchical LazyBinaryObject based on the given typeInfo.
   */
  public static LazyBinaryObject createLazyBinaryObject(ObjectInspector oi) {
    ObjectInspector.Category c = oi.getCategory();
    switch (c) {
    case PRIMITIVE:
      return createLazyBinaryPrimitiveClass((PrimitiveObjectInspector) oi);
    case MAP:
      return new LazyBinaryMap((LazyBinaryMapObjectInspector) oi);
    case LIST:
      return new LazyBinaryArray((LazyBinaryListObjectInspector) oi);
    case STRUCT:
      return new LazyBinaryStruct((LazyBinaryStructObjectInspector) oi);
    }

    throw new RuntimeException("Hive LazyBinarySerDe Internal error.");
  }
}
