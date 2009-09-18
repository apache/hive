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

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;


public class ObjectInspectorConverters {

  /**
   * A converter which will convert objects with one ObjectInspector to another.
   */
  public static interface Converter {
    public Object convert(Object o);
  }
  
  /**
   * Returns a converter that converts objects from one OI to another OI.
   * The returned (converted) object belongs to this converter, so that it can be reused
   * across different calls.
   */
  public static Converter getConverter(ObjectInspector inputOI, ObjectInspector outputOI) {
    switch (outputOI.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveObjectInspector)outputOI).getPrimitiveCategory()) {
          case BOOLEAN: 
            return new PrimitiveObjectInspectorConverter.BooleanConverter(
                (PrimitiveObjectInspector)inputOI, 
                (SettableBooleanObjectInspector)outputOI);
          case BYTE: 
            return new PrimitiveObjectInspectorConverter.ByteConverter(
                (PrimitiveObjectInspector)inputOI, 
                (SettableByteObjectInspector)outputOI);
          case SHORT: 
            return new PrimitiveObjectInspectorConverter.ShortConverter(
                (PrimitiveObjectInspector)inputOI, 
                (SettableShortObjectInspector)outputOI);
          case INT: 
            return new PrimitiveObjectInspectorConverter.IntConverter(
                (PrimitiveObjectInspector)inputOI, 
                (SettableIntObjectInspector)outputOI);
          case LONG: 
            return new PrimitiveObjectInspectorConverter.LongConverter(
                (PrimitiveObjectInspector)inputOI, 
                (SettableLongObjectInspector)outputOI);
          case FLOAT: 
            return new PrimitiveObjectInspectorConverter.FloatConverter(
                (PrimitiveObjectInspector)inputOI, 
                (SettableFloatObjectInspector)outputOI);
          case DOUBLE: 
            return new PrimitiveObjectInspectorConverter.DoubleConverter(
                (PrimitiveObjectInspector)inputOI, 
                (SettableDoubleObjectInspector)outputOI);
          case STRING:
            if (outputOI instanceof WritableStringObjectInspector) {
              return new PrimitiveObjectInspectorConverter.TextConverter(
                  (PrimitiveObjectInspector)inputOI);
            } else if  (outputOI instanceof WritableStringObjectInspector) {
              return new PrimitiveObjectInspectorConverter.TextConverter(
                  (PrimitiveObjectInspector)inputOI);
            }
          default: 
            throw new RuntimeException("Hive internal error: conversion of "
                + inputOI.getTypeName() + " to " + outputOI.getTypeName() + " not supported yet.");
        }
      case STRUCT:
      case LIST:
      case MAP:
      default:
        throw new RuntimeException("Hive internal error: conversion of "
            + inputOI.getTypeName() + " to " + outputOI.getTypeName() + " not supported yet.");
    }
  }
  
  
}
