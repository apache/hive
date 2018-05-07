/*
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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * Generic UDF params utility class
 */
public class GenericUDFParamUtils {

  private GenericUDFParamUtils() {
  }

  public static BytesWritable getBinaryValue(DeferredObject[] arguments, int i,
      Converter[] converters) throws HiveException {
    Object obj;
    if ((obj = arguments[i].get()) == null) {
      return null;
    }
    Object writableValue = converters[i].convert(obj);
    return (BytesWritable) writableValue;
  }

  public static Text getTextValue(DeferredObject[] arguments, int i, Converter[] converters)
      throws HiveException {
    Object obj;
    if ((obj = arguments[i].get()) == null) {
      return null;
    }
    Object writableValue = converters[i].convert(obj);
    return (Text) writableValue;
  }

  public static void obtainBinaryConverter(ObjectInspector[] arguments, int i,
      PrimitiveCategory[] inputTypes, Converter[] converters) throws UDFArgumentTypeException {
    PrimitiveObjectInspector inOi = (PrimitiveObjectInspector) arguments[i];
    PrimitiveCategory inputType = inOi.getPrimitiveCategory();

    Converter converter = ObjectInspectorConverters.getConverter(arguments[i],
        PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
    converters[i] = converter;
    inputTypes[i] = inputType;
  }

  public static BytesWritable getConstantBytesValue(ObjectInspector[] arguments, int i) {
    Object constValue = ((ConstantObjectInspector) arguments[i]).getWritableConstantValue();
    return (BytesWritable) constValue;
  }
}
