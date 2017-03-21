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
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringLength;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.lazy.LazyBinary;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * GenericUDFLength.
 *
 */
@Description(name = "length",
    value = "_FUNC_(str | binary) - Returns the length of str or number of bytes in binary data",
    extended = "Example:\n"
    + "  > SELECT _FUNC_('Facebook') FROM src LIMIT 1;\n" + "  8")
@VectorizedExpressions({StringLength.class})
public class GenericUDFLength extends GenericUDF {
  private final IntWritable result = new IntWritable();
  private transient PrimitiveObjectInspector argumentOI;
  private transient PrimitiveObjectInspectorConverter.StringConverter stringConverter;
  private transient PrimitiveObjectInspectorConverter.BinaryConverter binaryConverter;
  private transient boolean isInputString;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentLengthException(
          "LENGTH requires 1 argument, got " + arguments.length);
    }

    if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentException(
          "LENGTH only takes primitive types, got " + argumentOI.getTypeName());
    }
    argumentOI = (PrimitiveObjectInspector) arguments[0];

    PrimitiveObjectInspector.PrimitiveCategory inputType = argumentOI.getPrimitiveCategory();
    ObjectInspector outputOI = null;
    switch (inputType) {
      case CHAR:
      case VARCHAR:
      case STRING:
        isInputString = true;
        stringConverter = new PrimitiveObjectInspectorConverter.StringConverter(argumentOI);
        break;

      case BINARY:
        isInputString = false;
        binaryConverter = new PrimitiveObjectInspectorConverter.BinaryConverter(argumentOI,
            PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
        break;

      default:
        throw new UDFArgumentException(
            " LENGTH() only takes STRING/CHAR/VARCHAR/BINARY types as first argument, got "
            + inputType);
    }

    outputOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    byte[] data = null;
    if (isInputString) {
      String val = null;
      if (arguments[0] != null) {
        val = (String) stringConverter.convert(arguments[0].get());
      }
      if (val == null) {
        return null;
      }

      data = val.getBytes();

      int len = 0;
      for (int i = 0; i < data.length; i++) {
        if (GenericUDFUtils.isUtfStartByte(data[i])) {
          len++;
        }
      }
      result.set(len);
      return result;
    } else {
      BytesWritable val = null;
      if (arguments[0] != null) {
        val = (BytesWritable) binaryConverter.convert(arguments[0].get());
      }
      if (val == null) {
        return null;
      }

      result.set(val.getLength());
      return result;
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("length", children);
  }
}
