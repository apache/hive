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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.OctetLength;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

@Description(name = "octet_length",
    value = "_FUNC_(str | binary) - Returns the number of bytes in str or binary data",
    extended = "Example:\n"
    + "  > SELECT _FUNC_('안녕하세요') FROM src LIMIT 1;\n" + "  15")
@VectorizedExpressions({OctetLength.class})
public class GenericUDFOctetLength extends GenericUDF {
  private final IntWritable result = new IntWritable();
  private transient PrimitiveObjectInspector argumentOI;
  private transient PrimitiveObjectInspectorConverter.StringConverter stringConverter;
  private transient boolean isInputString;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentLengthException(
          "OCTET_LENGTH requires 1 argument, got " + arguments.length);
    }

    if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentException(
          "OCTET_LENGTH only takes primitive types, got " + argumentOI.getTypeName());
    }
    argumentOI = (PrimitiveObjectInspector) arguments[0];

    stringConverter = new PrimitiveObjectInspectorConverter.StringConverter(argumentOI);
    PrimitiveObjectInspector.PrimitiveCategory inputType = argumentOI.getPrimitiveCategory();
    ObjectInspector outputOI = null;
    switch (inputType) {
      case CHAR:
      case VARCHAR:
      case STRING:
        isInputString = true;
        break;

      case BINARY:
        isInputString = false;
        break;

      default:
        throw new UDFArgumentException(
            " OCTET_LENGTH() only takes STRING/CHAR/VARCHAR/BINARY types as first argument, got "
                + inputType);
    }

    outputOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    return outputOI;
  }

  @Override
  public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {
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
    } else {
      BytesWritable val = null;
      if (arguments[0] != null) {
        val = (BytesWritable) arguments[0].get();
      }
      if (val == null) {
        return null;
      }

      data = val.getBytes();
    }

    result.set(data.length);
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("octet_length", children);
  }
}
