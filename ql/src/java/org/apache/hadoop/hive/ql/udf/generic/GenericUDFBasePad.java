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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public abstract class GenericUDFBasePad extends GenericUDF {
  private transient Converter converter1;
  private transient Converter converter2;
  private transient Converter converter3;
  private Text result = new Text();
  private String udfName;
  private StringBuilder builder;

  public GenericUDFBasePad(String _udfName) {
    this.udfName = _udfName;
    this.builder = new StringBuilder();
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 3) {
      throw new UDFArgumentException(udfName + " requires three arguments. Found :"
        + arguments.length);
    }
    converter1 = checkTextArguments(arguments, 0);
    converter2 = checkIntArguments(arguments, 1);
    converter3 = checkTextArguments(arguments, 2);
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object valObject1 = arguments[0].get();
    Object valObject2 = arguments[1].get();
    Object valObject3 = arguments[2].get();
    if (valObject1 == null || valObject2 == null || valObject3 == null) {
      return null;
    }
    Text str = (Text) converter1.convert(valObject1);
    IntWritable lenW = (IntWritable) converter2.convert(valObject2);
    Text pad = (Text) converter3.convert(valObject3);
    if (str == null || pad == null || lenW == null || pad.toString().isEmpty()) {
      return null;
    }
    int len = lenW.get();
    builder.setLength(0);

    performOp(builder, len, str.toString(), pad.toString());
    result.set(builder.toString());
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(udfName, children);
  }

  protected abstract void performOp(
      StringBuilder builder, int len, String str, String pad);

  // Convert input arguments to Text, if necessary.
  private Converter checkTextArguments(ObjectInspector[] arguments, int i)
    throws UDFArgumentException {
    if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(i, "Only primitive type arguments are accepted but "
      + arguments[i].getTypeName() + " is passed.");
    }

    Converter converter = ObjectInspectorConverters.getConverter((PrimitiveObjectInspector) arguments[i],
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);

    return converter;
  }

  private Converter checkIntArguments(ObjectInspector[] arguments, int i)
    throws UDFArgumentException {
    if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(i, "Only primitive type arguments are accepted but "
      + arguments[i].getTypeName() + " is passed.");
    }
    PrimitiveCategory inputType = ((PrimitiveObjectInspector) arguments[i]).getPrimitiveCategory();
    Converter converter;
    switch (inputType) {
    case INT:
    case SHORT:
    case BYTE:
      converter = ObjectInspectorConverters.getConverter((PrimitiveObjectInspector) arguments[i],
      PrimitiveObjectInspectorFactory.writableIntObjectInspector);
      break;
    default:
      throw new UDFArgumentTypeException(i + 1, udfName
      + " only takes INT/SHORT/BYTE types as " + (i + 1) + "-ths argument, got "
      + inputType);
    }
    return converter;
  }
}
