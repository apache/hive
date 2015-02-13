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

import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;

public abstract class GenericUDFFloorCeilBase extends GenericUDF {
  private final String opName;
  protected String opDisplayName;

  private transient PrimitiveObjectInspector inputOI;
  private transient PrimitiveObjectInspector resultOI;

  private transient Converter converter;

  protected LongWritable longWritable = new LongWritable();
  protected HiveDecimalWritable decimalWritable = new HiveDecimalWritable();

  public GenericUDFFloorCeilBase() {
    opName = getClass().getSimpleName();
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentException(opName + " requires one argument.");
    }

    Category category = arguments[0].getCategory();
    if (category != Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "The "
          + GenericUDFUtils.getOrdinal(1)
          + " argument of " + opName + "  is expected to a "
          + Category.PRIMITIVE.toString().toLowerCase() + " type, but "
          + category.toString().toLowerCase() + " is found");
    }

    inputOI = (PrimitiveObjectInspector) arguments[0];
    if (!FunctionRegistry.isNumericType(inputOI.getTypeInfo())) {
      throw new UDFArgumentTypeException(0, "The "
          + GenericUDFUtils.getOrdinal(1)
          + " argument of " + opName + "  is expected to a "
          + "numeric type, but "
          + inputOI.getTypeName() + " is found");
    }

    PrimitiveTypeInfo resultTypeInfo = null;
    PrimitiveTypeInfo inputTypeInfo = inputOI.getTypeInfo();
    if (inputTypeInfo instanceof DecimalTypeInfo) {
      DecimalTypeInfo decTypeInfo = (DecimalTypeInfo) inputTypeInfo;
      resultTypeInfo = TypeInfoFactory.getDecimalTypeInfo(
          decTypeInfo.precision() - decTypeInfo.scale() + 1, 0);
      ObjectInspector decimalOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(decTypeInfo);
      converter =  ObjectInspectorConverters.getConverter(inputOI, decimalOI);
    } else {
      resultTypeInfo = TypeInfoFactory.longTypeInfo;
      ObjectInspector doubleObjectInspector = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
      converter = ObjectInspectorConverters.getConverter(inputOI, doubleObjectInspector);
    }

    return resultOI =
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(resultTypeInfo);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0] == null) {
      return null;
    }

    Object input = arguments[0].get();
    if (input == null) {
      return null;
    }

    input = converter.convert(input);
    if (input == null) {
      return null;
    }

    switch (resultOI.getPrimitiveCategory()) {
    case LONG:
      return evaluate((DoubleWritable)input);
    case DECIMAL:
      return evaluate((HiveDecimalWritable)input);
    default:
      // Should never happen.
      throw new IllegalStateException("Unexpected type in evaluating " + opName + ": " +
          inputOI.getPrimitiveCategory());
    }
  }

  protected abstract LongWritable evaluate(DoubleWritable input);

  protected abstract HiveDecimalWritable evaluate(HiveDecimalWritable input);

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1);
    return getStandardDisplayString(opDisplayName, children);
  }

}
