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
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FuncPowerDoubleToDouble;
import org.apache.hadoop.hive.ql.exec.vector.expressions.FuncPowerLongToDouble;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@Description(name = "power,pow",
value = "_FUNC_(x1, x2) - raise x1 to the power of x2",
extended = "Example:\n"
    + "  > SELECT _FUNC_(2, 3) FROM src LIMIT 1;\n" + "  8")
@VectorizedExpressions({FuncPowerLongToDouble.class, FuncPowerDoubleToDouble.class})
public class GenericUDFPower extends GenericUDF {
  private final String opName;
  private final String opDisplayName;

  private transient PrimitiveObjectInspector baseOI;
  private transient PrimitiveObjectInspector powerOI;
  protected transient PrimitiveObjectInspector resultOI;

  private transient Converter baseConverter;
  private transient Converter powerConverter;

  private final DoubleWritable doubleWritable = new DoubleWritable();

  public GenericUDFPower() {
    opName = getClass().getSimpleName();
    opDisplayName = "power";
    resultOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentException(opName + " requires two arguments.");
    }

    for (int i = 0; i < 2; i++) {
      Category category = arguments[i].getCategory();
      if (category != Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(i, "The "
            + GenericUDFUtils.getOrdinal(i + 1)
            + " argument of " + opName + "  is expected to a "
            + Category.PRIMITIVE.toString().toLowerCase() + " type, but "
            + category.toString().toLowerCase() + " is found");
      }
    }

    baseOI = (PrimitiveObjectInspector) arguments[0];
    if (!FunctionRegistry.isNumericType(baseOI.getTypeInfo())) {
      throw new UDFArgumentTypeException(0, "The "
          + GenericUDFUtils.getOrdinal(1)
          + " argument of " + opName + "  is expected to a "
          + "numeric type, but "
          + baseOI.getTypeName() + " is found");
    }

    powerOI = (PrimitiveObjectInspector) arguments[1];
    if (!FunctionRegistry.isNumericType(powerOI.getTypeInfo())) {
      throw new UDFArgumentTypeException(1, "The "
          + GenericUDFUtils.getOrdinal(2)
          + " argument of " + opName + "  is expected to a "
          + "numeric type, but "
          + powerOI.getTypeName() + " is found");
    }

    baseConverter = ObjectInspectorConverters.getConverter(baseOI,
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
    powerConverter = ObjectInspectorConverters.getConverter(powerOI,
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
    return resultOI;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 2);
    return opDisplayName + "(" + children[0] + ", " + children[1] + ")";
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0] == null || arguments[1] == null) {
      return null;
    }

    Object base = arguments[0].get();
    Object power = arguments[1].get();
    if (base == null && power == null) {
      return null;
    }

    base = baseConverter.convert(base);
    if (base == null) {
      return null;
    }
    power = powerConverter.convert(power);
    if (power == null) {
      return null;
    }

    doubleWritable.set(Math.pow(((DoubleWritable)base).get(), ((DoubleWritable)power).get()));
    return doubleWritable;
  }

}
