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
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantLongObjectInspector;
import org.apache.hadoop.io.LongWritable;

import com.google.common.math.LongMath;

/**
 * UDF grouping
 */
@Description(name = "grouping",
value = "_FUNC_(a, p1, ..., pn) - Indicates whether a specified column expression in "
+ "is aggregated or not. Returns 1 for aggregated or 0 for not aggregated. ",
extended = "a is the grouping id, p1...pn are the indices we want to extract")
@UDFType(deterministic = true)
public class GenericUDFGrouping extends GenericUDF {

  private transient PrimitiveObjectInspector groupingIdOI;
  private int[] indices;
  private LongWritable longWritable = new LongWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentLengthException(
        "grouping() requires at least 2 argument, got " + arguments.length);
    }

    if (arguments[0].getCategory() != Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "The first argument to grouping() must be primitive");
    }
    PrimitiveObjectInspector arg1OI = (PrimitiveObjectInspector) arguments[0];
    // INT can happen in cases where grouping() is used without grouping sets, in all other cases it should be LONG.
    if (!(arg1OI.getPrimitiveCategory() == PrimitiveCategory.INT ||
      arg1OI.getPrimitiveCategory() == PrimitiveCategory.LONG)) {
      throw new UDFArgumentTypeException(0,
        "The first argument to grouping() must be an int/long. Got: " + arg1OI.getPrimitiveCategory());
    }
    groupingIdOI =  arg1OI;

    indices = new int[arguments.length - 1];
    for (int i = 1; i < arguments.length; i++) {
      checkArgPrimitive(arguments, i);
      PrimitiveObjectInspector arg2OI = (PrimitiveObjectInspector) arguments[i];
      if (!(arg2OI instanceof ConstantObjectInspector)) {
        throw new UDFArgumentTypeException(i, "Must be a constant. Got: " + arg2OI.getClass().getSimpleName());
      }
      indices[i - 1] = PrimitiveObjectInspectorUtils
        .getInt(((ConstantObjectInspector) arguments[i]).getWritableConstantValue(), arg2OI);
    }

    return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    // groupingId = PrimitiveObjectInspectorUtils.getInt(arguments[0].get(), groupingIdOI);
    // Check that the bit at the given index is '1' or '0'
    long result = 0;
    // grouping(c1, c2, c3)
    // is equivalent to
    // 4 * grouping(c1) + 2 * grouping(c2) + grouping(c3)
    for (int a = 1; a < arguments.length; a++) {
      result += LongMath.pow(2, indices.length - a) *
              ((PrimitiveObjectInspectorUtils.getLong(arguments[0].get(), groupingIdOI) >> indices[a - 1]) & 1);
    }
    longWritable.set(result);
    return longWritable;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length > 1);
    return getStandardDisplayString("grouping", children);
  }

}
