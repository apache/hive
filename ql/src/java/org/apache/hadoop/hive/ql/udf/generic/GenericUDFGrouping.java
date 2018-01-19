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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.io.IntWritable;

import com.google.common.math.IntMath;

/**
 * UDF grouping
 */
@Description(name = "grouping",
value = "_FUNC_(a, p1, ..., pn) - Indicates whether a specified column expression in "
+ "is aggregated or not. Returns 1 for aggregated or 0 for not aggregated. ",
extended = "a is the grouping id, p1...pn are the indices we want to extract")
@UDFType(deterministic = true)
public class GenericUDFGrouping extends GenericUDF {

  private transient IntObjectInspector groupingIdOI;
  private int[] indices;
  private IntWritable intWritable = new IntWritable();

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
    if (arg1OI.getPrimitiveCategory() != PrimitiveCategory.INT) {
      throw new UDFArgumentTypeException(0, "The first argument to grouping() must be an integer");
    }
    groupingIdOI = (IntObjectInspector) arguments[0];

    indices = new int[arguments.length - 1];
    for (int i = 1; i < arguments.length; i++) {
      PrimitiveObjectInspector arg2OI = (PrimitiveObjectInspector) arguments[i];
      if (!(arg2OI instanceof WritableConstantIntObjectInspector)) {
        throw new UDFArgumentTypeException(i, "Must be a constant");
      }
      indices[i - 1] = ((WritableConstantIntObjectInspector)arg2OI).getWritableConstantValue().get();
    }

    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    // groupingId = PrimitiveObjectInspectorUtils.getInt(arguments[0].get(), groupingIdOI);
    // Check that the bit at the given index is '1' or '0'
    int result = 0;
    // grouping(c1, c2, c3)
    // is equivalent to
    // 4 * grouping(c1) + 2 * grouping(c2) + grouping(c3)
    for (int a = 1; a < arguments.length; a++) {
      result += IntMath.pow(2, indices.length - a) *
              ((PrimitiveObjectInspectorUtils.getInt(arguments[0].get(), groupingIdOI) >> indices[a - 1]) & 1);
    }
    intWritable.set(result);
    return intWritable;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length > 1);
    return getStandardDisplayString("grouping", children);
  }

}
