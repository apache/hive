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
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;

/**
 * UDF grouping
 */
@Description(name = "grouping",
value = "_FUNC_(a, b) - Indicates whether a specified column expression in "
+ "is aggregated or not. Returns 1 for aggregated or 0 for not aggregated. ",
extended = "a is the grouping id, b is the index we want to extract")
@UDFType(deterministic = true)
@NDV(maxNdv = 2)
public class GenericUDFGrouping extends GenericUDF {

  private transient IntObjectInspector groupingIdOI;
  private int index = 0;
  private ByteWritable byteWritable = new ByteWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
        "grouping() requires 2 argument, got " + arguments.length);
    }

    if (arguments[0].getCategory() != Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "The first argument to grouping() must be primitive");
    }
    PrimitiveObjectInspector arg1OI = (PrimitiveObjectInspector) arguments[0];
    if (arg1OI.getPrimitiveCategory() != PrimitiveCategory.INT) {
      throw new UDFArgumentTypeException(0, "The first argument to grouping() must be an integer");
    }
    groupingIdOI = (IntObjectInspector) arguments[0];

    PrimitiveObjectInspector arg2OI = (PrimitiveObjectInspector) arguments[1];
    if (!(arg2OI instanceof WritableConstantIntObjectInspector)) {
      throw new UDFArgumentTypeException(1, "The second argument to grouping() must be a constant");
    }
    index = ((WritableConstantIntObjectInspector)arg2OI).getWritableConstantValue().get();

    return PrimitiveObjectInspectorFactory.writableByteObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    // groupingId = PrimitiveObjectInspectorUtils.getInt(arguments[0].get(), groupingIdOI);
    // Check that the bit at the given index is '1' or '0'
    byteWritable.set((byte)
            ((PrimitiveObjectInspectorUtils.getInt(arguments[0].get(), groupingIdOI) >> index) & 1));
    return byteWritable;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 2);
    return getStandardDisplayString("grouping", children);
  }

}
