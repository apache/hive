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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.VOID_GROUP;


@Description(name = "width_bucket",
    value = "_FUNC_(expr, min_value, max_value, num_buckets) - Returns an integer between 0 and num_buckets+1 by "
        + "mapping the expr into buckets defined by the range [min_value, max_value]",
    extended = "Returns an integer between 0 and num_buckets+1 by "
        + "mapping expr into the ith equally sized bucket. Buckets are made by dividing [min_value, max_value] into "
        + "equally sized regions. If expr < min_value, return 1, if expr > max_value return num_buckets+1\n"
        + "Example: expr is an integer column withs values 1, 10, 20, 30.\n"
        + "  > SELECT _FUNC_(expr, 5, 25, 4) FROM src;\n1\n1\n3\n5")
public class GenericUDFWidthBucket extends GenericUDF {

  private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[4];
  private transient ObjectInspectorConverters.Converter[] converters = new ObjectInspectorConverters.Converter[4];

  private final IntWritable output = new IntWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 4, 4);

    checkArgPrimitive(arguments, 0);
    checkArgPrimitive(arguments, 1);
    checkArgPrimitive(arguments, 2);
    checkArgPrimitive(arguments, 3);

    checkArgGroups(arguments, 0, inputTypes, NUMERIC_GROUP, VOID_GROUP);
    checkArgGroups(arguments, 1, inputTypes, NUMERIC_GROUP, VOID_GROUP);
    checkArgGroups(arguments, 2, inputTypes, NUMERIC_GROUP, VOID_GROUP);
    checkArgGroups(arguments, 3, inputTypes, NUMERIC_GROUP, VOID_GROUP);

    obtainLongConverter(arguments, 0, inputTypes, converters);
    obtainLongConverter(arguments, 1, inputTypes, converters);
    obtainLongConverter(arguments, 2, inputTypes, converters);
    obtainIntConverter(arguments, 3, inputTypes, converters);

    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Long exprValue = getLongValue(arguments, 0, converters);
    Long minValue = getLongValue(arguments, 1, converters);
    Long maxValue = getLongValue(arguments, 2, converters);
    Integer numBuckets = getIntValue(arguments, 3, converters);

    if (exprValue == null || minValue == null || maxValue == null || numBuckets == null) {
      return null;
    }

    Preconditions.checkArgument(numBuckets > 0, "numBuckets in width_bucket function must be above 0");
    long intervalSize = (maxValue - minValue) / numBuckets;

    if (exprValue < minValue) {
      output.set(0);
    } else if (exprValue > maxValue) {
      output.set(numBuckets + 1);
    } else {
      long diff = exprValue - minValue;
      if (diff % intervalSize == 0) {
        output.set((int) (diff/intervalSize + 1));
      } else {
        output.set((int) Math.ceil((double) (diff) / intervalSize));
      }
    }

    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("width_bucket", children);
  }
}
