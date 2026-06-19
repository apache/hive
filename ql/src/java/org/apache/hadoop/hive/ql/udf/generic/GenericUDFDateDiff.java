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

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDateDiffColCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDateDiffColScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDateDiffScalarCol;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TimestampConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hive.common.util.DateParser;

import javax.annotation.Nullable;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.DATE_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

/**
 * UDFDateDiff.
 *
 * Calculate the difference in the number of days. The time part of the string
 * will be ignored. If dateString1 is earlier than dateString2, then the
 * result can be negative.
 *
 */
@Description(name = "datediff",
    value = "_FUNC_(date1, date2) - Returns the number of days between date1 and date2",
    extended = "date1 and date2 are strings in the format "
        + "'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'. The time parts are ignored."
        + "If date1 is earlier than date2, the result is negative.\n"
        + "Example:\n "
        + "  > SELECT _FUNC_('2009-07-30', '2009-07-31') FROM src LIMIT 1;\n"
        + "  1")
@VectorizedExpressions({VectorUDFDateDiffColScalar.class, VectorUDFDateDiffColCol.class, VectorUDFDateDiffScalarCol.class})
public class GenericUDFDateDiff extends GenericUDF {
  private final transient Converter[] tsConverters = new Converter[2];
  private IntWritable output = new IntWritable();
  private final transient PrimitiveCategory[] tsInputTypes = new PrimitiveCategory[2];

  public GenericUDFDateDiff() {
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 2);
    for (int i = 0; i < arguments.length; i++) {
      checkArgPrimitive(arguments, i);
      checkArgGroups(arguments, i, tsInputTypes, STRING_GROUP, DATE_GROUP);
      obtainTimestampConverter(arguments, i, tsInputTypes, tsConverters);
    }
    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  @Override
  public IntWritable evaluate(DeferredObject[] arguments) throws HiveException {

    Timestamp ts1 = getTimestampValue(arguments, 0, tsConverters);
    Timestamp ts2 = getTimestampValue(arguments, 1, tsConverters);

    if (ts1 == null || ts2 == null) {
      return null;
    }

    output.set(DateWritableV2.millisToDays(ts1.toEpochMilli()) - DateWritableV2.millisToDays(ts2.toEpochMilli()));
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("datediff", children);
  }

}
