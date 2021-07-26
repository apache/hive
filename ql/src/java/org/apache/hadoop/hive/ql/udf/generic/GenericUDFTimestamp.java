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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDateToTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDecimalToTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDoubleToTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastLongToTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringToTimestamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.BOOLEAN_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.DATE_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.VOID_GROUP;

/**
 *
 * GenericUDFTimestamp
 *
 * Example usage:
 * ... CAST(&lt;Timestamp string&gt; as TIMESTAMP) ...
 *
 * Creates a TimestampWritableV2 object using PrimitiveObjectInspectorConverter
 *
 */
@Description(name = "timestamp",
             value = "cast(date as timestamp) - Returns timestamp")
@VectorizedExpressions({ CastLongToTimestamp.class, CastDateToTimestamp.class, CastDoubleToTimestamp.class,
                           CastDecimalToTimestamp.class, CastStringToTimestamp.class })
public class GenericUDFTimestamp extends GenericUDF {

  private final transient ObjectInspectorConverters.Converter[] tsConvertors =
      new ObjectInspectorConverters.Converter[1];
  private final transient PrimitiveCategory[] tsInputTypes = new PrimitiveCategory[1];
  /*
   * Integer value was interpreted to timestamp inconsistently in milliseconds comparing
   * to float/double in seconds. Since the issue exists for a long time and some users may
   * use in such inconsistent way, use the following flag to keep backward compatible.
   * If the flag is set to false, integer value is interpreted as timestamp in milliseconds;
   * otherwise, it's interpreted as timestamp in seconds.
   */
  private boolean intToTimestampInSeconds = false;
  private boolean strict = true;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);
    checkArgPrimitive(arguments, 0);
    checkArgGroups(arguments, 0, tsInputTypes, STRING_GROUP, DATE_GROUP, NUMERIC_GROUP, VOID_GROUP, BOOLEAN_GROUP);

    strict = SessionState.get() != null ? SessionState.get().getConf()
        .getBoolVar(ConfVars.HIVE_STRICT_TIMESTAMP_CONVERSION) : new HiveConf()
        .getBoolVar(ConfVars.HIVE_STRICT_TIMESTAMP_CONVERSION);
    intToTimestampInSeconds = SessionState.get() != null ? SessionState.get().getConf()
        .getBoolVar(ConfVars.HIVE_INT_TIMESTAMP_CONVERSION_IN_SECONDS) : new HiveConf()
        .getBoolVar(ConfVars.HIVE_INT_TIMESTAMP_CONVERSION_IN_SECONDS);

    if (strict) {
      if (PrimitiveObjectInspectorUtils.getPrimitiveGrouping(tsInputTypes[0]) == PrimitiveGrouping.NUMERIC_GROUP) {
        throw new UDFArgumentException(
            "Casting NUMERIC types to TIMESTAMP is prohibited (" + ConfVars.HIVE_STRICT_TIMESTAMP_CONVERSION + ")");
      }
    }

    obtainTimestampConverter(arguments, 0, tsInputTypes, tsConvertors);
    return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    PrimitiveObjectInspectorConverter.TimestampConverter ts =
        (PrimitiveObjectInspectorConverter.TimestampConverter) tsConvertors[0];
    ts.setIntToTimestampInSeconds(intToTimestampInSeconds);
    return ts.convert(arguments[0].get());
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1);
    return "CAST( " + children[0] + " AS TIMESTAMP)";
  }

  public boolean isIntToTimestampInSeconds() {
    return intToTimestampInSeconds;
  }
}
