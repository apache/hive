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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDateToTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDecimalToTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDoubleToTimestamp;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastLongToTimestamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TimestampConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 *
 * GenericUDFTimestamp
 *
 * Example usage:
 * ... CAST(<Timestamp string> as TIMESTAMP) ...
 *
 * Creates a TimestampWritable object using PrimitiveObjectInspectorConverter
 *
 */
@Description(name = "timestamp",
value = "cast(date as timestamp) - Returns timestamp")
@VectorizedExpressions({CastLongToTimestamp.class, CastDateToTimestamp.class,
  CastDoubleToTimestamp.class, CastDecimalToTimestamp.class})
public class GenericUDFTimestamp extends GenericUDF {

  private transient PrimitiveObjectInspector argumentOI;
  private transient TimestampConverter tc;
  /*
   * Integer value was interpreted to timestamp inconsistently in milliseconds comparing
   * to float/double in seconds. Since the issue exists for a long time and some users may
   * use in such inconsistent way, use the following flag to keep backward compatible.
   * If the flag is set to false, integer value is interpreted as timestamp in milliseconds;
   * otherwise, it's interpreted as timestamp in seconds.
   */
  private boolean intToTimestampInSeconds = false;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 1) {
      throw new UDFArgumentLengthException(
          "The function TIMESTAMP requires at least one argument, got "
          + arguments.length);
    }

    SessionState ss = SessionState.get();
    if (ss != null) {
      intToTimestampInSeconds = ss.getConf().getBoolVar(ConfVars.HIVE_INT_TIMESTAMP_CONVERSION_IN_SECONDS);
    }

    try {
      argumentOI = (PrimitiveObjectInspector) arguments[0];
    } catch (ClassCastException e) {
      throw new UDFArgumentException(
          "The function TIMESTAMP takes only primitive types");
    }

    tc = new TimestampConverter(argumentOI,
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);
    tc.setIntToTimestampInSeconds(intToTimestampInSeconds);

    return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object o0 = arguments[0].get();
    if (o0 == null) {
      return null;
    }

    return tc.convert(o0);
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1);
    StringBuilder sb = new StringBuilder();
    sb.append("CAST( ");
    sb.append(children[0]);
    sb.append(" AS TIMESTAMP)");
    return sb.toString();
  }

  public boolean isIntToTimestampInSeconds() {
    return intToTimestampInSeconds;
  }
}
