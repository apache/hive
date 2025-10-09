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


import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * Tumbling windows are a series of fixed-sized, non-overlapping and contiguous time intervals.
 * Tumbling windows are inclusive start exclusive end.
 * By default the beginning instant of fist window is Epoch 0 Thu Jan 01 00:00:00 1970 UTC.
 * Optionally users may provide a different origin as a timestamp arg3.
 *
 * This an example of series of window with an interval of 5 seconds and origin Epoch 0 Thu Jan 01 00:00:00 1970 UTC:
 *
 *
 *   interval 1           interval 2            interval 3
 *   Jan 01 00:00:00      Jan 01 00:00:05       Jan 01 00:00:10
 * 0 -------------- 4 : 5 --------------- 9: 10 --------------- 14
 *
 * This UDF rounds timestamp agr1 to the beginning of window interval where it belongs to.
 *
 */
@Description(name = "tumbling_window", value =
    "_FUNC_(timestamp, interval, origin) - returns the timestamp truncated to the "
        + " beginning of tumbling window time interval starting from origin timestamp, "
        + " by default origin is unix epoch 0", extended = "param has to be a timestamp value, Example:\n"
    + "  > SELECT _FUNC_(timestamp, interval) FROM src ;\n"
    + "  > SELECT _FUNC_(timestamp, interval, origin_timestamp);\n")
public class GenericUDFTumbledWindow extends GenericUDF {
  private PrimitiveObjectInspector intervalOI;
  private PrimitiveObjectInspector timestampOI;
  private PrimitiveObjectInspector originTsOI = null;
  private final transient TimestampWritableV2 timestampResult = new TimestampWritableV2();

  /**
   * Initialize this GenericUDF. This will be called once and only once per
   * GenericUDF instance.
   *
   * @param arguments
   *          The ObjectInspector for the arguments
   * @throws UDFArgumentException
   *           Thrown when arguments have wrong types, wrong length, etc.
   * @return The ObjectInspector for the return value
   */
  @Override public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 3);
    for (int i = 0; i < arguments.length; i++) {
      checkArgPrimitive(arguments, i);
    }
    //arg 1 has to be of timestamp type
    //arg 2 has to be an interval
    //arg 3 has to be absent or timestamp type

    timestampOI = (PrimitiveObjectInspector) arguments[0];
    intervalOI = (PrimitiveObjectInspector) arguments[1];

    if (arguments.length == 3) {
      originTsOI = (PrimitiveObjectInspector) arguments[2];
      if (!PrimitiveObjectInspectorUtils.getPrimitiveGrouping(originTsOI.getPrimitiveCategory())
          .equals(PrimitiveObjectInspectorUtils.PrimitiveGrouping.DATE_GROUP)) {
        throw new UDFArgumentException("Third arg has to be timestamp got " + originTsOI.getTypeInfo().toString());
      }
    }

    if (!PrimitiveObjectInspectorUtils.getPrimitiveGrouping(timestampOI.getPrimitiveCategory())
        .equals(PrimitiveObjectInspectorUtils.PrimitiveGrouping.DATE_GROUP)) {
      throw new UDFArgumentException("First arg has to be timestamp got " + timestampOI.getTypeInfo().toString());
    }

    if (!intervalOI.getPrimitiveCategory().equals(PrimitiveObjectInspector.PrimitiveCategory.INTERVAL_DAY_TIME)) {
      throw new UDFArgumentException("Second arg has to be interval got " + intervalOI.getTypeInfo().toString());
    }

    return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.timestampTypeInfo);
  }

  /**
   * Evaluate the GenericUDF with the arguments.
   *
   * @param arguments timestamp and interval.
   *
   * @return The truncated timestamp to the beginning of tumbled window interval.
   */
  @Override public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0] == null) {
      return null;
    }
    Timestamp ts = PrimitiveObjectInspectorUtils.getTimestamp(arguments[0].get(), timestampOI);
    HiveIntervalDayTime idt = PrimitiveObjectInspectorUtils.getHiveIntervalDayTime(arguments[1].get(), intervalOI);
    Timestamp
        origin =
        originTsOI == null ?
            Timestamp.ofEpochMilli(0) :
            PrimitiveObjectInspectorUtils.getTimestamp(arguments[2].get(), originTsOI);
    timestampResult.set(Timestamp.ofEpochMilli(truncate(ts, idt, origin)));
    return timestampResult;
  }

  private long truncate(Timestamp ts, HiveIntervalDayTime idt, Timestamp origin) {
    long intervalDurationMs = idt.getTotalSeconds() * 1000L + idt.getNanos() / 1000L;
    long offset = ts.toEpochMilli() % intervalDurationMs - origin.toEpochMilli() % intervalDurationMs;
    if (offset < 0) {
      offset += intervalDurationMs;
    }
    return ts.toEpochMilli() - offset;
  }

  @Override public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  @Override protected String getFuncName() {
    return "tumbling_window";
  }
}
