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

import java.util.TimeZone;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TextConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TimestampConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "from_utc_timestamp",
             value = "from_utc_timestamp(timestamp, string timezone) - "
                     + "Assumes given timestamp is UTC and converts to given timezone (as of Hive 0.8.0)")
public class GenericUDFFromUtcTimestamp extends GenericUDF {

  static final Logger LOG = LoggerFactory.getLogger(GenericUDFFromUtcTimestamp.class);

  private transient PrimitiveObjectInspector[] argumentOIs;
  private transient TimestampConverter timestampConverter;
  private transient TextConverter textConverter;
  private transient TimeZone tzUTC = TimeZone.getTimeZone("UTC");

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException("The function " + getName() + " requires two "
          + "argument, got " + arguments.length);
    }
    try {
      argumentOIs = new PrimitiveObjectInspector[2];
      argumentOIs[0] = (PrimitiveObjectInspector) arguments[0];
      argumentOIs[1] = (PrimitiveObjectInspector) arguments[1];
    } catch (ClassCastException e) {
      throw new UDFArgumentException(
          "The function " + getName() + " takes only primitive types");
    }

    timestampConverter = new TimestampConverter(argumentOIs[0],
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);
    textConverter = new TextConverter(argumentOIs[1]);
    return PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object o0 = arguments[0].get();
    if (o0 == null) {
      return null;
    }
    Object o1 = arguments[1].get();
    if (o1 == null) {
      return null;
    }

    Object converted_o0 = timestampConverter.convert(o0);
    if (converted_o0 == null) {
      return null;
    }

    Timestamp inputTs = ((TimestampWritableV2) converted_o0).getTimestamp();

    String tzStr = textConverter.convert(o1).toString();
    TimeZone timezone = TimeZone.getTimeZone(tzStr);

    TimeZone fromTz;
    TimeZone toTz;
    if (invert()) {
      fromTz = timezone;
      toTz = tzUTC;
    } else {
      fromTz = tzUTC;
      toTz = timezone;
    }

    // inputTs is the year/month/day/hour/minute/second in the local timezone.
    // For this UDF we want it in the timezone represented by fromTz
    TimestampTZ fromTs = TimestampTZUtil.parse(inputTs.toString(), fromTz.toZoneId());
    if (fromTs == null) {
      return null;
    }

    // Now output this timestamp's millis value to the equivalent toTz.
    Timestamp result = Timestamp.valueOf(
        fromTs.getZonedDateTime().withZoneSameInstant(toTz.toZoneId()).toLocalDateTime().toString());
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder();
    sb.append("Converting field ");
    sb.append(children[0]);
    sb.append(" from UTC to timezone: ");
    if (children.length > 1) {
      sb.append(children[1]);
    }
    return sb.toString();
  }

  public String getName() {
    return "from_utc_timestamp";
  }

  protected boolean invert() {
    return false;
  }
}
