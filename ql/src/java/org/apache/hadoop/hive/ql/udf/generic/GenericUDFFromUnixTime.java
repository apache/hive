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

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

/**
 * GenericUDFFromUnixTime.
 *
 */
@Description(name = "from_unixtime",
    value = "_FUNC_(unix_time, format) - returns unix_time in the specified format",
    extended = "Example:\n"
        + "  > SELECT _FUNC_(0, 'yyyy-MM-dd HH:mm:ss') FROM src LIMIT 1;\n"
        + "  '1970-01-01 00:00:00'")
public class GenericUDFFromUnixTime extends GenericUDF {

  private transient IntObjectInspector inputIntOI;
  private transient LongObjectInspector inputLongOI;
  private transient ZoneId timeZone;
  private transient final Text result = new Text();
  private transient String lastFormat ="uuuu-MM-dd HH:mm:ss";
  private transient DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern(lastFormat);
  private transient Converter[] converters = new Converter[2];
  private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[2];

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 2);

    for (int i = 0; i < arguments.length; i++) {
      checkArgPrimitive(arguments, i);
    }

    PrimitiveObjectInspector arg0OI = (PrimitiveObjectInspector) arguments[0];
    switch (arg0OI.getPrimitiveCategory()) {
      case INT:
        inputIntOI = (IntObjectInspector) arguments[0];
        break;
      case LONG:
        inputLongOI = (LongObjectInspector) arguments[0];
        break;
      default:
        throw new UDFArgumentException("The function from_unixtime takes only int/long types for first argument. Got Type:"
            + arg0OI.getPrimitiveCategory().name());
    }

    if (arguments.length == 2) {
      checkArgGroups(arguments, 1, inputTypes, STRING_GROUP);
      obtainStringConverter(arguments, 1, inputTypes, converters);
    }

    if (timeZone == null) {
      timeZone = SessionState.get() == null ? new HiveConf().getLocalTimeZone() : SessionState.get().getConf()
              .getLocalTimeZone();
      FORMATTER.withZone(timeZone);
    }

    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public void configure(MapredContext context) {
    if (context != null) {
      String timeZoneStr = HiveConf.getVar(context.getJobConf(), HiveConf.ConfVars.HIVE_LOCAL_TIME_ZONE);
      timeZone = TimestampTZUtil.parseTimeZone(timeZoneStr);
      FORMATTER.withZone(timeZone);
    }
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }

    if(arguments.length == 2) {
      String format = getStringValue(arguments, 1, converters);
      if (format == null) {
        return null;
      }
      if (!format.equals(lastFormat)) {
        FORMATTER = DateTimeFormatter.ofPattern(format);
        lastFormat = format;
      }
    }

    long unixTime = (inputIntOI != null) ? inputIntOI.get(arguments[0].get()) : inputLongOI.get(arguments[0].get());
    Instant instant = Instant.ofEpochSecond(unixTime);
    ZonedDateTime zonedDT = ZonedDateTime.ofInstant(instant, timeZone);
    result.set(zonedDT.format(FORMATTER));
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("from_unixtime", children, ", ");
  }
}

