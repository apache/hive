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

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.DATE_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

/**
 * GenericUDFDateFormat.
 *
 * converts a date/timestamp/string to a value of string in the format specified
 * by the java date format
 *
 */
@Description(name = "date_format", value = "_FUNC_(date/timestamp/string, fmt) - converts a date/timestamp/string "
    + "to a value of string in the format specified by the date format fmt.",
    extended = "Supported formats are DateTimeFormatter formats - "
        + "https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html. "
        + "Second argument fmt should be constant.\n"
        + "Example: > SELECT _FUNC_('2015-04-08', 'y');\n '2015'")
public class GenericUDFDateFormat extends GenericUDF {
  private final transient Converter[] tsConverters = new Converter[2];
  private final transient PrimitiveCategory[] tsInputTypes = new PrimitiveCategory[2];

  private final Text output = new Text();
  private transient ZoneId timeZone;
  private transient DateTimeFormatter formatter;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 2);

    checkArgPrimitive(arguments, 0);
    checkArgPrimitive(arguments, 1);

    // the function should support both short date and full timestamp format
    // time part of the timestamp should not be skipped
    checkArgGroups(arguments, 0, tsInputTypes, STRING_GROUP, DATE_GROUP);
    checkArgGroups(arguments, 1, tsInputTypes, STRING_GROUP);

    obtainTimestampConverter(arguments, 0, tsInputTypes, tsConverters);

    if (arguments[1] instanceof ConstantObjectInspector) {
      String fmtStr = getConstantStringValue(arguments, 1);
      if (fmtStr != null) {
        try {
          if (timeZone == null) {
            timeZone = SessionState.get() == null ? new HiveConf().getLocalTimeZone() : SessionState.get().getConf()
                .getLocalTimeZone();
          }
          formatter = DateTimeFormatter.ofPattern(fmtStr);
        } catch (IllegalArgumentException e) {
          // ignore
        }
      }
    } else {
      throw new UDFArgumentTypeException(1, getFuncName() + " only takes constant as " + getArgOrder(1) + " argument");
    }

    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (formatter == null) {
      return null;
    }

    // the function should support both short date and full timestamp format
    // time part of the timestamp should not be skipped
    Timestamp ts = getTimestampValue(arguments, 0, tsConverters);

    if (ts == null) {
      return null;
    }

    Instant instant = Instant.ofEpochSecond(ts.toEpochSecond(), ts.getNanos());
    ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
    String res = formatter.format(zonedDateTime.withZoneSameLocal(timeZone));

    output.set(res);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  @Override
  protected String getFuncName() {
    return "date_format";
  }
}