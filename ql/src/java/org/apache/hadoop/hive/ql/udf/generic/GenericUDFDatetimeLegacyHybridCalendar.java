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

import java.text.SimpleDateFormat;
import java.util.TimeZone;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDatetimeLegacyHybridCalendarDate;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDatetimeLegacyHybridCalendarTimestamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


/**
 * GenericUDFDatetimeLegacyHybridCalendar.
 */
@Description(name = "datetime_legacy_hybrid_calendar",
    value = "_FUNC_(date/timestamp) - Converts a date/timestamp to legacy hybrid Julian-Gregorian "
        + "calendar\n"
        + "assuming that its internal days/milliseconds since epoch is calculated using the "
        + "proleptic Gregorian calendar.",
    extended = "Converts a date/timestamp to legacy Gregorian-Julian hybrid calendar, i.e., "
        + "calendar that supports both\n"
        + "the Julian and Gregorian calendar systems with the support of a single discontinuity, "
        + "which corresponds by\n"
        + "default to the Gregorian date when the Gregorian calendar was instituted; assuming "
        + "that its internal\n"
        + "days/milliseconds since epoch is calculated using new proleptic Gregorian calendar "
        + "(ISO 8601 standard), which\n"
        + "is produced by extending the Gregorian calendar backward to dates preceding its "
        + "official introduction in 1582.\n")
@VectorizedExpressions({VectorUDFDatetimeLegacyHybridCalendarTimestamp.class,
    VectorUDFDatetimeLegacyHybridCalendarDate.class })
public class GenericUDFDatetimeLegacyHybridCalendar extends GenericUDF {

  private transient PrimitiveObjectInspector inputOI;
  private transient PrimitiveObjectInspector resultOI;
  private transient Converter converter;
  private transient SimpleDateFormat formatter;

  private DateWritableV2 dateWritable = new DateWritableV2();
  private TimestampWritableV2 timestampWritable = new TimestampWritableV2();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 1) {
      throw new UDFArgumentLengthException(
          "The function datetime_legacy_hybrid_calendar requires at least one argument, got "
              + arguments.length);
    }

    try {
      inputOI = (PrimitiveObjectInspector) arguments[0];
      PrimitiveCategory pc = inputOI.getPrimitiveCategory();
      switch (pc) {
        case DATE:
          formatter = new SimpleDateFormat("yyyy-MM-dd");
          formatter.setLenient(false);
          formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
          converter = ObjectInspectorConverters.getConverter(inputOI,
              PrimitiveObjectInspectorFactory.writableDateObjectInspector);
          resultOI = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
          break;
        case TIMESTAMP:
          formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
          formatter.setLenient(false);
          formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
          converter = ObjectInspectorConverters.getConverter(inputOI,
              PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);
          resultOI = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
          break;
        default:
          throw new UDFArgumentException(
              "datetime_legacy_hybrid_calendar only allows date or timestamp types");
      }
    } catch (ClassCastException e) {
      throw new UDFArgumentException(
          "The function datetime_legacy_hybrid_calendar takes only primitive types");
    }

    return resultOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object input = arguments[0].get();
    if (input == null) {
      return null;
    }

    input = converter.convert(input);

    switch (resultOI.getPrimitiveCategory()) {
      case DATE:
        Date date = ((DateWritableV2) input).get();
        java.sql.Date oldDate = new java.sql.Date(date.toEpochMilli());
        dateWritable.set(Date.valueOf(formatter.format(oldDate)));
        return dateWritable;
      case TIMESTAMP:
        Timestamp timestamp = ((TimestampWritableV2) input).getTimestamp();
        Timestamp adjustedTimestamp = Timestamp.valueOf(
            formatter.format(new java.sql.Timestamp(timestamp.toEpochMilli())));
        adjustedTimestamp.setNanos(timestamp.getNanos());
        timestampWritable.set(adjustedTimestamp);
        return timestampWritable;
      default:
        // Should never happen.
        throw new IllegalStateException("Unexpected type in evaluating datetime_legacy_hybrid_calendar: " +
            inputOI.getPrimitiveCategory());
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  @Override
  protected String getFuncName() {
    return "datetime_legacy_hybrid_calendar";
  }
}
