/**
 * Licensed to the Apache Software Foundation (ASF) under one
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

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TimestampConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.io.Text;

/**
 * GenericUDFTrunc.
 *
 * Returns the first day of the month which the date belongs to.
 * The time part of the date will be  ignored.
 *
 */
@Description(name = "trunc",
value = "_FUNC_(date, fmt) - Returns returns date with the time portion of the day truncated "
    + "to the unit specified by the format model fmt. If you omit fmt, then date is truncated to "
    + "the nearest day. It now only supports 'MONTH'/'MON'/'MM' and 'YEAR'/'YYYY'/'YY' as format.",
extended = "date is a string in the format 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'."
    + " The time part of date is ignored.\n"
    + "Example:\n "
    + " > SELECT _FUNC_('2009-02-12', 'MM');\n" + "OK\n" + " '2009-02-01'" + "\n"
    + " > SELECT _FUNC_('2015-10-27', 'YEAR');\n" + "OK\n" + " '2015-01-01'")
public class GenericUDFTrunc extends GenericUDF {

  private transient SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private transient TimestampConverter timestampConverter;
  private transient Converter textConverter1;
  private transient Converter textConverter2;
  private transient Converter dateWritableConverter;
  private transient PrimitiveCategory inputType1;
  private transient PrimitiveCategory inputType2;
  private final Calendar calendar = Calendar.getInstance();
  private final Text output = new Text();
  private transient String fmtInput;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException("trunc() requires 2 argument, got " + arguments.length);
    }

    if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but "
          + arguments[0].getTypeName() + " is passed. as first arguments");
    }

    if (arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(1, "Only primitive type arguments are accepted but "
          + arguments[1].getTypeName() + " is passed. as second arguments");
    }

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    inputType1 = ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory();
    switch (inputType1) {
    case STRING:
    case VARCHAR:
    case CHAR:
    case VOID:
      inputType1 = PrimitiveCategory.STRING;
      textConverter1 = ObjectInspectorConverters.getConverter(
          arguments[0],
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      break;
    case TIMESTAMP:
      timestampConverter = new TimestampConverter((PrimitiveObjectInspector) arguments[0],
          PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);
      break;
    case DATE:
      dateWritableConverter = ObjectInspectorConverters.getConverter(
          arguments[0],
          PrimitiveObjectInspectorFactory.writableDateObjectInspector);
      break;
    default:
      throw new UDFArgumentTypeException(0,
          "TRUNC() only takes STRING/TIMESTAMP/DATEWRITABLE types as first argument, got "
              + inputType1);
    }

    inputType2 = ((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory();
    if (PrimitiveObjectInspectorUtils.getPrimitiveGrouping(inputType2)
        != PrimitiveGrouping.STRING_GROUP && PrimitiveObjectInspectorUtils.getPrimitiveGrouping(inputType2)
        != PrimitiveGrouping.VOID_GROUP) {
      throw new UDFArgumentTypeException(1,
          "trunk() only takes STRING/CHAR/VARCHAR types as second argument, got " + inputType2);
    }

    inputType2 = PrimitiveCategory.STRING;

    if (arguments[1] instanceof ConstantObjectInspector) {
      Object obj = ((ConstantObjectInspector) arguments[1]).getWritableConstantValue();
      fmtInput = obj != null ? obj.toString() : null;
    } else {
      textConverter2 = ObjectInspectorConverters.getConverter(
          arguments[1],
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException("trunc() requires 2 argument, got " + arguments.length);
    }

    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }

    if (textConverter2 != null) {
      fmtInput = textConverter2.convert(arguments[1].get()).toString();
    }

    Date date;
    switch (inputType1) {
    case STRING:
      String dateString = textConverter1.convert(arguments[0].get()).toString();
      try {
        date = formatter.parse(dateString.toString());
      } catch (ParseException e) {
        return null;
      }
      break;
    case TIMESTAMP:
      Timestamp ts = ((TimestampWritable) timestampConverter.convert(arguments[0].get()))
          .getTimestamp();
      date = ts;
      break;
    case DATE:
      DateWritable dw = (DateWritable) dateWritableConverter.convert(arguments[0].get());
      date = dw.get();
      break;
    default:
      throw new UDFArgumentTypeException(0,
          "TRUNC() only takes STRING/TIMESTAMP/DATEWRITABLE types, got " + inputType1);
    }

    if (evalDate(date) == null) {
      return null;
    }

    Date newDate = calendar.getTime();
    output.set(formatter.format(newDate));
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("trunc", children);
  }

  private Calendar evalDate(Date d) throws UDFArgumentException {
    calendar.setTime(d);
    if ("MONTH".equals(fmtInput) || "MON".equals(fmtInput) || "MM".equals(fmtInput)) {
      calendar.set(Calendar.DAY_OF_MONTH, 1);
      return calendar;
    } else if ("YEAR".equals(fmtInput) || "YYYY".equals(fmtInput) || "YY".equals(fmtInput)) {
      calendar.set(Calendar.MONTH, 0);
      calendar.set(Calendar.DAY_OF_MONTH, 1);
      return calendar;
    } else {
      return null;
    }
  }
}