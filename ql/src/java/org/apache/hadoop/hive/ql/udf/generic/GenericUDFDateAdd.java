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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TimestampConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

/**
 * UDFDateAdd.
 *
 * Add a number of days to the date. The time part of the string will be
 * ignored.
 *
 * NOTE: This is a subset of what MySQL offers as:
 * http://dev.mysql.com/doc/refman
 * /5.1/en/date-and-time-functions.html#function_date-add
 *
 */
@Description(name = "date_add",
    value = "_FUNC_(start_date, num_days) - Returns the date that is num_days after start_date.",
    extended = "start_date is a string in the format 'yyyy-MM-dd HH:mm:ss' or"
        + " 'yyyy-MM-dd'. num_days is a number. The time part of start_date is "
        + "ignored.\n"
        + "Example:\n "
        + "  > SELECT _FUNC_('2009-30-07', 1) FROM src LIMIT 1;\n"
        + "  '2009-31-07'")
public class GenericUDFDateAdd extends GenericUDF {
  private transient SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private transient TimestampConverter timestampConverter;
  private transient Converter textConverter;
  private transient Converter dateWritableConverter;
  private transient Converter intWritableConverter;
  private transient PrimitiveCategory inputType1;
  private transient PrimitiveCategory inputType2;
  private final Calendar calendar = Calendar.getInstance();
  private final Text output = new Text();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
        "date_add() requires 2 argument, got " + arguments.length);
    }
    if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
        "Only primitive type arguments are accepted but "
        + arguments[0].getTypeName() + " is passed. as first arguments");
    }
    if (arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(1,
        "Only primitive type arguments are accepted but "
        + arguments[2].getTypeName() + " is passed. as second arguments");
    }

    inputType1 = ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory();
    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    switch (inputType1) {
    case STRING:
      textConverter = ObjectInspectorConverters.getConverter(
        (PrimitiveObjectInspector) arguments[0],
        PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      break;
    case TIMESTAMP:
      timestampConverter = new TimestampConverter((PrimitiveObjectInspector) arguments[0],
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);
      break;
    case DATE:
      dateWritableConverter = ObjectInspectorConverters.getConverter(
        (PrimitiveObjectInspector) arguments[0],
        PrimitiveObjectInspectorFactory.writableDateObjectInspector);
      break;
    default:
      throw new UDFArgumentException(
        " DATE_ADD() only takes STRING/TIMESTAMP/DATEWRITABLE types as first argument, got "
        + inputType1);
    }

    inputType2 = ((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory();
    if (inputType2 != PrimitiveCategory.INT) {
      throw new UDFArgumentException(
        " DATE_ADD() only takes INT types as second  argument, got " + inputType2);
    }
    intWritableConverter = ObjectInspectorConverters.getConverter(
      (PrimitiveObjectInspector) arguments[1],
      PrimitiveObjectInspectorFactory.writableIntObjectInspector);
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    IntWritable toBeAdded = (IntWritable) intWritableConverter.convert(arguments[1].get());
    switch (inputType1) {
    case STRING:
      String dateString = textConverter.convert(arguments[0].get()).toString();
      try {
        calendar.setTime(formatter.parse(dateString.toString()));
        calendar.add(Calendar.DAY_OF_MONTH, toBeAdded.get());
      } catch (ParseException e) {
        return null;
      }
      break;
    case TIMESTAMP:
      Timestamp ts = ((TimestampWritable) timestampConverter.convert(arguments[0].get()))
        .getTimestamp();
      calendar.setTime(ts);
      calendar.add(Calendar.DAY_OF_MONTH, toBeAdded.get());
      break;
    case DATE:
      DateWritable dw = (DateWritable) dateWritableConverter.convert(arguments[0].get());
      calendar.setTime(dw.get());
      calendar.add(Calendar.DAY_OF_MONTH, toBeAdded.get());
      break;
    default:
      throw new UDFArgumentException(
        "DATE_ADD() only takes STRING/TIMESTAMP/DATEWRITABLE types, got " + inputType1);
    }
    Date newDate = calendar.getTime();
    output.set(formatter.format(newDate));
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder();
    sb.append("date_add(");
    if (children.length > 0) {
      sb.append(children[0]);
      for (int i = 1; i < children.length; i++) {
        sb.append(", ");
        sb.append(children[i]);
      }
    }
    sb.append(")");
    return sb.toString();
  }
}
