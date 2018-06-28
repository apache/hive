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
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDateAddColCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDateAddColScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDateAddScalarCol;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TimestampConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hive.common.util.DateParser;

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
        + "  > SELECT _FUNC_('2009-07-30', 1) FROM src LIMIT 1;\n"
        + "  '2009-07-31'")
@VectorizedExpressions({VectorUDFDateAddColScalar.class, VectorUDFDateAddScalarCol.class, VectorUDFDateAddColCol.class})
public class GenericUDFDateAdd extends GenericUDF {
  private transient final DateParser dateParser = new DateParser();
  private transient final Date dateVal = new Date();
  private transient Converter dateConverter;
  private transient Converter daysConverter;
  private transient PrimitiveCategory inputType1;
  private transient PrimitiveCategory inputType2;
  private final DateWritableV2 output = new DateWritableV2();
  protected int signModifier = 1;  // 1 for addition, -1 for subtraction

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
        + arguments[1].getTypeName() + " is passed. as second arguments");
    }

    inputType1 = ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory();
    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    switch (inputType1) {
    case STRING:
    case VARCHAR:
    case CHAR:
      inputType1 = PrimitiveCategory.STRING;
      dateConverter = ObjectInspectorConverters.getConverter(
        (PrimitiveObjectInspector) arguments[0],
        PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      break;
    case TIMESTAMP:
      dateConverter = new TimestampConverter((PrimitiveObjectInspector) arguments[0],
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);
      break;
    case DATE:
      dateConverter = ObjectInspectorConverters.getConverter(
        (PrimitiveObjectInspector) arguments[0],
        PrimitiveObjectInspectorFactory.writableDateObjectInspector);
      break;
    default:
      throw new UDFArgumentException(
        " DATE_ADD() only takes STRING/TIMESTAMP/DATEWRITABLE types as first argument, got "
        + inputType1);
    }

    inputType2 = ((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory();
    switch (inputType2) {
      case BYTE:
        daysConverter = ObjectInspectorConverters.getConverter(
            (PrimitiveObjectInspector) arguments[1],
            PrimitiveObjectInspectorFactory.writableByteObjectInspector);
        break;
      case SHORT:
        daysConverter = ObjectInspectorConverters.getConverter(
            (PrimitiveObjectInspector) arguments[1],
            PrimitiveObjectInspectorFactory.writableShortObjectInspector);
        break;
      case INT:
        daysConverter = ObjectInspectorConverters.getConverter(
            (PrimitiveObjectInspector) arguments[1],
            PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        break;
      default:
        throw new UDFArgumentException(
            " DATE_ADD() only takes TINYINT/SMALLINT/INT types as second argument, got " + inputType2);
    }

    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }

    Object daysWritableObject = daysConverter.convert(arguments[1].get());
    if (daysWritableObject == null) {
      return null;
    }

    int toBeAdded;
    if (daysWritableObject instanceof ByteWritable) {
      toBeAdded = ((ByteWritable) daysWritableObject).get();
    } else if (daysWritableObject instanceof ShortWritable) {
      toBeAdded = ((ShortWritable) daysWritableObject).get();
    } else if (daysWritableObject instanceof IntWritable) {
      toBeAdded = ((IntWritable) daysWritableObject).get();
    } else {
      return null;
    }

    // Convert the first param into a DateWritableV2 value
    switch (inputType1) {
    case STRING:
      String dateString = dateConverter.convert(arguments[0].get()).toString();
      if (dateParser.parseDate(dateString, dateVal)) {
        output.set(dateVal);
      } else {
        return null;
      }
      break;
    case TIMESTAMP:
      Timestamp ts = ((TimestampWritableV2) dateConverter.convert(arguments[0].get()))
        .getTimestamp();
      output.set(DateWritableV2.millisToDays(ts.toEpochMilli()));
      break;
    case DATE:
      DateWritableV2 dw = (DateWritableV2) dateConverter.convert(arguments[0].get());
      output.set(dw.getDays());
      break;
    default:
      throw new UDFArgumentException(
        "DATE_ADD() only takes STRING/TIMESTAMP/DATEWRITABLE types, got " + inputType1);
    }

    int newDays = output.getDays() + (signModifier * toBeAdded);
    output.set(newDays);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("date_add", children);
  }
}
