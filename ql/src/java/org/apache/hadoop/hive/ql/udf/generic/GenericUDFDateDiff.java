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
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDateDiffColCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDateDiffColScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDateDiffScalarCol;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TimestampConverter;
import org.apache.hadoop.io.IntWritable;

/**
 * UDFDateDiff.
 *
 * Calculate the difference in the number of days. The time part of the string
 * will be ignored. If dateString1 is earlier than dateString2, then the
 * result can be negative.
 *
 */
@Description(name = "datediff",
    value = "_FUNC_(date1, date2) - Returns the number of days between date1 and date2",
    extended = "date1 and date2 are strings in the format "
        + "'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'. The time parts are ignored."
        + "If date1 is earlier than date2, the result is negative.\n"
        + "Example:\n "
        + "  > SELECT _FUNC_('2009-07-30', '2009-07-31') FROM src LIMIT 1;\n"
        + "  1")
@VectorizedExpressions({VectorUDFDateDiffColScalar.class, VectorUDFDateDiffColCol.class, VectorUDFDateDiffScalarCol.class})
public class GenericUDFDateDiff extends GenericUDF {
  private transient SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private transient Converter inputConverter1;
  private transient Converter inputConverter2;
  private IntWritable output = new IntWritable();
  private transient PrimitiveCategory inputType1;
  private transient PrimitiveCategory inputType2;
  private IntWritable result = new IntWritable();

  public GenericUDFDateDiff() {
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
        "datediff() requires 2 argument, got " + arguments.length);
    }
    inputConverter1 = checkArguments(arguments, 0);
    inputConverter2 = checkArguments(arguments, 1);
    inputType1 = ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory();
    inputType2 = ((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory();
    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    return outputOI;
  }

  @Override
  public IntWritable evaluate(DeferredObject[] arguments) throws HiveException {
    output = evaluate(convertToDate(inputType1, inputConverter1, arguments[0]),
      convertToDate(inputType2, inputConverter2, arguments[1]));
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("datediff", children);
  }

  private Date convertToDate(PrimitiveCategory inputType, Converter converter, DeferredObject argument)
    throws HiveException {
    assert(converter != null);
    assert(argument != null);
    if (argument.get() == null) {
      return null;
    }
    Date date = new Date();
    switch (inputType) {
    case STRING:
    case VARCHAR:
    case CHAR:
      String dateString = converter.convert(argument.get()).toString();
      try {
        date = formatter.parse(dateString);
      } catch (ParseException e) {
        return null;
      }
      break;
    case TIMESTAMP:
      Timestamp ts = ((TimestampWritable) converter.convert(argument.get()))
        .getTimestamp();
      date.setTime(ts.getTime());
      break;
    case DATE:
      DateWritable dw = (DateWritable) converter.convert(argument.get());
      date = dw.get();
      break;
    default:
      throw new UDFArgumentException(
        "TO_DATE() only takes STRING/TIMESTAMP/DATEWRITABLE types, got " + inputType);
    }
    return date;
  }

  private Converter checkArguments(ObjectInspector[] arguments, int i) throws UDFArgumentException {
    if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
        "Only primitive type arguments are accepted but "
        + arguments[i].getTypeName() + " is passed. as first arguments");
    }
    PrimitiveCategory inputType = ((PrimitiveObjectInspector) arguments[i]).getPrimitiveCategory();
    Converter converter;
    switch (inputType) {
    case STRING:
    case VARCHAR:
    case CHAR:
      converter = ObjectInspectorConverters.getConverter(
        (PrimitiveObjectInspector) arguments[i],
        PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      break;
    case TIMESTAMP:
      converter = new TimestampConverter((PrimitiveObjectInspector) arguments[i],
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);
      break;
    case DATE:
      converter = ObjectInspectorConverters.getConverter((PrimitiveObjectInspector)arguments[i],
        PrimitiveObjectInspectorFactory.writableDateObjectInspector);
      break;
    default:
      throw new UDFArgumentException(
          " DATEDIFF() only takes STRING/TIMESTAMP/DATEWRITABLE types as " + (i + 1)
              + "-th argument, got " + inputType);
    }
    return converter;
  }

  private IntWritable evaluate(Date date, Date date2) {

    if (date == null || date2 == null) {
      return null;
    }
    // NOTE: This implementation avoids the extra-second problem
    // by comparing with UTC epoch and integer division.
    // 86400 is the number of seconds in a day
    long diffInMilliSeconds = date.getTime() - date2.getTime();
    result.set((int) (diffInMilliSeconds / (86400 * 1000)));
    return result;
  }
}
