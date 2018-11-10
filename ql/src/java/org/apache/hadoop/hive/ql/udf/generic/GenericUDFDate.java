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
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDateString;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDateTimestamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TimestampConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hive.common.util.DateParser;

/**
 * UDFDate.
 *
 */
@Description(name = "to_date",
    value = "_FUNC_(expr) - Extracts the date part of the date or datetime expression expr",
    extended = "Example:\n "
        + "  > SELECT _FUNC_('2009-07-30 04:17:52') FROM src LIMIT 1;\n"
        + "  '2009-07-30'")
@VectorizedExpressions({VectorUDFDateString.class, VectorUDFDateTimestamp.class})
public class GenericUDFDate extends GenericUDF {
  private transient TimestampConverter timestampConverter;
  private transient Converter textConverter;
  private transient Converter dateWritableConverter;
  private transient PrimitiveCategory inputType;
  private transient PrimitiveObjectInspector argumentOI;
  private transient DateParser dateParser = new DateParser();
  private transient final DateWritableV2 output = new DateWritableV2();
  private transient final Date date = new Date();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentLengthException(
        "to_date() requires 1 argument, got " + arguments.length);
    }
    if (arguments[0].getCategory() != Category.PRIMITIVE) {
      throw new UDFArgumentException("to_date() only accepts STRING/TIMESTAMP/DATEWRITABLE types, got "
          + arguments[0].getTypeName());
    }
    argumentOI = (PrimitiveObjectInspector) arguments[0];
    inputType = argumentOI.getPrimitiveCategory();
    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    switch (inputType) {
    case VOID:
      break;
    case CHAR:
    case VARCHAR:
    case STRING:
      inputType = PrimitiveCategory.STRING;
      textConverter = ObjectInspectorConverters.getConverter(
        argumentOI, PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      break;
    case TIMESTAMP:
      timestampConverter = new TimestampConverter(argumentOI,
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);
      break;
    case TIMESTAMPLOCALTZ:
    case DATE:
      dateWritableConverter = ObjectInspectorConverters.getConverter(argumentOI,
          PrimitiveObjectInspectorFactory.writableDateObjectInspector);
      break;
    default:
      throw new UDFArgumentException(
          "TO_DATE() only takes STRING/TIMESTAMP/DATEWRITABLE types, got " + inputType);
    }
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }

    switch (inputType) {
    case VOID:
      throw new UDFArgumentException("TO_DATE() received non-null object of VOID type");
    case STRING:
      String dateString = textConverter.convert(arguments[0].get()).toString();
      if (dateParser.parseDate(dateString, date)) {
        output.set(date);
      } else {
        return null;
      }
      break;
    case TIMESTAMP:
      Timestamp ts = ((TimestampWritableV2) timestampConverter.convert(arguments[0].get()))
          .getTimestamp();
      output.set(DateWritableV2.millisToDays(ts.toEpochMilli()));
      break;
    case TIMESTAMPLOCALTZ:
    case DATE:
      DateWritableV2 dw = (DateWritableV2) dateWritableConverter.convert(arguments[0].get());
      output.set(dw);
      break;
    default:
      throw new UDFArgumentException(
          "TO_DATE() only takes STRING/TIMESTAMP/DATEWRITABLE types, got " + inputType);
    }
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("to_date", children);
  }

}
