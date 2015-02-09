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

import static org.apache.hadoop.hive.ql.udf.generic.GenericUDFNextDay.DayOfWeek.FRI;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDFNextDay.DayOfWeek.MON;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDFNextDay.DayOfWeek.SAT;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDFNextDay.DayOfWeek.SUN;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDFNextDay.DayOfWeek.THU;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDFNextDay.DayOfWeek.TUE;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDFNextDay.DayOfWeek.WED;

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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.io.Text;

/**
 * GenericUDFNextDay.
 *
 * Returns the first date which is later than start_date and named as indicated
 *
 */
@Description(name = "next_day",
    value = "_FUNC_(start_date, day_of_week) - Returns the first date"
        + " which is later than start_date and named as indicated.",
    extended = "start_date is a string in the format 'yyyy-MM-dd HH:mm:ss' or"
        + " 'yyyy-MM-dd'. day_of_week is day of the week (e.g. Mo, tue, FRIDAY)."
        + "Example:\n " + " > SELECT _FUNC_('2015-01-14', 'TU') FROM src LIMIT 1;\n" + " '2015-01-20'")
public class GenericUDFNextDay extends GenericUDF {
  private transient SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private transient TimestampConverter timestampConverter;
  private transient Converter textConverter0;
  private transient Converter textConverter1;
  private transient Converter dateWritableConverter;
  private transient PrimitiveCategory inputType1;
  private transient PrimitiveCategory inputType2;
  private final Calendar calendar = Calendar.getInstance();
  private final Text output = new Text();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException("next_day() requires 2 argument, got "
          + arguments.length);
    }
    if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but "
          + arguments[0].getTypeName() + " is passed as first arguments");
    }
    if (arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(1, "Only primitive type arguments are accepted but "
          + arguments[1].getTypeName() + " is passed as second arguments");
    }
    inputType1 = ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory();
    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    switch (inputType1) {
    case STRING:
    case VARCHAR:
    case CHAR:
      inputType1 = PrimitiveCategory.STRING;
      textConverter0 = ObjectInspectorConverters.getConverter(
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
      throw new UDFArgumentTypeException(0,
          "next_day() only takes STRING/TIMESTAMP/DATEWRITABLE types as first argument, got "
              + inputType1);
    }
    inputType2 = ((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory();
    if (PrimitiveObjectInspectorUtils.getPrimitiveGrouping(inputType2) != PrimitiveGrouping.STRING_GROUP) {
      throw new UDFArgumentTypeException(1,
          "next_day() only takes STRING_GROUP types as second argument, got " + inputType2);
    }
    textConverter1 = ObjectInspectorConverters.getConverter(
        (PrimitiveObjectInspector) arguments[1],
        PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }
    String dayOfWeek = textConverter1.convert(arguments[1].get()).toString();
    int dayOfWeekInt = getIntDayOfWeek(dayOfWeek);
    if (dayOfWeekInt == -1) {
      return null;
    }

    Date date;
    switch (inputType1) {
    case STRING:
      String dateString = textConverter0.convert(arguments[0].get()).toString();
      try {
        date = formatter.parse(dateString);
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
          "next_day() only takes STRING/TIMESTAMP/DATEWRITABLE types, got " + inputType1);
    }

    nextDay(date, dayOfWeekInt);
    Date newDate = calendar.getTime();
    output.set(formatter.format(newDate));
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder();
    sb.append("next_day(");
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

  protected Calendar nextDay(Date date, int dayOfWeek) {
    calendar.setTime(date);

    int currDayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);

    int daysToAdd;
    if (currDayOfWeek < dayOfWeek) {
      daysToAdd = dayOfWeek - currDayOfWeek;
    } else {
      daysToAdd = 7 - currDayOfWeek + dayOfWeek;
    }

    calendar.add(Calendar.DATE, daysToAdd);

    return calendar;
  }

  protected int getIntDayOfWeek(String dayOfWeek) throws UDFArgumentException {
    if (MON.matches(dayOfWeek)) {
      return Calendar.MONDAY;
    }
    if (TUE.matches(dayOfWeek)) {
      return Calendar.TUESDAY;
    }
    if (WED.matches(dayOfWeek)) {
      return Calendar.WEDNESDAY;
    }
    if (THU.matches(dayOfWeek)) {
      return Calendar.THURSDAY;
    }
    if (FRI.matches(dayOfWeek)) {
      return Calendar.FRIDAY;
    }
    if (SAT.matches(dayOfWeek)) {
      return Calendar.SATURDAY;
    }
    if (SUN.matches(dayOfWeek)) {
      return Calendar.SUNDAY;
    }
    return -1;
  }

  public static enum DayOfWeek {
    MON ("MO", "MON", "MONDAY"),
    TUE ("TU", "TUE", "TUESDAY"),
    WED ("WE", "WED", "WEDNESDAY"),
    THU ("TH", "THU", "THURSDAY"),
    FRI ("FR", "FRI", "FRIDAY"),
    SAT ("SA", "SAT", "SATURDAY"),
    SUN ("SU", "SUN", "SUNDAY");

    private String name2;
    private String name3;
    private String fullName;

    private DayOfWeek(String name2, String name3, String fullName) {
      this.name2 = name2;
      this.name3 = name3;
      this.fullName = fullName;
    }

    public String getName2() {
      return name2;
    }

    public String getName3() {
      return name3;
    }

    public String getFullName() {
      return fullName;
    }

    public boolean matches(String dayOfWeek) {
      if (dayOfWeek.length() == 2) {
        return name2.equalsIgnoreCase(dayOfWeek);
      }
      if (dayOfWeek.length() == 3) {
        return name3.equalsIgnoreCase(dayOfWeek);
      }
      return fullName.equalsIgnoreCase(dayOfWeek);
    }
  }
}
