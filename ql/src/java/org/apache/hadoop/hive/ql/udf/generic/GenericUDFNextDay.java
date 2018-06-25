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

import static org.apache.hadoop.hive.ql.udf.generic.GenericUDFNextDay.DayOfWeek.FRI;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDFNextDay.DayOfWeek.MON;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDFNextDay.DayOfWeek.SAT;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDFNextDay.DayOfWeek.SUN;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDFNextDay.DayOfWeek.THU;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDFNextDay.DayOfWeek.TUE;
import static org.apache.hadoop.hive.ql.udf.generic.GenericUDFNextDay.DayOfWeek.WED;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.DATE_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.VOID_GROUP;

import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.DateUtils;

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
  private transient Converter[] converters = new Converter[2];
  private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[2];
  private final Calendar calendar = Calendar.getInstance();
  private final Text output = new Text();
  private transient int dayOfWeekIntConst;
  private transient boolean isDayOfWeekConst;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 2);

    checkArgPrimitive(arguments, 0);
    checkArgPrimitive(arguments, 1);

    checkArgGroups(arguments, 0, inputTypes, STRING_GROUP, DATE_GROUP, VOID_GROUP);
    checkArgGroups(arguments, 1, inputTypes, STRING_GROUP, VOID_GROUP);

    obtainDateConverter(arguments, 0, inputTypes, converters);
    obtainStringConverter(arguments, 1, inputTypes, converters);

    if (arguments[1] instanceof ConstantObjectInspector) {
      String dayOfWeek = getConstantStringValue(arguments, 1);
      isDayOfWeekConst = true;
      dayOfWeekIntConst = getIntDayOfWeek(dayOfWeek);
    }

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    int dayOfWeekInt;
    if (isDayOfWeekConst) {
      dayOfWeekInt = dayOfWeekIntConst;
    } else {
      String dayOfWeek = getStringValue(arguments, 1, converters);
      dayOfWeekInt = getIntDayOfWeek(dayOfWeek);
    }
    if (dayOfWeekInt == -1) {
      return null;
    }

    Date date = getDateValue(arguments, 0, inputTypes, converters);
    if (date == null) {
      return null;
    }

    nextDay(date, dayOfWeekInt);
    Date newDate = calendar.getTime();
    output.set(DateUtils.getDateFormat().format(newDate));
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  @Override
  protected String getFuncName() {
    return "next_day";
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
    if (dayOfWeek == null) {
      return -1;
    }
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
    MON("MO", "MON", "MONDAY"), TUE("TU", "TUE", "TUESDAY"), WED("WE", "WED", "WEDNESDAY"), THU(
        "TH", "THU", "THURSDAY"), FRI("FR", "FRI", "FRIDAY"), SAT("SA", "SAT", "SATURDAY"), SUN(
        "SU", "SUN", "SUNDAY");

    private final String name2;
    private final String name3;
    private final String fullName;

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
