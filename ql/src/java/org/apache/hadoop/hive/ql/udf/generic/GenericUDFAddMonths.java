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

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.DATE_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.VOID_GROUP;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.DateUtils;

/**
 * GenericUDFAddMonths.
 *
 * Add a number of months to the date. The time part of the string will be
 * ignored.
 *
 */
@Description(name = "add_months",
    value = "_FUNC_(start_date, num_months, output_date_format) - "
        + "Returns the date that is num_months after start_date.",
    extended = "start_date is a string or timestamp indicating a valid date. "
        + "num_months is a number. output_date_format is an optional String which specifies the format for output.\n"
        + "The default output format is 'YYYY-MM-dd'.\n"
        + "Example:\n  > SELECT _FUNC_('2009-08-31', 1) FROM src LIMIT 1;\n" + " '2009-09-30'."
        + "\n  > SELECT _FUNC_('2017-12-31 14:15:16', 2, 'YYYY-MM-dd HH:mm:ss') LIMIT 1;\n"
        + "'2018-02-28 14:15:16'.\n")
@NDV(maxNdv = 250) // 250 seems to be reasonable upper limit for this
public class GenericUDFAddMonths extends GenericUDF {
  private transient Converter[] tsConverters = new Converter[3];
  private transient PrimitiveCategory[] tsInputTypes = new PrimitiveCategory[3];
  private transient Converter[] dtConverters = new Converter[3];
  private transient PrimitiveCategory[] dtInputTypes = new PrimitiveCategory[3];
  private final Text output = new Text();
  private transient SimpleDateFormat formatter = null;
  private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
  private transient Integer numMonthsConst;
  private transient boolean isNumMonthsConst;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 3);

    checkArgPrimitive(arguments, 0);
    checkArgPrimitive(arguments, 1);

    if (arguments.length == 3) {
      if (arguments[2] instanceof ConstantObjectInspector) {
        checkArgPrimitive(arguments, 2);
        checkArgGroups(arguments, 2, tsInputTypes, STRING_GROUP);
        String fmtStr = getConstantStringValue(arguments, 2);
        if (fmtStr != null) {
          formatter = new SimpleDateFormat(fmtStr);
          formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        }
      } else {
        throw new UDFArgumentTypeException(2, getFuncName() + " only takes constant as "
            + getArgOrder(2) + " argument");
      }
    }
    if (formatter == null) {
      //If the DateFormat is not provided by the user or is invalid, use the default format YYYY-MM-dd
      formatter = DateUtils.getDateFormat();
    }

    // the function should support both short date and full timestamp format
    // time part of the timestamp should not be skipped
    checkArgGroups(arguments, 0, tsInputTypes, STRING_GROUP, DATE_GROUP, VOID_GROUP);
    checkArgGroups(arguments, 0, dtInputTypes, STRING_GROUP, DATE_GROUP, VOID_GROUP);

    obtainTimestampConverter(arguments, 0, tsInputTypes, tsConverters);
    obtainDateConverter(arguments, 0, dtInputTypes, dtConverters);

    checkArgGroups(arguments, 1, tsInputTypes, NUMERIC_GROUP, VOID_GROUP);
    obtainIntConverter(arguments, 1, tsInputTypes, tsConverters);

    if (arguments[1] instanceof ConstantObjectInspector) {
      numMonthsConst = getConstantIntValue(arguments, 1);
      isNumMonthsConst = true;
    }

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Integer numMonthV;
    if (isNumMonthsConst) {
      numMonthV = numMonthsConst;
    } else {
      numMonthV = getIntValue(arguments, 1, tsConverters);
    }

    if (numMonthV == null) {
      return null;
    }

    int numMonthInt = numMonthV.intValue();

    // the function should support both short date and full timestamp format
    // time part of the timestamp should not be skipped
    Timestamp ts = getTimestampValue(arguments, 0, tsConverters);
    if (ts != null) {
      addMonth(ts, numMonthInt);
    } else {
      Date date = getDateValue(arguments, 0, dtInputTypes, dtConverters);
      if (date != null) {
        addMonth(date, numMonthInt);
      } else {
        return null;
      }
    }

    String res = formatter.format(calendar.getTime());

    output.set(res);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  @Override
  protected String getFuncName() {
    return "add_months";
  }

  private Calendar addMonth(Date d, int numMonths) {
    calendar.setTimeInMillis(d.toEpochMilli());

    return addMonth(numMonths);
  }

  private Calendar addMonth(Timestamp ts, int numMonths) {
    calendar.setTimeInMillis(ts.toEpochMilli());

    return addMonth(numMonths);
  }

  private Calendar addMonth(int numMonths) {
    boolean lastDatOfMonth = isLastDayOfMonth(calendar);

    calendar.add(Calendar.MONTH, numMonths);

    if (lastDatOfMonth) {
      int maxDd = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
      calendar.set(Calendar.DAY_OF_MONTH, maxDd);
    }
    return calendar;
  }

  private boolean isLastDayOfMonth(Calendar cal) {
    int maxDd = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
    int dd = cal.get(Calendar.DAY_OF_MONTH);
    return dd == maxDd;
  }
}
