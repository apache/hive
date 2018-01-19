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
 * GenericUDFAddMonths.
 *
 * Add a number of months to the date. The time part of the string will be
 * ignored.
 *
 */
@Description(name = "add_months",
    value = "_FUNC_(start_date, num_months) - Returns the date that is num_months after start_date.",
    extended = "start_date is a string in the format 'yyyy-MM-dd HH:mm:ss' or"
        + " 'yyyy-MM-dd'. num_months is a number. The time part of start_date is "
        + "ignored.\n"
        + "Example:\n " + " > SELECT _FUNC_('2009-08-31', 1) FROM src LIMIT 1;\n" + " '2009-09-30'")
@NDV(maxNdv = 250) // 250 seems to be reasonable upper limit for this
public class GenericUDFAddMonths extends GenericUDF {
  private transient Converter[] converters = new Converter[2];
  private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[2];
  private final Calendar calendar = Calendar.getInstance();
  private final Text output = new Text();
  private transient Integer numMonthsConst;
  private transient boolean isNumMonthsConst;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 2);

    checkArgPrimitive(arguments, 0);
    checkArgPrimitive(arguments, 1);

    checkArgGroups(arguments, 0, inputTypes, STRING_GROUP, DATE_GROUP, VOID_GROUP);
    checkArgGroups(arguments, 1, inputTypes, NUMERIC_GROUP, VOID_GROUP);

    obtainDateConverter(arguments, 0, inputTypes, converters);
    obtainIntConverter(arguments, 1, inputTypes, converters);

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
      numMonthV = getIntValue(arguments, 1, converters);
    }

    if (numMonthV == null) {
      return null;
    }

    int numMonthInt = numMonthV.intValue();
    Date date = getDateValue(arguments, 0, inputTypes, converters);
    if (date == null) {
      return null;
    }

    addMonth(date, numMonthInt);
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
    return "add_months";
  }

  protected Calendar addMonth(Date d, int numMonths) {
    calendar.setTime(d);

    boolean lastDatOfMonth = isLastDayOfMonth(calendar);

    calendar.add(Calendar.MONTH, numMonths);

    if (lastDatOfMonth) {
      int maxDd = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
      calendar.set(Calendar.DAY_OF_MONTH, maxDd);
    }
    return calendar;
  }

  protected boolean isLastDayOfMonth(Calendar cal) {
    int maxDd = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
    int dd = cal.get(Calendar.DAY_OF_MONTH);
    return dd == maxDd;
  }
}
