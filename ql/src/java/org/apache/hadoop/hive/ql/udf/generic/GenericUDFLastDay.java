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
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.VOID_GROUP;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/**
 * GenericUDFLastDay.
 *
 * Returns the last day of the month which the date belongs to.
 * The time part of the date will be  ignored.
 *
 */
@Description(name = "last_day",
    value = "_FUNC_(date) - Returns the last day of the month which the date belongs to.",
    extended = "date is a string in the format 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'."
        + " The time part of date is ignored.\n"
        + "Example:\n " + " > SELECT _FUNC_('2009-01-12') FROM src LIMIT 1;\n" + " '2009-01-31'")
public class GenericUDFLastDay extends GenericUDF {
  private transient Converter[] converters = new Converter[1];
  private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[1];
  private final Date date = new Date();
  private final Text output = new Text();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);

    checkArgPrimitive(arguments, 0);

    checkArgGroups(arguments, 0, inputTypes, STRING_GROUP, DATE_GROUP, VOID_GROUP);

    obtainDateConverter(arguments, 0, inputTypes, converters);

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Date d = getDateValue(arguments, 0, inputTypes, converters);
    if (d == null) {
      return null;
    }

    lastDay(d);
    output.set(date.toString());
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  @Override
  protected String getFuncName() {
    return "last_day";
  }

  protected Date lastDay(Date d) {
    date.setTimeInDays(d.toEpochDay());
    date.setDayOfMonth(date.lengthOfMonth());
    return date;
  }
}
