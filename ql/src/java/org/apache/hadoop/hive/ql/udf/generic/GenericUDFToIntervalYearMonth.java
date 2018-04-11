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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringToIntervalYearMonth;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.HiveIntervalYearMonthConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
*
* GenericUDFIntervalYearMonth
*
* Example usage:
* ... CAST(<Interval string> as INTERVAL YEAR TO MONTH) ...
*
* Creates a HiveIntervalYearMonthWritable object using PrimitiveObjectInspectorConverter
*
*/
@Description(name = "interval_year_month",
  value = "CAST(<string> AS INTERVAL YEAR TO MONTH) - Returns the year-month interval represented by the string")
@VectorizedExpressions({CastStringToIntervalYearMonth.class})
public class GenericUDFToIntervalYearMonth extends GenericUDF {

  private transient PrimitiveObjectInspector argumentOI;
  private transient HiveIntervalYearMonthConverter tc;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 1) {
      throw new UDFArgumentLengthException(
          "The function INTERVAL_YEAR_MONTH requires at least one argument, got "
          + arguments.length);
    }
    try {
      argumentOI = (PrimitiveObjectInspector) arguments[0];
    } catch (ClassCastException e) {
      throw new UDFArgumentException(
          "The function INTERVAL_YEAR_MONTH takes only primitive types");
    }

    tc = new HiveIntervalYearMonthConverter(argumentOI,
        PrimitiveObjectInspectorFactory.writableHiveIntervalYearMonthObjectInspector);
    return PrimitiveObjectInspectorFactory.writableHiveIntervalYearMonthObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object o0 = arguments[0].get();
    if (o0 == null) {
      return null;
    }

    return tc.convert(o0);
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1);
    StringBuilder sb = new StringBuilder();
    sb.append("CAST( ");
    sb.append(children[0]);
    sb.append(" AS INTERVAL YEAR TO MONTH)");
    return sb.toString();
  }
}
