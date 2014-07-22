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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastLongToDate;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringToDate;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.DateConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;

/**
 * GenericUDFToDate
 */
@Description(name = "date",
    value = "CAST(<Date string> as DATE) - Returns the date represented by the date string.",
    extended = "date_string is a string in the format 'yyyy-MM-dd.'"
    + "Example:\n "
    + "  > SELECT CAST('2009-01-01' AS DATE) FROM src LIMIT 1;\n"
    + "  '2009-01-01'")
@VectorizedExpressions({CastStringToDate.class, CastLongToDate.class})
public class GenericUDFToDate extends GenericUDF {

  private transient PrimitiveObjectInspector argumentOI;
  private transient DateConverter dc;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 1) {
      throw new UDFArgumentLengthException(
          "The function CAST as DATE requires at least one argument, got "
          + arguments.length);
    }
    try {
      argumentOI = (PrimitiveObjectInspector) arguments[0];
      PrimitiveCategory pc = argumentOI.getPrimitiveCategory();
      PrimitiveGrouping pg =
          PrimitiveObjectInspectorUtils.getPrimitiveGrouping(pc);
      switch (pg) {
        case DATE_GROUP:
        case STRING_GROUP:
        case VOID_GROUP:
          break;
        default:
          throw new UDFArgumentException(
              "CAST as DATE only allows date,string, or timestamp types");
      }
    } catch (ClassCastException e) {
      throw new UDFArgumentException(
          "The function CAST as DATE takes only primitive types");
    }

    dc = new DateConverter(argumentOI,
        PrimitiveObjectInspectorFactory.writableDateObjectInspector);
    return PrimitiveObjectInspectorFactory.writableDateObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object o0 = arguments[0].get();
    if (o0 == null) {
      return null;
    }

    return dc.convert(o0);
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1);
    StringBuilder sb = new StringBuilder();
    sb.append("CAST( ");
    sb.append(children[0]);
    sb.append(" AS DATE)");
    return sb.toString();
  }

}
