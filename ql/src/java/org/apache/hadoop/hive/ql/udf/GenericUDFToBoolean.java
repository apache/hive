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

package org.apache.hadoop.hive.ql.udf;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDecimalToBoolean;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringToBoolean;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastDoubleToBooleanViaDoubleToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastLongToBooleanViaLongToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDateToBoolean;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastTimestampToBoolean;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.DATE_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.VOID_GROUP;

/**
 * UDFToBoolean.
 *
 */
@VectorizedExpressions({CastLongToBooleanViaLongToLong.class,
    CastDateToBoolean.class, CastTimestampToBoolean.class, CastStringToBoolean.class,
  CastDoubleToBooleanViaDoubleToLong.class, CastDecimalToBoolean.class, CastStringToLong.class})
@Description(
        name = "boolean",
        value = "_FUNC_(x) - converts it's parameter to _FUNC_",
        extended =
                "- x is NULL -> NULL\n" +
                "- byte, short, integer, long, float, double, decimal:\n" +
                "  x == 0 -> false\n" +
                "  x != 0 -> true\n" +
                "- string:\n" +
                "  x is '', 'false', 'no', 'zero', 'off' -> false\n" +
                "  true otherwise\n" +
                "- date: always NULL\n" +
                "- timestamp\n" +
                "  seconds or nanos are 0 -> false\n" +
                "  true otherwise\n" +
                "Example:\n "
                + "  > SELECT _FUNC_(0);\n"
                + "  false")
public class GenericUDFToBoolean extends GenericUDF {

  private final transient ObjectInspectorConverters.Converter[] booleanConverters = new ObjectInspectorConverters.Converter[1];
  private final transient PrimitiveObjectInspector.PrimitiveCategory[] booleanInputTypes = new PrimitiveObjectInspector.PrimitiveCategory[1];

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments,1,1);
    checkArgPrimitive(arguments,0);
    checkArgGroups(arguments,0, booleanInputTypes,NUMERIC_GROUP, VOID_GROUP,STRING_GROUP,DATE_GROUP);
    obtainDateConverter(arguments,0,booleanInputTypes,booleanConverters);
    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    return booleanConverters[0].convert(arguments[0].get());
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1);
    return "CAST( " + children[0] + " AS BOOLEAN)";
  }

}
