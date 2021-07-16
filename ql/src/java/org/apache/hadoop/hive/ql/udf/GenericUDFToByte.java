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
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastDecimalToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastDoubleToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastTimestampToLong;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.BOOLEAN_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.DATE_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.VOID_GROUP;

/**
 * UDFToByte.
 *
 */
@VectorizedExpressions({ CastTimestampToLong.class, CastDoubleToLong.class, CastDecimalToLong.class,
                           CastStringToLong.class })
@Description(name = "tinyint",
             value = "_FUNC_(x) - converts it's parameter to _FUNC_",
             extended = "- x is NULL -> NULL\n" + "- byte, short, integer, long, float, double, decimal, timestamp:\n"
                 + "  x fits into the type _FUNC_ -> integer part of x\n" + "  undefined otherwise\n" + "- boolean:\n"
                 + "  true  -> 1\n" + "  false -> 0\n" + "- string:\n" + "  x is a valid integer -> x\n"
                 + "  NULL otherwise\n" + "Example:\n " + "  > SELECT _FUNC_(true);\n" + "  1")
public class GenericUDFToByte extends GenericUDF {
  private final transient ObjectInspectorConverters.Converter[] byteConvertors =
      new ObjectInspectorConverters.Converter[1];
  private final transient PrimitiveObjectInspector.PrimitiveCategory[] byteInputType =
      new PrimitiveObjectInspector.PrimitiveCategory[1];

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);
    checkArgPrimitive(arguments, 0);
    checkArgGroups(arguments, 0, byteInputType, NUMERIC_GROUP, DATE_GROUP, VOID_GROUP, BOOLEAN_GROUP, STRING_GROUP);
    obtainByteConverter(arguments, 0, byteConvertors);
    return PrimitiveObjectInspectorFactory.writableByteObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    return byteConvertors[0].convert(arguments[0].get());
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1);
    return "CAST( " + children[0] + " AS BYTE)";
  }

}
