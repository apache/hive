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
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastStringToDate;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastTimestampToDate;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.DATE_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.VOID_GROUP;

/**
 * GenericUDFToDate
 */
@Description(name = "date",
    value = "CAST(<Date string> as DATE) - Returns the date represented by the date string.",
    extended = "date_string is a string in the format 'yyyy-MM-dd.'"
    + "Example:\n "
    + "  > SELECT CAST('2009-01-01' AS DATE) FROM src LIMIT 1;\n"
    + "  '2009-01-01'")
@VectorizedExpressions({CastStringToDate.class, CastTimestampToDate.class})
public class GenericUDFToDate extends GenericUDF {

  private final transient ObjectInspectorConverters.Converter[] dateConverters = new ObjectInspectorConverters.Converter[1];
  private final transient PrimitiveCategory[] dateInputTypes = new PrimitiveCategory[1];

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments,1,1);
    checkArgPrimitive(arguments,0);
    checkArgGroups(arguments,0,dateInputTypes,DATE_GROUP,STRING_GROUP,VOID_GROUP);
    obtainDateConverter(arguments,0,dateInputTypes,dateConverters);
    return PrimitiveObjectInspectorFactory.writableDateObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    return dateConverters[0].convert(arguments[0].get());
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1);
    return "CAST( " + children[0] + " AS DATE)";
  }

}
