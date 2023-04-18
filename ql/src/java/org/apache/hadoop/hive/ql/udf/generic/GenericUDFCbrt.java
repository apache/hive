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

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.VOID_GROUP;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * GenericUDFCbrt.
 *
 */
@Description(
    name = "cbrt",
    value = "_FUNC_(double) - Returns the cube root of a double value.",
    extended = "Example:\n > SELECT _FUNC_(27.0);\n 3.0")
public class GenericUDFCbrt extends GenericUDF {
  private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[1];
  private transient Converter[] converters = new Converter[1];
  private final DoubleWritable output = new DoubleWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);

    checkArgPrimitive(arguments, 0);

    checkArgGroups(arguments, 0, inputTypes, NUMERIC_GROUP, VOID_GROUP);

    obtainDoubleConverter(arguments, 0, inputTypes, converters);

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Double val = getDoubleValue(arguments, 0, converters);
    if (val == null) {
      return null;
    }

    double cbrt = StrictMath.cbrt(val);
    output.set(cbrt);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  @Override
  protected String getFuncName() {
    return "cbrt";
  }
}