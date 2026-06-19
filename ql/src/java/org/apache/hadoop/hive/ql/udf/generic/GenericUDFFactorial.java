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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;

/**
 * GenericUDFFactorial
 *
 */
@Description(
    name = "factorial",
    value = "_FUNC_(int) - Returns n factorial. Valid n is [0..20].",
    extended = "Returns null if n is out of [0..20] range.\n"
        + "Example:\n > SELECT _FUNC_(5);\n 120")
public class GenericUDFFactorial extends GenericUDF {

  /** All long-representable factorials */
  static final long[] FACTORIALS = new long[] {
                     1l,                  1l,                   2l,
                     6l,                 24l,                 120l,
                   720l,               5040l,               40320l,
                362880l,            3628800l,            39916800l,
             479001600l,         6227020800l,         87178291200l,
         1307674368000l,     20922789888000l,     355687428096000l,
      6402373705728000l, 121645100408832000l, 2432902008176640000l };

  private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[1];
  private transient Converter[] converters = new Converter[1];
  private final LongWritable output = new LongWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);

    checkArgPrimitive(arguments, 0);

    checkArgGroups(arguments, 0, inputTypes, NUMERIC_GROUP, VOID_GROUP);

    obtainIntConverter(arguments, 0, inputTypes, converters);

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Integer val = getIntValue(arguments, 0, converters);
    if (val == null || val < 0 || val > 20) {
      return null;
    }

    long factorial = FACTORIALS[val];
    output.set(factorial);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  @Override
  protected String getFuncName() {
    return "factorial";
  }
}