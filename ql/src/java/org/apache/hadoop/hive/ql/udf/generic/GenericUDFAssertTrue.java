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
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.apache.hadoop.io.BooleanWritable;

/**
 * GenericUDFAssertTrue
 */
@Description(name = "assert_true",
    value = "_FUNC_(condition) - " +
            "Throw an exception if 'condition' is not true.",
    extended = "Example:\n "
    + "  > SELECT _FUNC_(x >= 0) FROM src LIMIT 1;\n"
    + "  NULL")
@UDFType(deterministic = false)
public class GenericUDFAssertTrue extends GenericUDF {
  private ObjectInspectorConverters.Converter conditionConverter = null;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentLengthException(
          "ASSERT_TRUE() expects one argument.");
    }
    if (arguments[0].getCategory() != Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, 
          "Argument to ASSERT_TRUE() should be primitive.");
    }
    conditionConverter = ObjectInspectorConverters.getConverter(arguments[0], 
        PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);

    return PrimitiveObjectInspectorFactory.writableVoidObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    BooleanWritable condition =
        (BooleanWritable)conditionConverter.convert(arguments[0].get());
    if (condition == null || !condition.get()) {
      throw new HiveException("ASSERT_TRUE(): assertion failed.");
    }
    return null;
  }

 @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("assert_true", children);
  }  
}
