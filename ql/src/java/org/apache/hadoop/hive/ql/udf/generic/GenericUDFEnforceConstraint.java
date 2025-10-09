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
import org.apache.hadoop.hive.ql.exec.errors.DataConstraintViolationError;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

/**
 * GenericUDFAbs.
 *
 */
@Description(name = "enforce_constraint",
    value = "_FUNC_(x) - Internal UDF to enforce CHECK and NOT NULL constraint",
    extended = "For internal use only")
public class GenericUDFEnforceConstraint extends GenericUDF {
  private final BooleanWritable resultBool = new BooleanWritable();
  private transient BooleanObjectInspector boi;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length > 1) {
      throw new UDFArgumentLengthException(
          "Invalid number of arguments. enforce_constraint UDF expected one argument but received: "
              + arguments.length);
    }

    if (!(arguments[0] instanceof BooleanObjectInspector)) {
      throw new UDFArgumentTypeException(0,
          String.format("%s only takes BOOLEAN, got %s", getFuncName(), arguments[0].getTypeName()));
    }
    boi = (BooleanObjectInspector) arguments[0];
    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    Object a = arguments[0].get();
    boolean result = boi.get(a);

    if(!result) {
      throw new DataConstraintViolationError(
          "Either CHECK or NOT NULL constraint violated!");
    }
    resultBool.set(true);
    return resultBool;
  }

  @Override
  protected String getFuncName() {
    return "enforce_constraint";
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }
}
