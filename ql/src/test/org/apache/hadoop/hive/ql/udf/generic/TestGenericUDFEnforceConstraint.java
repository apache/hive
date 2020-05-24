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


import org.apache.hadoop.hive.ql.exec.errors.DataConstraintViolationError;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Test class for {@link GenericUDFEnforceConstraint}.
 */
public class TestGenericUDFEnforceConstraint {

  @Test
  public void testNull() throws HiveException {
    try {
      GenericUDFEnforceConstraint udf = new GenericUDFEnforceConstraint();
      ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
      ObjectInspector[] arguments = {valueOI };
      udf.initialize(arguments);

      BooleanWritable input = new BooleanWritable(false);
      GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(input) };
      udf.evaluate(args);
      fail("Unreachable line");
    } catch (DataConstraintViolationError e) {
      //DataConstraintViolationError is expected
      assertTrue(e.getMessage().contains("NOT NULL constraint violated!"));
    }
  }

  @Test
  public void testInvalidArgumentsLength() throws HiveException {
    try {
      GenericUDFEnforceConstraint udf = new GenericUDFEnforceConstraint();
      ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
      ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
      ObjectInspector[] arguments = {valueOI1, valueOI2 };
      udf.initialize(arguments);
      fail("Unreachable line");
    } catch (HiveException e) {
      //HiveException is expected
      assertTrue(e.getMessage().contains("Invalid number of arguments"));
    }
  }

  @Test
  public void testCorrect() throws HiveException {
    GenericUDFEnforceConstraint udf = new GenericUDFEnforceConstraint();
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    ObjectInspector[] arguments = {valueOI };
    udf.initialize(arguments);

    BooleanWritable input = new BooleanWritable(true);
    GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(input) };
    BooleanWritable writable = (BooleanWritable) udf.evaluate(args);
    assertTrue("Not expected result: expected [true] actual  [ " + writable.get() + " ]", writable.get());
  }
}
