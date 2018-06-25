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

import java.sql.Date;
import java.sql.Timestamp;

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDate;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

public class TestGenericUDFDate extends TestCase {
  public void testStringToDate() throws HiveException {
    GenericUDFDate udf = new GenericUDFDate();
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] arguments = {valueOI};

    udf.initialize(arguments);
    DeferredObject valueObj = new DeferredJavaObject(new Text("2009-07-30"));
    DeferredObject[] args = {valueObj};
    DateWritable output = (DateWritable) udf.evaluate(args);

    assertEquals("to_date() test for STRING failed ", "2009-07-30", output.toString());

    // Try with null args
    DeferredObject[] nullArgs = { new DeferredJavaObject(null) };
    output = (DateWritable) udf.evaluate(nullArgs);
    assertNull("to_date() with null STRING", output);
  }

  public void testTimestampToDate() throws HiveException {
    GenericUDFDate udf = new GenericUDFDate();
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    ObjectInspector[] arguments = {valueOI};

    udf.initialize(arguments);
    DeferredObject valueObj = new DeferredJavaObject(new TimestampWritable(new Timestamp(109, 06,
        30, 4, 17, 52, 0)));
    DeferredObject[] args = {valueObj};
    DateWritable output = (DateWritable) udf.evaluate(args);

    assertEquals("to_date() test for TIMESTAMP failed ", "2009-07-30", output.toString());

    // Try with null args
    DeferredObject[] nullArgs = { new DeferredJavaObject(null) };
    output = (DateWritable) udf.evaluate(nullArgs);
    assertNull("to_date() with null TIMESTAMP", output);
  }

  public void testDateWritablepToDate() throws HiveException {
    GenericUDFDate udf = new GenericUDFDate();
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    ObjectInspector[] arguments = {valueOI};

    udf.initialize(arguments);
    DeferredObject valueObj = new DeferredJavaObject(new DateWritable(new Date(109, 06, 30)));
    DeferredObject[] args = {valueObj};
    DateWritable output = (DateWritable) udf.evaluate(args);

    assertEquals("to_date() test for DATEWRITABLE failed ", "2009-07-30", output.toString());

    // Try with null args
    DeferredObject[] nullArgs = { new DeferredJavaObject(null) };
    output = (DateWritable) udf.evaluate(nullArgs);
    assertNull("to_date() with null DATE", output);
  }

  public void testVoidToDate() throws HiveException {
    GenericUDFDate udf = new GenericUDFDate();
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableVoidObjectInspector;
    ObjectInspector[] arguments = {valueOI};

    udf.initialize(arguments);
    DeferredObject[] args = { new DeferredJavaObject(null) };
    DateWritable output = (DateWritable) udf.evaluate(args);

    // Try with null VOID
    assertNull("to_date() with null DATE ", output);

    // Try with erroneously generated VOID
    DeferredObject[] junkArgs = { new DeferredJavaObject(new Text("2015-11-22")) };
    try {
      udf.evaluate(junkArgs);
      fail("to_date() test with VOID non-null failed");
    } catch (UDFArgumentException udfae) {
      assertEquals("TO_DATE() received non-null object of VOID type", udfae.getMessage());
    }
  }

}
