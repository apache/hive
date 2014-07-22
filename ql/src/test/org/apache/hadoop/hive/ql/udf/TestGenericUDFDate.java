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

package org.apache.hadoop.hive.ql.udf;

import java.sql.Date;
import java.sql.Timestamp;

import junit.framework.TestCase;

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
    Text output = (Text) udf.evaluate(args);

    assertEquals("to_date() test for STRING failed ", "2009-07-30", output.toString());

    // Try with null args
    DeferredObject[] nullArgs = { new DeferredJavaObject(null) };
    output = (Text) udf.evaluate(nullArgs);
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
    Text output = (Text) udf.evaluate(args);

    assertEquals("to_date() test for TIMESTAMP failed ", "2009-07-30", output.toString());

    // Try with null args
    DeferredObject[] nullArgs = { new DeferredJavaObject(null) };
    output = (Text) udf.evaluate(nullArgs);
    assertNull("to_date() with null TIMESTAMP", output);
  }

  public void testDateWritablepToDate() throws HiveException {
    GenericUDFDate udf = new GenericUDFDate();
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    ObjectInspector[] arguments = {valueOI};

    udf.initialize(arguments);
    DeferredObject valueObj = new DeferredJavaObject(new DateWritable(new Date(109, 06, 30)));
    DeferredObject[] args = {valueObj};
    Text output = (Text) udf.evaluate(args);

    assertEquals("to_date() test for DATEWRITABLE failed ", "2009-07-30", output.toString());

    // Try with null args
    DeferredObject[] nullArgs = { new DeferredJavaObject(null) };
    output = (Text) udf.evaluate(nullArgs);
    assertNull("to_date() with null DATE", output);
  }

}
