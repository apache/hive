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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDateDiff;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class TestGenericUDFDateDiff extends TestCase {
  public void testStringToDate() throws HiveException {
    GenericUDFDateDiff udf = new GenericUDFDateDiff();
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] arguments = {valueOI1, valueOI2};

    udf.initialize(arguments);
    DeferredObject valueObj1 = new DeferredJavaObject(new Text("2009-07-20"));
    DeferredObject valueObj2 = new DeferredJavaObject(new Text("2009-07-22"));
    DeferredObject[] args = {valueObj1, valueObj2};
    IntWritable output = (IntWritable) udf.evaluate(args);

    assertEquals("date_iff() test for STRING failed ", "-2", output.toString());

    // Test with null args
    args = new DeferredObject[] { new DeferredJavaObject(null), valueObj2 };
    assertNull("date_add() 1st arg null", udf.evaluate(args));

    args = new DeferredObject[] { valueObj1, new DeferredJavaObject(null) };
    assertNull("date_add() 2nd arg null", udf.evaluate(args));

    args = new DeferredObject[] { new DeferredJavaObject(null), new DeferredJavaObject(null) };
    assertNull("date_add() both args null", udf.evaluate(args));
  }

  public void testTimestampToDate() throws HiveException {
    GenericUDFDateDiff udf = new GenericUDFDateDiff();
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    ObjectInspector[] arguments = {valueOI1, valueOI2};

    udf.initialize(arguments);
    DeferredObject valueObj1 = new DeferredJavaObject(new TimestampWritable(new Timestamp(109, 06,
        20, 0, 0, 0, 0)));
    DeferredObject valueObj2 = new DeferredJavaObject(new TimestampWritable(new Timestamp(109, 06,
        17, 0, 0, 0, 0)));
    DeferredObject[] args = {valueObj1, valueObj2};
    IntWritable output = (IntWritable) udf.evaluate(args);

    assertEquals("datediff() test for TIMESTAMP failed ", "3", output.toString());

    // Test with null args
    args = new DeferredObject[] { new DeferredJavaObject(null), valueObj2 };
    assertNull("date_add() 1st arg null", udf.evaluate(args));

    args = new DeferredObject[] { valueObj1, new DeferredJavaObject(null) };
    assertNull("date_add() 2nd arg null", udf.evaluate(args));

    args = new DeferredObject[] { new DeferredJavaObject(null), new DeferredJavaObject(null) };
    assertNull("date_add() both args null", udf.evaluate(args));
  }

  public void testDateWritablepToDate() throws HiveException {
    GenericUDFDateDiff udf = new GenericUDFDateDiff();
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    ObjectInspector[] arguments = {valueOI1, valueOI2};


    udf.initialize(arguments);
    DeferredObject valueObj1 = new DeferredJavaObject(new DateWritable(new Date(109, 06, 20)));
    DeferredObject valueObj2 = new DeferredJavaObject(new DateWritable(new Date(109, 06, 10)));
    DeferredObject[] args = {valueObj1, valueObj2};
    IntWritable output = (IntWritable) udf.evaluate(args);

    assertEquals("datediff() test for DATEWRITABLE failed ", "10", output.toString());

    // Test with null args
    args = new DeferredObject[] { new DeferredJavaObject(null), valueObj2 };
    assertNull("date_add() 1st arg null", udf.evaluate(args));

    args = new DeferredObject[] { valueObj1, new DeferredJavaObject(null) };
    assertNull("date_add() 2nd arg null", udf.evaluate(args));

    args = new DeferredObject[] { new DeferredJavaObject(null), new DeferredJavaObject(null) };
    assertNull("date_add() both args null", udf.evaluate(args));
  }

}
