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

import java.time.LocalDateTime;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

public class TestGenericUDFDateSub extends TestCase {
  public void testStringToDate() throws HiveException {
    GenericUDFDateSub udf = new GenericUDFDateSub();
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    ObjectInspector[] arguments = {valueOI1, valueOI2};

    udf.initialize(arguments);
    DeferredObject valueObj1 = new DeferredJavaObject(new Text("2009-07-20 04:17:52"));
    DeferredObject valueObj2 = new DeferredJavaObject(new Integer("2"));
    DeferredObject[] args = {valueObj1, valueObj2};
    DateWritableV2 output = (DateWritableV2) udf.evaluate(args);

    assertEquals("date_sub() test for STRING failed ", "2009-07-18", output.toString());

    // Test with null args
    args = new DeferredObject[] { new DeferredJavaObject(null), valueObj2 };
    assertNull("date_add() 1st arg null", udf.evaluate(args));

    args = new DeferredObject[] { valueObj1, new DeferredJavaObject(null) };
    assertNull("date_add() 2nd arg null", udf.evaluate(args));

    args = new DeferredObject[] { new DeferredJavaObject(null), new DeferredJavaObject(null) };
    assertNull("date_add() both args null", udf.evaluate(args));
  }

  public void testTimestampToDate() throws HiveException {
    GenericUDFDateSub udf = new GenericUDFDateSub();
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    ObjectInspector[] arguments = {valueOI1, valueOI2};

    udf.initialize(arguments);
    DeferredObject valueObj1 = new DeferredJavaObject(new TimestampWritableV2(
        Timestamp.valueOf(LocalDateTime.of(109, 06, 20, 4, 17, 52, 0).toString())));
    DeferredObject valueObj2 = new DeferredJavaObject(new Integer("3"));
    DeferredObject[] args = {valueObj1, valueObj2};
    DateWritableV2 output = (DateWritableV2) udf.evaluate(args);

    assertEquals("date_sub() test for TIMESTAMP failed ", "0109-06-17", output.toString());

    // Test with null args
    args = new DeferredObject[] { new DeferredJavaObject(null), valueObj2 };
    assertNull("date_add() 1st arg null", udf.evaluate(args));

    args = new DeferredObject[] { valueObj1, new DeferredJavaObject(null) };
    assertNull("date_add() 2nd arg null", udf.evaluate(args));

    args = new DeferredObject[] { new DeferredJavaObject(null), new DeferredJavaObject(null) };
    assertNull("date_add() both args null", udf.evaluate(args));
  }

  public void testDateWritablepToDate() throws HiveException {
    GenericUDFDateSub udf = new GenericUDFDateSub();
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    ObjectInspector[] arguments = {valueOI1, valueOI2};


    udf.initialize(arguments);
    DeferredObject valueObj1 = new DeferredJavaObject(new DateWritableV2(Date.of(109, 06, 20)));
    DeferredObject valueObj2 = new DeferredJavaObject(new Integer("4"));
    DeferredObject[] args = {valueObj1, valueObj2};
    DateWritableV2 output = (DateWritableV2) udf.evaluate(args);

    assertEquals("date_sub() test for DATEWRITABLE failed ", "0109-06-16", output.toString());

    // Test with null args
    args = new DeferredObject[] { new DeferredJavaObject(null), valueObj2 };
    assertNull("date_add() 1st arg null", udf.evaluate(args));

    args = new DeferredObject[] { valueObj1, new DeferredJavaObject(null) };
    assertNull("date_add() 2nd arg null", udf.evaluate(args));

    args = new DeferredObject[] { new DeferredJavaObject(null), new DeferredJavaObject(null) };
    assertNull("date_add() both args null", udf.evaluate(args));
  }

  public void testByteDataTypeAsDays() throws HiveException {
    GenericUDFDateSub udf = new GenericUDFDateSub();
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.javaByteObjectInspector;
    ObjectInspector[] arguments = {valueOI1, valueOI2};

    udf.initialize(arguments);
    DeferredObject valueObj1 = new DeferredJavaObject(new DateWritableV2(Date.of(109, 06, 20)));
    DeferredObject valueObj2 = new DeferredJavaObject(Byte.valueOf("4"));
    DeferredObject[] args = {valueObj1, valueObj2};
    DateWritableV2 output = (DateWritableV2) udf.evaluate(args);

    assertEquals("date_add() test for BYTE failed ", "0109-06-16", output.toString());
  }

  public void testShortDataTypeAsDays() throws HiveException {
    GenericUDFDateSub udf = new GenericUDFDateSub();
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.javaShortObjectInspector;
    ObjectInspector[] arguments = {valueOI1, valueOI2};

    udf.initialize(arguments);
    DeferredObject valueObj1 = new DeferredJavaObject(new DateWritableV2(Date.of(109, 06, 20)));
    DeferredObject valueObj2 = new DeferredJavaObject(new Short("4"));
    DeferredObject[] args = {valueObj1, valueObj2};
    DateWritableV2 output = (DateWritableV2) udf.evaluate(args);

    assertEquals("date_add() test for SHORT failed ", "0109-06-16", output.toString());
  }
}
