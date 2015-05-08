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
package org.apache.hadoop.hive.ql.udf.generic;

import junit.framework.TestCase;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class TestGenericUDFSha2 extends TestCase {

  public void testSha0Str() throws HiveException {
    GenericUDFSha2 udf = new GenericUDFSha2();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    IntWritable lenWr = new IntWritable(0);
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, lenWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyStr("ABC", lenWr,
        "b5d4045c3f466fa91fe2cc6abe79232a1a57cdf104f7a26e716e0a1e2789df78", udf);
    runAndVerifyStr("", lenWr, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        udf);
    // null
    runAndVerifyStr(null, lenWr, null, udf);
  }

  public void testSha0Bin() throws HiveException {
    GenericUDFSha2 udf = new GenericUDFSha2();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    IntWritable lenWr = new IntWritable(0);
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, lenWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyBin(new byte[] { 65, 66, 67 }, lenWr,
        "b5d4045c3f466fa91fe2cc6abe79232a1a57cdf104f7a26e716e0a1e2789df78", udf);
    runAndVerifyBin(new byte[0], lenWr,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", udf);
    // null
    runAndVerifyBin(null, lenWr, null, udf);
  }

  public void testSha200Str() throws HiveException {
    GenericUDFSha2 udf = new GenericUDFSha2();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    IntWritable lenWr = new IntWritable(200);
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, lenWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyStr("ABC", lenWr, null, udf);
  }

  public void testSha200Bin() throws HiveException {
    GenericUDFSha2 udf = new GenericUDFSha2();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    IntWritable lenWr = new IntWritable(200);
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, lenWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyBin(new byte[] { 65, 66, 67 }, lenWr, null, udf);
  }

  public void testSha256Str() throws HiveException {
    GenericUDFSha2 udf = new GenericUDFSha2();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    IntWritable lenWr = new IntWritable(256);
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, lenWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyStr("ABC", lenWr,
        "b5d4045c3f466fa91fe2cc6abe79232a1a57cdf104f7a26e716e0a1e2789df78", udf);
    runAndVerifyStr("", lenWr, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        udf);
    // null
    runAndVerifyStr(null, lenWr, null, udf);
  }

  public void testSha256Bin() throws HiveException {
    GenericUDFSha2 udf = new GenericUDFSha2();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    IntWritable lenWr = new IntWritable(256);
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, lenWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyBin(new byte[] { 65, 66, 67 }, lenWr,
        "b5d4045c3f466fa91fe2cc6abe79232a1a57cdf104f7a26e716e0a1e2789df78", udf);
    runAndVerifyBin(new byte[0], lenWr,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", udf);
    // null
    runAndVerifyBin(null, lenWr, null, udf);
  }

  public void testSha384Str() throws HiveException {
    GenericUDFSha2 udf = new GenericUDFSha2();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    IntWritable lenWr = new IntWritable(384);
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, lenWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyStr(
        "ABC",
        lenWr,
        "1e02dc92a41db610c9bcdc9b5935d1fb9be5639116f6c67e97bc1a3ac649753baba7ba021c813e1fe20c0480213ad371",
        udf);
    runAndVerifyStr(
        "",
        lenWr,
        "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b",
        udf);
    // null
    runAndVerifyStr(null, lenWr, null, udf);
  }

  public void testSha384Bin() throws HiveException {
    GenericUDFSha2 udf = new GenericUDFSha2();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    IntWritable lenWr = new IntWritable(384);
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, lenWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyBin(
        new byte[] { 65, 66, 67 },
        lenWr,
        "1e02dc92a41db610c9bcdc9b5935d1fb9be5639116f6c67e97bc1a3ac649753baba7ba021c813e1fe20c0480213ad371",
        udf);
    runAndVerifyBin(
        new byte[0],
        lenWr,
        "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b",
        udf);
    // null
    runAndVerifyBin(null, lenWr, null, udf);
  }

  public void testSha512Str() throws HiveException {
    GenericUDFSha2 udf = new GenericUDFSha2();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    IntWritable lenWr = new IntWritable(512);
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, lenWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyStr(
        "ABC",
        lenWr,
        "397118fdac8d83ad98813c50759c85b8c47565d8268bf10da483153b747a74743a58a90e85aa9f705ce6984ffc128db567489817e4092d050d8a1cc596ddc119",
        udf);
    runAndVerifyStr(
        "",
        lenWr,
        "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
        udf);
    // null
    runAndVerifyStr(null, lenWr, null, udf);
  }

  public void testSha512Bin() throws HiveException {
    GenericUDFSha2 udf = new GenericUDFSha2();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    IntWritable lenWr = new IntWritable(512);
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, lenWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyBin(
        new byte[] { 65, 66, 67 },
        lenWr,
        "397118fdac8d83ad98813c50759c85b8c47565d8268bf10da483153b747a74743a58a90e85aa9f705ce6984ffc128db567489817e4092d050d8a1cc596ddc119",
        udf);
    runAndVerifyBin(
        new byte[0],
        lenWr,
        "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
        udf);
    // null
    runAndVerifyBin(null, lenWr, null, udf);
  }

  public void testShaNullStr() throws HiveException {
    GenericUDFSha2 udf = new GenericUDFSha2();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    IntWritable lenWr = null;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, lenWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyStr("ABC", lenWr, null, udf);
  }

  public void testShaNullBin() throws HiveException {
    GenericUDFSha2 udf = new GenericUDFSha2();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    IntWritable lenWr = null;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, lenWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyBin(new byte[] { 65, 66, 67 }, lenWr, null, udf);
  }

  private void runAndVerifyStr(String str, IntWritable lenWr, String expResult, GenericUDFSha2 udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(str != null ? new Text(str) : null);
    DeferredObject valueObj1 = new DeferredJavaObject(lenWr);
    DeferredObject[] args = { valueObj0, valueObj1 };
    Text output = (Text) udf.evaluate(args);
    assertEquals("sha2() test ", expResult, output != null ? output.toString() : null);
  }

  private void runAndVerifyBin(byte[] b, IntWritable lenWr, String expResult, GenericUDFSha2 udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(b != null ? new BytesWritable(b) : null);
    DeferredObject valueObj1 = new DeferredJavaObject(lenWr);
    DeferredObject[] args = { valueObj0, valueObj1 };
    Text output = (Text) udf.evaluate(args);
    assertEquals("sha2() test ", expResult, output != null ? output.toString() : null);
  }
}
