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

import static org.junit.Assert.assertEquals;

import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestGenericUDFAesEncrypt {

  @Test
  public void testAesEnc128ConstStr() throws HiveException {
    GenericUDFAesEncrypt udf = new GenericUDFAesEncrypt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    Text keyWr = new Text("1234567890123456");
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, keyWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyStr("ABC", keyWr, "y6Ss+zCYObpCbgfWfyNWTw==", udf);
    runAndVerifyStr("", keyWr, "BQGHoM3lqYcsurCRq3PlUw==", udf);
    // null
    runAndVerifyStr(null, keyWr, null, udf);
  }

  @Test
  public void testAesEnc256ConstStr() throws HiveException, NoSuchAlgorithmException {
    int maxKeyLen = Cipher.getMaxAllowedKeyLength("AES");
    // skip the test if Java Cryptography Extension (JCE) Unlimited Strength
    // Jurisdiction Policy Files not installed
    if (maxKeyLen < 256) {
      return;
    }
    GenericUDFAesEncrypt udf = new GenericUDFAesEncrypt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    Text keyWr = new Text("1234567890123456" + "1234567890123456");
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, keyWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyStr("ABC", keyWr, "nYfCuJeRd5eD60yXDw7WEA==", udf);
    runAndVerifyStr("", keyWr, "mVClVqZ6W4VF6b842FOgCA==", udf);
    // null
    runAndVerifyStr(null, keyWr, null, udf);
  }

  @Test
  public void testAesEnc128Str() throws HiveException {
    GenericUDFAesEncrypt udf = new GenericUDFAesEncrypt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    Text keyWr = new Text("1234567890123456");
    runAndVerifyStr("ABC", keyWr, "y6Ss+zCYObpCbgfWfyNWTw==", udf);
    runAndVerifyStr("", keyWr, "BQGHoM3lqYcsurCRq3PlUw==", udf);
    // null
    runAndVerifyStr(null, keyWr, null, udf);
  }

  @Test
  public void testAesEnc128ConstBin() throws HiveException {
    GenericUDFAesEncrypt udf = new GenericUDFAesEncrypt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    BytesWritable keyWr = new BytesWritable("1234567890123456".getBytes());
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.binaryTypeInfo, keyWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyBin(new byte[] { 65, 66, 67 }, keyWr, "y6Ss+zCYObpCbgfWfyNWTw==", udf);
    runAndVerifyBin(new byte[0], keyWr, "BQGHoM3lqYcsurCRq3PlUw==", udf);
    // null
    runAndVerifyBin(null, keyWr, null, udf);
  }

  @Test
  public void testAesEnc128Bin() throws HiveException {
    GenericUDFAesEncrypt udf = new GenericUDFAesEncrypt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    BytesWritable keyWr = new BytesWritable("1234567890123456".getBytes());
    runAndVerifyBin(new byte[] { 65, 66, 67 }, keyWr, "y6Ss+zCYObpCbgfWfyNWTw==", udf);
    runAndVerifyBin(new byte[0], keyWr, "BQGHoM3lqYcsurCRq3PlUw==", udf);
    // null
    runAndVerifyBin(null, keyWr, null, udf);
  }

  @Test
  public void testAesEnc192Bin() throws HiveException, NoSuchAlgorithmException {
    int maxKeyLen = Cipher.getMaxAllowedKeyLength("AES");
    // skip the test if Java Cryptography Extension (JCE) Unlimited Strength
    // Jurisdiction Policy Files not installed
    if (maxKeyLen < 192) {
      return;
    }
    GenericUDFAesEncrypt udf = new GenericUDFAesEncrypt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    BytesWritable keyWr = new BytesWritable(("1234567890123456" + "12345678").getBytes());
    runAndVerifyBin(new byte[] { 65, 66, 67 }, keyWr, "ucvvpP9r2/LfQ6BilQuFtA==", udf);
    runAndVerifyBin(new byte[0], keyWr, "KqMT3cF6VwSISMaUVUB4Qw==", udf);
    // null
    runAndVerifyBin(null, keyWr, null, udf);
  }

  @Test
  public void testAesEncKeyNullConstStr() throws HiveException {
    GenericUDFAesEncrypt udf = new GenericUDFAesEncrypt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    Text keyWr = null;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, keyWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyStr("ABC", keyWr, null, udf);
  }

  @Test
  public void testAesEncKeyNullStr() throws HiveException {
    GenericUDFAesEncrypt udf = new GenericUDFAesEncrypt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    Text keyWr = null;
    runAndVerifyStr("ABC", keyWr, null, udf);
  }

  @Test
  public void testAesEncKeyNullConstBin() throws HiveException {
    GenericUDFAesEncrypt udf = new GenericUDFAesEncrypt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    BytesWritable keyWr = null;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.binaryTypeInfo, keyWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyBin(new byte[] { 65, 66, 67 }, keyWr, null, udf);
  }

  @Test
  public void testAesEncKeyNullBin() throws HiveException {
    GenericUDFAesEncrypt udf = new GenericUDFAesEncrypt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    BytesWritable keyWr = null;
    runAndVerifyBin(new byte[] { 65, 66, 67 }, keyWr, null, udf);
  }

  private void runAndVerifyStr(String str, Text keyWr, String expResultBase64, GenericUDFAesEncrypt udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(str != null ? new Text(str) : null);
    DeferredObject valueObj1 = new DeferredJavaObject(keyWr);
    DeferredObject[] args = { valueObj0, valueObj1 };
    BytesWritable output = (BytesWritable) udf.evaluate(args);
    assertEquals("aes_encrypt() test ", expResultBase64, output != null ? copyBytesAndBase64(output) : null);
  }

  private void runAndVerifyBin(byte[] b, BytesWritable keyWr, String expResultBase64, GenericUDFAesEncrypt udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(b != null ? new BytesWritable(b) : null);
    DeferredObject valueObj1 = new DeferredJavaObject(keyWr);
    DeferredObject[] args = { valueObj0, valueObj1 };
    BytesWritable output = (BytesWritable) udf.evaluate(args);
    assertEquals("aes_encrypt() test ", expResultBase64, output != null ? copyBytesAndBase64(output) : null);
  }

  private String copyBytesAndBase64(BytesWritable bw) {
    int size = bw.getLength();
    byte[] bytes = new byte[size];
    System.arraycopy(bw.getBytes(), 0, bytes, 0, size);
    return new String(Base64.encodeBase64(bytes));
  }
}
