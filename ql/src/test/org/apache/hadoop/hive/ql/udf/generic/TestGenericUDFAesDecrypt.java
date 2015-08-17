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

import static org.junit.Assert.assertEquals;

import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestGenericUDFAesDecrypt {

  @Test
  public void testAesDec128ConstStr() throws HiveException {
    GenericUDFAesDecrypt udf = new GenericUDFAesDecrypt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    Text keyWr = new Text("1234567890123456");
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, keyWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyStr("y6Ss+zCYObpCbgfWfyNWTw==", keyWr, "ABC", udf);
    runAndVerifyStr("BQGHoM3lqYcsurCRq3PlUw==", keyWr, "", udf);
    // null
    runAndVerifyStr(null, keyWr, null, udf);
  }

  @Test
  public void testAesDec256ConstStr() throws HiveException, NoSuchAlgorithmException {
    int maxKeyLen = Cipher.getMaxAllowedKeyLength("AES");
    // skip the test if Java Cryptography Extension (JCE) Unlimited Strength
    // Jurisdiction Policy Files not installed
    if (maxKeyLen < 256) {
      return;
    }
    GenericUDFAesDecrypt udf = new GenericUDFAesDecrypt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    Text keyWr = new Text("1234567890123456" + "1234567890123456");
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, keyWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyStr("nYfCuJeRd5eD60yXDw7WEA==", keyWr, "ABC", udf);
    runAndVerifyStr("mVClVqZ6W4VF6b842FOgCA==", keyWr, "", udf);
    // null
    runAndVerifyStr(null, keyWr, null, udf);
  }

  @Test
  public void testAesDec128Str() throws HiveException {
    GenericUDFAesDecrypt udf = new GenericUDFAesDecrypt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    Text keyWr = new Text("1234567890123456");
    runAndVerifyStr("y6Ss+zCYObpCbgfWfyNWTw==", keyWr, "ABC", udf);
    runAndVerifyStr("BQGHoM3lqYcsurCRq3PlUw==", keyWr, "", udf);
    // null
    runAndVerifyStr(null, keyWr, null, udf);
  }

  @Test
  public void testAesDec128ConstBin() throws HiveException {
    GenericUDFAesDecrypt udf = new GenericUDFAesDecrypt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    BytesWritable keyWr = new BytesWritable("1234567890123456".getBytes());
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.binaryTypeInfo, keyWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyBin("y6Ss+zCYObpCbgfWfyNWTw==", keyWr, "ABC", udf);
    runAndVerifyBin("BQGHoM3lqYcsurCRq3PlUw==", keyWr, "", udf);
    // null
    runAndVerifyBin(null, keyWr, null, udf);
  }

  @Test
  public void testAesDec128Bin() throws HiveException {
    GenericUDFAesDecrypt udf = new GenericUDFAesDecrypt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    BytesWritable keyWr = new BytesWritable("1234567890123456".getBytes());
    runAndVerifyBin("y6Ss+zCYObpCbgfWfyNWTw==", keyWr, "ABC", udf);
    runAndVerifyBin("BQGHoM3lqYcsurCRq3PlUw==", keyWr, "", udf);
    // null
    runAndVerifyBin(null, keyWr, null, udf);
  }

  @Test
  public void testAesDec192Bin() throws HiveException, NoSuchAlgorithmException {
    int maxKeyLen = Cipher.getMaxAllowedKeyLength("AES");
    // skip the test if Java Cryptography Extension (JCE) Unlimited Strength
    // Jurisdiction Policy Files not installed
    if (maxKeyLen < 192) {
      return;
    }
    GenericUDFAesDecrypt udf = new GenericUDFAesDecrypt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    BytesWritable keyWr = new BytesWritable(("1234567890123456" + "12345678").getBytes());
    runAndVerifyBin("ucvvpP9r2/LfQ6BilQuFtA==", keyWr, "ABC", udf);
    runAndVerifyBin("KqMT3cF6VwSISMaUVUB4Qw==", keyWr, "", udf);
    // null
    runAndVerifyBin(null, keyWr, null, udf);
  }

  @Test
  public void testAesDecKeyNullConstStr() throws HiveException {
    GenericUDFAesDecrypt udf = new GenericUDFAesDecrypt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    Text keyWr = null;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, keyWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyStr("y6Ss+zCYObpCbgfWfyNWTw==", keyWr, null, udf);
  }

  @Test
  public void testAesDecKeyNullStr() throws HiveException {
    GenericUDFAesDecrypt udf = new GenericUDFAesDecrypt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    Text keyWr = null;
    runAndVerifyStr("y6Ss+zCYObpCbgfWfyNWTw==", keyWr, null, udf);
  }

  @Test
  public void testAesDecKeyNullConstBin() throws HiveException {
    GenericUDFAesDecrypt udf = new GenericUDFAesDecrypt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    BytesWritable keyWr = null;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.binaryTypeInfo, keyWr);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyBin("y6Ss+zCYObpCbgfWfyNWTw==", keyWr, null, udf);
  }

  @Test
  public void testAesDecKeyNullBin() throws HiveException {
    GenericUDFAesDecrypt udf = new GenericUDFAesDecrypt();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    BytesWritable keyWr = null;
    runAndVerifyBin("y6Ss+zCYObpCbgfWfyNWTw==", keyWr, null, udf);
  }

  private void runAndVerifyStr(String strBase64, Text keyWr, String expResult, GenericUDFAesDecrypt udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(
        strBase64 != null ? new BytesWritable(Base64.decodeBase64(strBase64)) : null);
    DeferredObject valueObj1 = new DeferredJavaObject(keyWr);
    DeferredObject[] args = { valueObj0, valueObj1 };
    BytesWritable output = (BytesWritable) udf.evaluate(args);
    String expResultHex = expResult == null ? null : Hex.encodeHexString(expResult.getBytes());
    assertEquals("aes_decrypt() test ", expResultHex, output != null ? copyBytesAndHex(output) : null);
  }

  private void runAndVerifyBin(String strBase64, BytesWritable keyWr, String expResult, GenericUDFAesDecrypt udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(
        strBase64 != null ? new BytesWritable(Base64.decodeBase64(strBase64)) : null);
    DeferredObject valueObj1 = new DeferredJavaObject(keyWr);
    DeferredObject[] args = { valueObj0, valueObj1 };
    BytesWritable output = (BytesWritable) udf.evaluate(args);
    String expResultHex = expResult == null ? null : Hex.encodeHexString(expResult.getBytes());
    assertEquals("aes_decrypt() test ", expResultHex, output != null ? copyBytesAndHex(output) : null);
  }

  private String copyBytesAndHex(BytesWritable bw) {
    int size = bw.getLength();
    byte[] bytes = new byte[size];
    System.arraycopy(bw.getBytes(), 0, bytes, 0, size);
    return Hex.encodeHexString(bytes);
  }
}
