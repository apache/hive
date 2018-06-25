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
package org.apache.hadoop.hive.common.type;

import java.sql.Timestamp;
import java.util.Random;
import java.util.Arrays;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.hadoop.hive.serde2.io.HiveDecimalWritableV1;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.impl.SerializationUtils;
import org.apache.hadoop.hive.common.type.RandomTypeUtil;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.util.TimestampUtils;

import com.google.code.tempusfugit.concurrency.annotations.*;
import com.google.code.tempusfugit.concurrency.*;

import org.junit.*;

import static org.junit.Assert.*;

public class TestHiveDecimalOrcSerializationUtils extends HiveDecimalTestBase {

  //------------------------------------------------------------------------------------------------

  @Test
  @Concurrent(count=4)
  public void testSerializationUtilsWriteRead() {
    testSerializationUtilsWriteRead("0.00");
    testSerializationUtilsWriteRead("1");
    testSerializationUtilsWriteRead("234.79");
    testSerializationUtilsWriteRead("-12.25");
    testSerializationUtilsWriteRead("99999999999999999999999999999999");
    testSerializationUtilsWriteRead("-99999999999999999999999999999999");
    testSerializationUtilsWriteRead("99999999999999999999999999999999999999");
    //                               12345678901234567890123456789012345678
    testSerializationUtilsWriteRead("-99999999999999999999999999999999999999");
    testSerializationUtilsWriteRead("999999999999.99999999999999999999");
    testSerializationUtilsWriteRead("-999999.99999999999999999999999999");
    testSerializationUtilsWriteRead("9999999999999999999999.9999999999999999");
    testSerializationUtilsWriteRead("-9999999999999999999999999999999.9999999");

    testSerializationUtilsWriteRead("4611686018427387903");  // 2^62 - 1
    testSerializationUtilsWriteRead("-4611686018427387903");
    testSerializationUtilsWriteRead("4611686018427387904");  // 2^62
    testSerializationUtilsWriteRead("-4611686018427387904");

    testSerializationUtilsWriteRead("42535295865117307932921825928971026431");  // 2^62*2^63 - 1
    testSerializationUtilsWriteRead("-42535295865117307932921825928971026431");
    testSerializationUtilsWriteRead("42535295865117307932921825928971026432");  // 2^62*2^63
    testSerializationUtilsWriteRead("-42535295865117307932921825928971026432");

    testSerializationUtilsWriteRead("54216721532321902598.70");
    testSerializationUtilsWriteRead("-906.62545207002374150309544832320");
  }

  private void testSerializationUtilsWriteRead(String string) {
    // System.out.println("TEST_FAST_SERIALIZATION_UTILS_WRITE_BIG_INTEGER ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ START ~~~~~~~~~~~~~~~~~");

    HiveDecimal dec = HiveDecimal.create(string);
    assertTrue(dec != null);
    // System.out.println("TEST_FAST_SERIALIZATION_UTILS_WRITE_BIG_INTEGER dec " + dec.toString());

    BigInteger bigInteger = dec.unscaledValue();
    int scale = dec.scale();
    // System.out.println("TEST_FAST_SERIALIZATION_UTILS_WRITE_BIG_INTEGER bigInteger " + bigInteger.toString());
    // System.out.println("TEST_FAST_SERIALIZATION_UTILS_WRITE_BIG_INTEGER scale " + scale);

    //---------------------------------------------------
    HiveDecimalV1 oldDec = HiveDecimalV1.create(string);
    assertTrue(oldDec != null);
    // System.out.println("TEST_FAST_SERIALIZATION_UTILS_WRITE_BIG_INTEGER oldDec " + oldDec.toString());

    BigInteger oldBigInteger = oldDec.unscaledValue();
    int oldScale = oldDec.scale();
    // System.out.println("TEST_FAST_SERIALIZATION_UTILS_WRITE_BIG_INTEGER oldBigInteger " + oldBigInteger.toString());
    // System.out.println("TEST_FAST_SERIALIZATION_UTILS_WRITE_BIG_INTEGER oldScale " + oldScale);
    //---------------------------------------------------

    long[] scratchLongs = new long[HiveDecimal.SCRATCH_LONGS_LEN];

    int which = 0;
    try {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      if (!dec.serializationUtilsWrite(
          outputStream, scratchLongs)) {
        // System.out.println("TEST_FAST_SERIALIZATION_UTILS_WRITE_BIG_INTEGER serializationUtilsWrite failed");
        fail();
      }
      byte[] bytes = outputStream.toByteArray();
  
      ByteArrayOutputStream outputStreamExpected = new ByteArrayOutputStream();
      SerializationUtils.writeBigInteger(outputStreamExpected, bigInteger);
      byte[] bytesExpected = outputStreamExpected.toByteArray();
  
      // System.out.println("TEST_FAST_SERIALIZATION_UTILS_WRITE_BIG_INTEGER check streams");
      // System.out.println("TEST_FAST_SERIALIZATION_UTILS_WRITE_BIG_INTEGER bytes1        " + displayBytes(bytes, 0, bytes.length));
      if (!StringExpr.equal(bytes, 0, bytes.length, bytesExpected, 0, bytesExpected.length)) {
        // Tailing zeroes difference ok.
        // System.out.println("TEST_FAST_SERIALIZATION_UTILS_WRITE_BIG_INTEGER streams not equal");
        // System.out.println("TEST_FAST_SERIALIZATION_UTILS_WRITE_BIG_INTEGER bytesExpected " + displayBytes(bytesExpected, 0, bytesExpected.length));
      }
      // Deserialize and check...
      which = 1;
      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
      BigInteger deserializedBigInteger = SerializationUtils.readBigInteger(byteArrayInputStream);

      which = 2;
      ByteArrayInputStream byteArrayInputStreamExpected = new ByteArrayInputStream(bytesExpected);
      BigInteger deserializedBigIntegerExpected = SerializationUtils.readBigInteger(byteArrayInputStreamExpected);
      // System.out.println("TEST_FAST_SERIALIZATION_UTILS_WRITE_BIG_INTEGER deserialized equals " +
      //    deserializedBigInteger.equals(deserializedBigIntegerExpected));
      if (!deserializedBigInteger.equals(deserializedBigIntegerExpected)) {
        // System.out.println("TEST_FAST_SERIALIZATION_UTILS_WRITE_BIG_INTEGER deserializedBigInteger " + deserializedBigInteger.toString() +
        //    " deserializedBigIntegerExpected " + deserializedBigIntegerExpected.toString());
        fail();
      }

      which = 3;
      ByteArrayInputStream byteArrayInputStreamRead = new ByteArrayInputStream(bytes);
      byte[] scratchBytes = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_SERIALIZATION_UTILS_READ];
      HiveDecimal readHiveDecimal =
          HiveDecimal.serializationUtilsRead(byteArrayInputStreamRead, scale, scratchBytes);
      assertTrue(readHiveDecimal != null);
      // System.out.println("TEST_FAST_SERIALIZATION_UTILS_WRITE_BIG_INTEGER read readHiveDecimal " + readHiveDecimal.toString() +
      //    " dec " + dec.toString() + " (scale parameter " + scale + ")");
      // System.out.println("TEST_FAST_SERIALIZATION_UTILS_WRITE_BIG_INTEGER read toString equals " +
      //    readHiveDecimal.toString().equals(dec.toString()));
      assertEquals(readHiveDecimal.toString(), dec.toString());
      // System.out.println("TEST_FAST_SERIALIZATION_UTILS_WRITE_BIG_INTEGER read equals " +
      //    readHiveDecimal.equals(dec));
      assertEquals(readHiveDecimal, dec);
    } catch (IOException e) {
      // System.out.println("TEST_FAST_SERIALIZATION_UTILS_WRITE_BIG_INTEGER " + e + " which " + which);
      fail();
    }
    // System.out.println("TEST_FAST_SERIALIZATION_UTILS_WRITE_BIG_INTEGER ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  END  ~~~~~~~~~~~~~~~~~");

  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomSerializationUtilsRead()
      throws IOException {
    doTestRandomSerializationUtilsRead(standardAlphabet);
  }

  @Test
  public void testRandomSerializationUtilsReadSparse()
      throws IOException {
    for (String digitAlphabet : sparseAlphabets) {
      doTestRandomSerializationUtilsRead(digitAlphabet);
    }
  }

  private void doTestRandomSerializationUtilsRead(String digitAlphabet)
      throws IOException {

    Random r = new Random(2389);
    for (int i = 0; i < POUND_FACTOR; i++) {
      BigInteger bigInteger = randHiveBigInteger(r, digitAlphabet);

      doTestSerializationUtilsRead(r, bigInteger);
    }
  }

  @Test
  public void testSerializationUtilsReadSpecial()
      throws IOException {
    Random r = new Random(9923);
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestSerializationUtilsRead(r, bigDecimal.unscaledValue());
    }
  }

  private void doTestSerializationUtilsRead(Random r, BigInteger bigInteger)
     throws IOException {

    // System.out.println("TEST_SERIALIZATION_UTILS_READ bigInteger " + bigInteger);

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigInteger);
    if (oldDec != null && isTenPowerBug(oldDec.toString())) {
      return;
    }
    HiveDecimal dec = HiveDecimal.create(bigInteger);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();
    // System.out.println("TEST_SERIALIZATION_UTILS_READ oldDec " + oldDec);
    // System.out.println("TEST_SERIALIZATION_UTILS_READ dec " + dec);

    Assert.assertEquals(bigInteger, oldDec.unscaledValue());
    Assert.assertEquals(bigInteger, dec.unscaledValue());

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    SerializationUtils.writeBigInteger(outputStream, bigInteger);
    byte[] bytes = outputStream.toByteArray();

    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
    BigInteger deserializedBigInteger =
        SerializationUtils.readBigInteger(byteArrayInputStream);

    // Verify SerializationUtils first.
    Assert.assertEquals(bigInteger, deserializedBigInteger);

    // Now HiveDecimal
    byte[] scratchBytes = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_SERIALIZATION_UTILS_READ];

    byteArrayInputStream = new ByteArrayInputStream(bytes);
    HiveDecimal resultDec =
        dec.serializationUtilsRead(
            byteArrayInputStream, dec.scale(),
            scratchBytes);
    assertTrue(resultDec != null);
    resultDec.validate();

    Assert.assertEquals(dec.toString(), resultDec.toString());

    //----------------------------------------------------------------------------------------------

    // Add scale.

    int scale = 0 + r.nextInt(38 + 1);
    BigDecimal bigDecimal = new BigDecimal(bigInteger, scale);

    oldDec = HiveDecimalV1.create(bigDecimal);
    dec = HiveDecimal.create(bigDecimal);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();
    // System.out.println("TEST_SERIALIZATION_UTILS_READ with scale oldDec " + oldDec);
    // System.out.println("TEST_SERIALIZATION_UTILS_READ with scale dec " + dec);

    outputStream = new ByteArrayOutputStream();
    SerializationUtils.writeBigInteger(outputStream, dec.unscaledValue());
    bytes = outputStream.toByteArray();

    // Now HiveDecimal
    byteArrayInputStream = new ByteArrayInputStream(bytes);
    resultDec =
        dec.serializationUtilsRead(
            byteArrayInputStream, dec.scale(),
            scratchBytes);
    assertTrue(resultDec != null);
    resultDec.validate();

    Assert.assertEquals(dec.toString(), resultDec.toString());
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRandomSerializationUtilsWrite()
      throws IOException {
    doTestRandomSerializationUtilsWrite(standardAlphabet, false);
  }

  @Test
  public void testRandomSerializationUtilsWriteFractionsOnly()
      throws IOException {
    doTestRandomSerializationUtilsWrite(standardAlphabet, true);
  }

  @Test
  public void testRandomSerializationUtilsWriteSparse()
      throws IOException {
    for (String digitAlphabet : sparseAlphabets) {
      doTestRandomSerializationUtilsWrite(digitAlphabet, false);
    }
  }

  private void doTestRandomSerializationUtilsWrite(String digitAlphabet, boolean fractionsOnly)
      throws IOException {

    Random r = new Random(823);
    for (int i = 0; i < POUND_FACTOR; i++) {
      BigInteger bigInteger = randHiveBigInteger(r, digitAlphabet);

      doTestSerializationUtilsWrite(r, bigInteger);
    }
  }

  @Test
  public void testSerializationUtilsWriteSpecial()
      throws IOException {
    Random r = new Random(998737);
    for (BigDecimal bigDecimal : specialBigDecimals) {
      doTestSerializationUtilsWrite(r, bigDecimal.unscaledValue());
    }
  }

  private void doTestSerializationUtilsWrite(Random r, BigInteger bigInteger)
     throws IOException {

    // System.out.println("TEST_SERIALIZATION_UTILS_WRITE bigInteger " + bigInteger);

    HiveDecimalV1 oldDec = HiveDecimalV1.create(bigInteger);
    if (oldDec != null && isTenPowerBug(oldDec.toString())) {
      return;
    }
    HiveDecimal dec = HiveDecimal.create(bigInteger);
    if (oldDec == null) {
      assertTrue(dec == null);
      return;
    }
    assertTrue(dec != null);
    dec.validate();
    // System.out.println("TEST_SERIALIZATION_UTILS_WRITE oldDec " + oldDec);
    // System.out.println("TEST_SERIALIZATION_UTILS_WRITE dec " + dec);

    Assert.assertEquals(bigInteger, oldDec.unscaledValue());
    Assert.assertEquals(bigInteger, dec.unscaledValue());

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    SerializationUtils.writeBigInteger(outputStream, bigInteger);
    byte[] bytes = outputStream.toByteArray();

    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
    BigInteger deserializedBigInteger =
        SerializationUtils.readBigInteger(byteArrayInputStream);

    // Verify SerializationUtils first.
    Assert.assertEquals(bigInteger, deserializedBigInteger);

    ByteArrayOutputStream decOutputStream = new ByteArrayOutputStream();

    long[] scratchLongs = new long[HiveDecimal.SCRATCH_LONGS_LEN];

    boolean successful =
        dec.serializationUtilsWrite(
            decOutputStream, scratchLongs);
    Assert.assertTrue(successful);
    byte[] decBytes = decOutputStream.toByteArray();

    if (!StringExpr.equal(bytes, 0, bytes.length, decBytes, 0, decBytes.length)) {
      // Tailing zeroes difference ok...
      // System.out.println("TEST_SERIALIZATION_UTILS_WRITE streams not equal");
      // System.out.println("TEST_SERIALIZATION_UTILS_WRITE bytes " + displayBytes(bytes, 0, bytes.length));
      // System.out.println("TEST_SERIALIZATION_UTILS_WRITE decBytes " + displayBytes(decBytes, 0, decBytes.length));
    }

    ByteArrayInputStream decByteArrayInputStream = new ByteArrayInputStream(decBytes);
    BigInteger decDeserializedBigInteger =
        SerializationUtils.readBigInteger(decByteArrayInputStream);

    Assert.assertEquals(bigInteger, decDeserializedBigInteger);
  }
}