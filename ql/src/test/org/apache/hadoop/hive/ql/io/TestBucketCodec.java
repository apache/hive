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
package org.apache.hadoop.hive.ql.io;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestBucketCodec {

  /**
   * There are only 2 valid codec [0,1].
   */
  @Test(expected = IllegalArgumentException.class)
  public void testGetBucketCodecInvalidVersion() {
    BucketCodec.getCodec(4);
  }

  /**
   * Test Bucket Codec version 0. This is the "legacy" version.
   */
  @Test
  public void testGetBucketCodecVersion0() {
    final BucketCodec codec = BucketCodec.getCodec(0);
    assertEquals(BucketCodec.V0, codec);
    assertEquals(0, codec.getVersion());

    // Returns the provided value
    assertEquals(7, codec.decodeWriterId(7));

    // Always returns 0
    assertEquals(0, codec.decodeStatementId(100));
    assertEquals(0, codec.decodeStatementId(-10));

    // Returns the bucket value from Options
    final AcidOutputFormat.Options options = new AcidOutputFormat.Options(null).bucket(7);
    assertEquals(7, codec.encode(options));
  }

  /**
   * Test Bucket Codec version 1. Represents format of "bucket" property in Hive
   * 3.0.
   */
  @Test
  public void testGetBucketCodecVersion1() {
    final BucketCodec codec = BucketCodec.getCodec(1);
    assertEquals(BucketCodec.V1, codec);
    assertEquals(1, codec.getVersion());

    assertEquals(2748, codec.decodeWriterId(0x0ABC0000));

    assertEquals(2748, codec.decodeStatementId(0x00000ABC));

    final AcidOutputFormat.Options options = new AcidOutputFormat.Options(null).bucket(7).statementId(16);
    assertEquals(537329680, codec.encode(options));

    // Statement ID of -1 is acceptable and has the same affect as a value of 0
    final AcidOutputFormat.Options optionsNeg = new AcidOutputFormat.Options(null).bucket(7).statementId(-1);
    final AcidOutputFormat.Options optionsZero = new AcidOutputFormat.Options(null).bucket(7).statementId(0);
    assertEquals(codec.encode(optionsZero), codec.encode(optionsNeg));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetBucketCodecVersion1EncodeNegativeBucketId() {
    BucketCodec.getCodec(1).encode(new AcidOutputFormat.Options(null).bucket(-1).statementId(16));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetBucketCodecVersion1EncodeMaxBucketId() {
    BucketCodec.getCodec(1)
        .encode(new AcidOutputFormat.Options(null).bucket(BucketCodec.MAX_BUCKET_ID + 1).statementId(16));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetBucketCodecVersion1EncodeNegativeStatementId() {
    // A value of "-1" is acceptable
    BucketCodec.getCodec(1).encode(new AcidOutputFormat.Options(null).bucket(7).statementId(-2));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetBucketCodecVersion1EncodeMaxStatementId() {
    BucketCodec.getCodec(1)
        .encode(new AcidOutputFormat.Options(null).bucket(7).statementId(BucketCodec.MAX_STATEMENT_ID + 1));
  }

  @Test
  public void testBucketIdTakesExtra8BitsFromStmtId() {
    AcidOutputFormat.Options options = new AcidOutputFormat
            .Options(null)
            .bucket(5000)             // 1|001110001000
            .statementId(12)          //   000000001100
            .maxStmtId(100); //        1100100
    int encode = BucketCodec.getCodec(1).encode(options);
    assertEquals("10001100", Integer.toString(BucketCodec.V1.decodeStatementId(encode), 2));
    assertEquals("1110001000", Integer.toString(BucketCodec.V1.decodeWriterId(encode), 2));
  }

  @Test
  public void testBucketIdTakesExtra4BitsFromStmtId() {
    AcidOutputFormat.Options options = new AcidOutputFormat
            .Options(null)
            .bucket(65535)
            .statementId(150)         //   000010010110
            .maxStmtId(150); //       10010110
    int encode = BucketCodec.getCodec(1).encode(options);
    assertEquals("111110010110", Integer.toString(BucketCodec.V1.decodeStatementId(encode), 2));
    assertEquals("111111111111", Integer.toString(BucketCodec.V1.decodeWriterId(encode), 2));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoRoomToOverflowBucketId() {
    BucketCodec.getCodec(1).encode(new AcidOutputFormat
            .Options(null)
            .bucket(65536)
            .statementId(1)
            .maxStmtId(150));   //   000010010110
  }

  @Test
  public void testBucketIdRollOver() {
    for (int i = 0; i < 16384; i++) {
      int encode = BucketCodec.getCodec(1).encode(new AcidOutputFormat
              .Options(null)
              .bucket(i)
              .statementId(10)
              .maxStmtId(0b001000000000)); // 2 bit free
      assertEquals(i % 4096, BucketCodec.getCodec(1).decodeWriterId(encode));
      if (i < 4096)
        assertEquals(10, BucketCodec.getCodec(1).decodeStatementId(encode));
      else if (i < 8192)
        assertEquals(0b010000001010, BucketCodec.getCodec(1).decodeStatementId(encode));
      else if (i < 12288)
        assertEquals(0b100000001010, BucketCodec.getCodec(1).decodeStatementId(encode));
      else
        assertEquals(0b110000001010, BucketCodec.getCodec(1).decodeStatementId(encode));
    }
  }
}
