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
package org.apache.hadoop.hive.ql.io.orc;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Random;

import org.junit.Test;

import com.google.common.primitives.Longs;

public class TestBitPack {

  private static final int SIZE = 100;
  private static Random rand = new Random(100);

  private long[] deltaEncode(long[] inp) {
    long[] output = new long[inp.length];
    for(int i = 0; i < inp.length; i++) {
      output[i] = SerializationUtils.zigzagEncode(inp[i]);
    }
    return output;
  }

  private long nextLong(Random rng, long n) {
    long bits, val;
    do {
      bits = (rng.nextLong() << 1) >>> 1;
      val = bits % n;
    } while (bits - val + (n - 1) < 0L);
    return val;
  }

  private void runTest(int numBits) throws IOException {
    long[] inp = new long[SIZE];
    for(int i = 0; i < SIZE; i++) {
      long val = 0;
      if (numBits <= 32) {
        if (numBits == 1) {
          val = -1 * rand.nextInt(2);
        } else {
          val = rand.nextInt((int) Math.pow(2, numBits - 1));
        }
      } else {
        val = nextLong(rand, (long) Math.pow(2, numBits - 2));
      }
      if (val % 2 == 0) {
        val = -val;
      }
      inp[i] = val;
    }
    long[] deltaEncoded = deltaEncode(inp);
    long minInput = Collections.min(Longs.asList(deltaEncoded));
    long maxInput = Collections.max(Longs.asList(deltaEncoded));
    long rangeInput = maxInput - minInput;
    int fixedWidth = SerializationUtils.findClosestNumBits(rangeInput);
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    OutStream output = new OutStream("test", SIZE, null, collect);
    SerializationUtils.writeInts(deltaEncoded, 0, deltaEncoded.length,
        fixedWidth, output);
    output.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    long[] buff = new long[SIZE];
    SerializationUtils.readInts(buff, 0, SIZE, fixedWidth,
                                InStream.create("test",
                                                new ByteBuffer[]{inBuf},
                                                new long[]{0},
                                                inBuf.remaining(),
                                                null, SIZE));
    for(int i = 0; i < SIZE; i++) {
      buff[i] = SerializationUtils.zigzagDecode(buff[i]);
    }
    assertEquals(numBits, fixedWidth);
    assertArrayEquals(inp, buff);
  }

  @Test
  public void test01BitPacking1Bit() throws IOException {
    runTest(1);
  }

  @Test
  public void test02BitPacking2Bit() throws IOException {
    runTest(2);
  }

  @Test
  public void test03BitPacking3Bit() throws IOException {
    runTest(3);
  }

  @Test
  public void test04BitPacking4Bit() throws IOException {
    runTest(4);
  }

  @Test
  public void test05BitPacking5Bit() throws IOException {
    runTest(5);
  }

  @Test
  public void test06BitPacking6Bit() throws IOException {
    runTest(6);
  }

  @Test
  public void test07BitPacking7Bit() throws IOException {
    runTest(7);
  }

  @Test
  public void test08BitPacking8Bit() throws IOException {
    runTest(8);
  }

  @Test
  public void test09BitPacking9Bit() throws IOException {
    runTest(9);
  }

  @Test
  public void test10BitPacking10Bit() throws IOException {
    runTest(10);
  }

  @Test
  public void test11BitPacking11Bit() throws IOException {
    runTest(11);
  }

  @Test
  public void test12BitPacking12Bit() throws IOException {
    runTest(12);
  }

  @Test
  public void test13BitPacking13Bit() throws IOException {
    runTest(13);
  }

  @Test
  public void test14BitPacking14Bit() throws IOException {
    runTest(14);
  }

  @Test
  public void test15BitPacking15Bit() throws IOException {
    runTest(15);
  }

  @Test
  public void test16BitPacking16Bit() throws IOException {
    runTest(16);
  }

  @Test
  public void test17BitPacking17Bit() throws IOException {
    runTest(17);
  }

  @Test
  public void test18BitPacking18Bit() throws IOException {
    runTest(18);
  }

  @Test
  public void test19BitPacking19Bit() throws IOException {
    runTest(19);
  }

  @Test
  public void test20BitPacking20Bit() throws IOException {
    runTest(20);
  }

  @Test
  public void test21BitPacking21Bit() throws IOException {
    runTest(21);
  }

  @Test
  public void test22BitPacking22Bit() throws IOException {
    runTest(22);
  }

  @Test
  public void test23BitPacking23Bit() throws IOException {
    runTest(23);
  }

  @Test
  public void test24BitPacking24Bit() throws IOException {
    runTest(24);
  }

  @Test
  public void test26BitPacking26Bit() throws IOException {
    runTest(26);
  }

  @Test
  public void test28BitPacking28Bit() throws IOException {
    runTest(28);
  }

  @Test
  public void test30BitPacking30Bit() throws IOException {
    runTest(30);
  }

  @Test
  public void test32BitPacking32Bit() throws IOException {
    runTest(32);
  }

  @Test
  public void test40BitPacking40Bit() throws IOException {
    runTest(40);
  }

  @Test
  public void test48BitPacking48Bit() throws IOException {
    runTest(48);
  }

  @Test
  public void test56BitPacking56Bit() throws IOException {
    runTest(56);
  }

  @Test
  public void test64BitPacking64Bit() throws IOException {
    runTest(64);
  }
}
