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

package org.apache.hadoop.hive.serde2.io;

import junit.framework.Assert;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;

import org.apache.hadoop.hive.common.type.Decimal128;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hive.common.util.Decimal128FastBuffer;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for tsting the fast allocation-free conversion
 * between HiveDecimalWritable and Decimal128
 */
public class TestHiveDecimalWritable {

    private Decimal128FastBuffer scratch;

    @Before
    public void setUp() throws Exception {
      scratch = new Decimal128FastBuffer();
    }

    private void doTestFastStreamForHiveDecimal(String valueString) {
      BigDecimal value = new BigDecimal(valueString);
      Decimal128 dec = new Decimal128();
      dec.update(value);

      HiveDecimalWritable witness = new HiveDecimalWritable();
      witness.set(HiveDecimal.create(value));

      int bufferUsed = dec.fastSerializeForHiveDecimal(scratch);
      HiveDecimalWritable hdw = new HiveDecimalWritable();
      hdw.set(scratch.getBytes(bufferUsed), dec.getScale());

      HiveDecimal hd = hdw.getHiveDecimal();

      BigDecimal readValue = hd.bigDecimalValue();

      Assert.assertEquals(value, readValue);

      // Now test fastUpdate from the same serialized HiveDecimal
      Decimal128 decRead = new Decimal128().fastUpdateFromInternalStorage(
              witness.getInternalStorage(), (short) witness.getScale());

      Assert.assertEquals(dec, decRead);

      // Test fastUpdate from it's own (not fully compacted) serialized output
      Decimal128 decReadSelf = new Decimal128().fastUpdateFromInternalStorage(
              hdw.getInternalStorage(), (short) hdw.getScale());
      Assert.assertEquals(dec, decReadSelf);
    }

    @Test
    public void testFastStreamForHiveDecimal() {

      doTestFastStreamForHiveDecimal("0");
      doTestFastStreamForHiveDecimal("-0");
      doTestFastStreamForHiveDecimal("1");
      doTestFastStreamForHiveDecimal("-1");
      doTestFastStreamForHiveDecimal("2");
      doTestFastStreamForHiveDecimal("-2");
      doTestFastStreamForHiveDecimal("127");
      doTestFastStreamForHiveDecimal("-127");
      doTestFastStreamForHiveDecimal("128");
      doTestFastStreamForHiveDecimal("-128");
      doTestFastStreamForHiveDecimal("255");
      doTestFastStreamForHiveDecimal("-255");
      doTestFastStreamForHiveDecimal("256");
      doTestFastStreamForHiveDecimal("-256");
      doTestFastStreamForHiveDecimal("65535");
      doTestFastStreamForHiveDecimal("-65535");
      doTestFastStreamForHiveDecimal("65536");
      doTestFastStreamForHiveDecimal("-65536");

      doTestFastStreamForHiveDecimal("10");
      doTestFastStreamForHiveDecimal("1000");
      doTestFastStreamForHiveDecimal("1000000");
      doTestFastStreamForHiveDecimal("1000000000");
      doTestFastStreamForHiveDecimal("1000000000000");
      doTestFastStreamForHiveDecimal("1000000000000000");
      doTestFastStreamForHiveDecimal("1000000000000000000");
      doTestFastStreamForHiveDecimal("1000000000000000000000");
      doTestFastStreamForHiveDecimal("1000000000000000000000000");
      doTestFastStreamForHiveDecimal("1000000000000000000000000000");
      doTestFastStreamForHiveDecimal("1000000000000000000000000000000");

      doTestFastStreamForHiveDecimal("-10");
      doTestFastStreamForHiveDecimal("-1000");
      doTestFastStreamForHiveDecimal("-1000000");
      doTestFastStreamForHiveDecimal("-1000000000");
      doTestFastStreamForHiveDecimal("-1000000000000");
      doTestFastStreamForHiveDecimal("-1000000000000000000");
      doTestFastStreamForHiveDecimal("-1000000000000000000000");
      doTestFastStreamForHiveDecimal("-1000000000000000000000000");
      doTestFastStreamForHiveDecimal("-1000000000000000000000000000");
      doTestFastStreamForHiveDecimal("-1000000000000000000000000000000");


      doTestFastStreamForHiveDecimal("0.01");
      doTestFastStreamForHiveDecimal("-0.01");
      doTestFastStreamForHiveDecimal("0.02");
      doTestFastStreamForHiveDecimal("-0.02");
      doTestFastStreamForHiveDecimal("0.0127");
      doTestFastStreamForHiveDecimal("-0.0127");
      doTestFastStreamForHiveDecimal("0.0128");
      doTestFastStreamForHiveDecimal("-0.0128");
      doTestFastStreamForHiveDecimal("0.0255");
      doTestFastStreamForHiveDecimal("-0.0255");
      doTestFastStreamForHiveDecimal("0.0256");
      doTestFastStreamForHiveDecimal("-0.0256");
      doTestFastStreamForHiveDecimal("0.065535");
      doTestFastStreamForHiveDecimal("-0.065535");
      doTestFastStreamForHiveDecimal("0.065536");
      doTestFastStreamForHiveDecimal("-0.065536");

      doTestFastStreamForHiveDecimal("0.101");
      doTestFastStreamForHiveDecimal("0.10001");
      doTestFastStreamForHiveDecimal("0.10000001");
      doTestFastStreamForHiveDecimal("0.10000000001");
      doTestFastStreamForHiveDecimal("0.10000000000001");
      doTestFastStreamForHiveDecimal("0.10000000000000001");
      doTestFastStreamForHiveDecimal("0.10000000000000000001");
      doTestFastStreamForHiveDecimal("0.10000000000000000000001");
      doTestFastStreamForHiveDecimal("0.10000000000000000000000001");
      doTestFastStreamForHiveDecimal("0.10000000000000000000000000001");
      doTestFastStreamForHiveDecimal("0.10000000000000000000000000000001");

      doTestFastStreamForHiveDecimal("-0.101");
      doTestFastStreamForHiveDecimal("-0.10001");
      doTestFastStreamForHiveDecimal("-0.10000001");
      doTestFastStreamForHiveDecimal("-0.10000000001");
      doTestFastStreamForHiveDecimal("-0.10000000000001");
      doTestFastStreamForHiveDecimal("-0.10000000000000000001");
      doTestFastStreamForHiveDecimal("-0.10000000000000000000001");
      doTestFastStreamForHiveDecimal("-0.10000000000000000000000001");
      doTestFastStreamForHiveDecimal("-0.10000000000000000000000000001");
      doTestFastStreamForHiveDecimal("-0.10000000000000000000000000000001");

      doTestFastStreamForHiveDecimal(Integer.toString(Integer.MAX_VALUE));
      doTestFastStreamForHiveDecimal(Integer.toString(Integer.MIN_VALUE));
      doTestFastStreamForHiveDecimal(Long.toString(Long.MAX_VALUE));
      doTestFastStreamForHiveDecimal(Long.toString(Long.MIN_VALUE));
      doTestFastStreamForHiveDecimal(Decimal128.MAX_VALUE.toFormalString());
      doTestFastStreamForHiveDecimal(Decimal128.MIN_VALUE.toFormalString());

            // Test known serialization tricky values
      int[] values = new int[] {
              0x80,
              0x8000,
              0x800000,
              0x80000000,
              0x81,
                    0x8001,
                    0x800001,
                    0x80000001,
              0x7f,
              0x7fff,
              0x7fffff,
              0x7fffffff,
              0xff,
              0xffff,
              0xffffff,
              0xffffffff};


      for(int value: values) {
          for (int i = 0; i < 4; ++i) {
              int[] pos = new int[] {1, 0, 0, 0, 0};
              int[] neg = new int[] {0xff, 0, 0, 0, 0};

              pos[i+1] = neg[i+1] = value;

              doTestDecimalWithBoundsCheck(new Decimal128().update32(pos, 0));
              doTestDecimalWithBoundsCheck(new Decimal128().update32(neg, 0));
              doTestDecimalWithBoundsCheck(new Decimal128().update64(pos, 0));
              doTestDecimalWithBoundsCheck(new Decimal128().update64(neg, 0));
              doTestDecimalWithBoundsCheck(new Decimal128().update96(pos, 0));
              doTestDecimalWithBoundsCheck(new Decimal128().update96(neg, 0));
              doTestDecimalWithBoundsCheck(new Decimal128().update128(pos, 0));
              doTestDecimalWithBoundsCheck(new Decimal128().update128(neg, 0));
          }
      }
    }

    void doTestDecimalWithBoundsCheck(Decimal128 value) {
       if ((value.compareTo(Decimal128.MAX_VALUE)) > 0 ||
           (value.compareTo(Decimal128.MIN_VALUE)) < 0) {
             // Ignore this one, out of bounds and HiveDecimal will NPE
             return;
       }
       doTestFastStreamForHiveDecimal(value.toFormalString());
    }

    @Test
    public void testHive6594() {
      String[] vs = new String[] {
          "-4033.445769230769",
          "6984454.211097692"};

      Decimal128 d = new Decimal128(0L, (short) 14);
      for (String s:vs) {
        Decimal128 p = new Decimal128(s, (short) 14);
        d.addDestructive(p, (short) (short) 14);
      }

      int bufferUsed = d.fastSerializeForHiveDecimal(scratch);
      HiveDecimalWritable hdw = new HiveDecimalWritable();
      hdw.set(scratch.getBytes(bufferUsed), d.getScale());

      HiveDecimal hd = hdw.getHiveDecimal();

      BigDecimal readValue = hd.bigDecimalValue();

      Assert.assertEquals(d.toBigDecimal().stripTrailingZeros(),
          readValue.stripTrailingZeros());
    }
}

