/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.lazy.fast;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.*;

public class TestStringToDouble {
  int iter = 10000;

  @Test
  public void testFullRandom() throws Exception {
    Random random = new Random();
    for (int i = 0; i < iter; i++) {
      double d = Double.longBitsToDouble(random.nextLong());
      String s = Double.toString(d);
      assertEquals(s, d, Double.parseDouble(s), Double.MIN_VALUE);
      assertEquals(s, d, StringToDouble.strtod(s), Double.MIN_VALUE);
    }
  }

  @Test
  public void testRandomBetween0And1() throws Exception {
    Random random = new Random();
    for (int i = 0; i < iter; i++) {
      for (int j = 1; j < 18; j++) {
        StringBuilder builder = new StringBuilder("0.1");
        for (int k = 0; k < j; k++) {
          builder.append(random.nextInt(10) + '0');
        }
        String s = builder.toString();
        double d = Double.parseDouble(s);
        assertEquals(s, d, Double.parseDouble(s), Double.MIN_VALUE);
        assertEquals(s, d, StringToDouble.strtod(s), Double.MIN_VALUE);
      }
    }
  }

  @Test
  public void testRandomInteger() throws Exception {
    Random random = new Random();
    for (int i = 0; i < iter; i++) {
      for (int j = 1; j < 18; j++) {
        StringBuilder builder = new StringBuilder();
        for (int k = 0; k < j; k++) {
          builder.append(random.nextInt(10) + '0');
        }
        String s = builder.toString();
        double d = Double.parseDouble(s);
        assertEquals(s, d, Double.parseDouble(s), Double.MIN_VALUE);
        assertEquals(s, d, StringToDouble.strtod(s), Double.MIN_VALUE);
      }
    }
  }
}