/**
 * Copyright (c) Microsoft Corporation
 *
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
package org.apache.hadoop.hive.common.type;

import static org.junit.Assert.*;
import org.junit.Test;

/**
 * This code was originally written for Microsoft PolyBase.
 */
public class TestSqlMathUtil {
  @Test
  public void testDivision() {
    {
      int[] dividend = new int[] { 1 + 33, 2 + 21, 3, 4 + 10, 20, 30, 40, 0 };
      int[] divisor = new int[] { 1, 2, 3, 4 };
      int[] quotient = new int[5];
      int[] remainder = SqlMathUtil.divideMultiPrecision(dividend, divisor,
          quotient);
      assertArrayEquals(new int[] { 1, 0, 0, 10, 0 }, quotient);
      assertArrayEquals(new int[] { 33, 21, 0, 0, 0, 0, 0, 0, 0 }, remainder);
    }

    {
      int[] dividend = new int[] { 0xF7000000, 0, 0x39000000, 0 };
      int[] divisor = new int[] { 0xF700, 0, 0x3900, 0 };
      int[] quotient = new int[5];
      int[] remainder = SqlMathUtil.divideMultiPrecision(dividend, divisor,
          quotient);
      assertArrayEquals(new int[] { 0x10000, 0, 0, 0, 0 }, quotient);
      assertArrayEquals(new int[] { 0, 0, 0, 0, 0 }, remainder);
    }

    {
      // Zero dividend
      int[] dividend = new int[] { 0, 0, 0, 0 };
      int[] divisor = new int[] { 0xF700, 0, 0x3900, 0 };
      int[] quotient = new int[5];
      int[] remainder = SqlMathUtil.divideMultiPrecision(dividend, divisor,
          quotient);
      assertArrayEquals(new int[] { 0, 0, 0, 0, 0 }, quotient);
      assertArrayEquals(new int[] { 0, 0, 0, 0, 0 }, remainder);
    }
  }
}
