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

package org.apache.hadoop.hive.common;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test suite for the {@link NumberUtils} class.
 */
public class TestNumberUtils {

  @Test
  public void testMinLong() {
    final long pair = NumberUtils.makeIntPair(Integer.MIN_VALUE, Integer.MIN_VALUE);
    Assert.assertEquals(Integer.MIN_VALUE, NumberUtils.getFirstInt(pair));
    Assert.assertEquals(Integer.MIN_VALUE, NumberUtils.getSecondInt(pair));
  }

  @Test
  public void testMaxLong() {
    final long pair = NumberUtils.makeIntPair(Integer.MAX_VALUE, Integer.MAX_VALUE);
    Assert.assertEquals(Integer.MAX_VALUE, NumberUtils.getFirstInt(pair));
    Assert.assertEquals(Integer.MAX_VALUE, NumberUtils.getSecondInt(pair));
  }

  @Test
  public void testZeroLong() {
    final long pair = NumberUtils.makeIntPair(0, 0);
    Assert.assertEquals(0, NumberUtils.getFirstInt(pair));
    Assert.assertEquals(0, NumberUtils.getSecondInt(pair));
  }

  @Test
  public void testNegativePositiveLong() {
    final long pair = NumberUtils.makeIntPair(1, -1);
    Assert.assertEquals(1, NumberUtils.getFirstInt(pair));
    Assert.assertEquals(-1, NumberUtils.getSecondInt(pair));
  }

}
