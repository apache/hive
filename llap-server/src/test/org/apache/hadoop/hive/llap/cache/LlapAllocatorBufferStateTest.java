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

package org.apache.hadoop.hive.llap.cache;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test Class for the buffer state
 */
public class LlapAllocatorBufferStateTest {
  // The clock bit is the most significant bit of the 64 bit word
  @Test
  public void testTestFixtureAsserts(){
    Assert.assertEquals(Long.MIN_VALUE, LlapAllocatorBuffer.State.CLOCK_BIT_MASK);
    Assert.assertEquals(64,
        LlapAllocatorBuffer.State.FLAGS_WIDTH
            + LlapAllocatorBuffer.State.REFCOUNT_WIDTH
            + LlapAllocatorBuffer.State.ARENA_WIDTH
            + LlapAllocatorBuffer.State.HEADER_WIDTH
            + LlapAllocatorBuffer.State.CLOCK_BIT_WIDTH);
  }
  @Test
  public void testSetClockBit() {
    long state = 4999L;
    Assert.assertFalse(LlapAllocatorBuffer.State.isClockBitSet(state));
    Assert.assertEquals(Long.MIN_VALUE | state, LlapAllocatorBuffer.State.setClockBit(state));
    state = -4999L;
    Assert.assertTrue(LlapAllocatorBuffer.State.isClockBitSet(state));
    Assert.assertEquals(Long.MIN_VALUE | state, LlapAllocatorBuffer.State.setClockBit(state));
  }

  @Test
  public void testUnsetClockBit() {
    long state = -4888L;
    Assert.assertTrue(LlapAllocatorBuffer.State.isClockBitSet(state));
    Assert.assertEquals(~(Long.MIN_VALUE) & state, LlapAllocatorBuffer.State.unSetClockBit(state));
    state = 4555L;
    Assert.assertFalse(LlapAllocatorBuffer.State.isClockBitSet(state));
    Assert.assertEquals(~(Long.MIN_VALUE) & state, LlapAllocatorBuffer.State.unSetClockBit(state));
  }
}
