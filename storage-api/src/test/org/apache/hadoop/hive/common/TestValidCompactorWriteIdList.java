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

import java.util.BitSet;

/**
 * Tests for {@link ValidCompactorWriteIdList}.
 */
public class TestValidCompactorWriteIdList {
  private final String tableName = "t1";

  @Test
  public void minTxnHigh() {
    BitSet bitSet = new BitSet(2);
    bitSet.set(0, 2);
    ValidWriteIdList writeIds = new ValidCompactorWriteIdList(tableName, new long[]{3, 4}, bitSet, 2);
    ValidWriteIdList.RangeResponse rsp = writeIds.isWriteIdRangeValid(7, 9);
    Assert.assertEquals(ValidWriteIdList.RangeResponse.NONE, rsp);
  }

  @Test
  public void maxTxnLow() {
    BitSet bitSet = new BitSet(2);
    bitSet.set(0, 2);
    ValidWriteIdList writeIds = new ValidCompactorWriteIdList(tableName, new long[]{13, 14}, bitSet, 12);
    ValidWriteIdList.RangeResponse rsp = writeIds.isWriteIdRangeValid(7, 9);
    Assert.assertEquals(ValidWriteIdList.RangeResponse.ALL, rsp);
  }

  @Test
  public void minTxnHighNoExceptions() {
    ValidWriteIdList writeIds = new ValidCompactorWriteIdList(tableName, new long[0], new BitSet(), 5);
    ValidWriteIdList.RangeResponse rsp = writeIds.isWriteIdRangeValid(7, 9);
    Assert.assertEquals(ValidWriteIdList.RangeResponse.NONE, rsp);
  }

  @Test
  public void maxTxnLowNoExceptions() {
    ValidWriteIdList writeIds = new ValidCompactorWriteIdList(tableName, new long[0], new BitSet(), 15);
    ValidWriteIdList.RangeResponse rsp = writeIds.isWriteIdRangeValid(7, 9);
    Assert.assertEquals(ValidWriteIdList.RangeResponse.ALL, rsp);
  }

  @Test
  public void exceptionsAllBelow() {
    BitSet bitSet = new BitSet(2);
    bitSet.set(0, 2);
    ValidWriteIdList writeIds = new ValidCompactorWriteIdList(tableName, new long[]{3, 6}, bitSet, 3);
    ValidWriteIdList.RangeResponse rsp = writeIds.isWriteIdRangeValid(7, 9);
    Assert.assertEquals(ValidWriteIdList.RangeResponse.NONE, rsp);
  }

  @Test
  public void exceptionsInMidst() {
    BitSet bitSet = new BitSet(1);
    bitSet.set(0, 1);
    ValidWriteIdList writeIds = new ValidCompactorWriteIdList(tableName, new long[]{8}, bitSet, 7);
    ValidWriteIdList.RangeResponse rsp = writeIds.isWriteIdRangeValid(7, 9);
    Assert.assertEquals(ValidWriteIdList.RangeResponse.NONE, rsp);
  }
  @Test
  public void exceptionsAboveHighWaterMark() {
    BitSet bitSet = new BitSet(4);
    bitSet.set(0, 4);
    ValidWriteIdList writeIds = new ValidCompactorWriteIdList(tableName, new long[]{8, 11, 17, 29}, bitSet, 15);
    Assert.assertArrayEquals("", new long[]{8, 11}, writeIds.getInvalidWriteIds());
    ValidWriteIdList.RangeResponse rsp = writeIds.isWriteIdRangeValid(7, 9);
    Assert.assertEquals(ValidWriteIdList.RangeResponse.ALL, rsp);
    rsp = writeIds.isWriteIdRangeValid(12, 16);
    Assert.assertEquals(ValidWriteIdList.RangeResponse.NONE, rsp);
  }

  @Test
  public void writeToString() {
    BitSet bitSet = new BitSet(4);
    bitSet.set(0, 4);
    ValidWriteIdList writeIds
            = new ValidCompactorWriteIdList(tableName, new long[]{7, 9, 10, Long.MAX_VALUE}, bitSet, 8);
    Assert.assertEquals(tableName + ":8:" + Long.MAX_VALUE + "::7", writeIds.writeToString());
    writeIds = new ValidCompactorWriteIdList();
    Assert.assertEquals("null:" + Long.toString(Long.MAX_VALUE) + ":" + Long.MAX_VALUE + "::",
            writeIds.writeToString());
    writeIds = new ValidCompactorWriteIdList(tableName, new long[0], new BitSet(), 23);
    Assert.assertEquals(tableName + ":23:" + Long.MAX_VALUE + "::", writeIds.writeToString());
  }

  @Test
  public void readFromString() {
    ValidCompactorWriteIdList writeIds
            = new ValidCompactorWriteIdList(tableName + ":37:" + Long.MAX_VALUE + "::7,9,10");
    Assert.assertEquals(tableName, writeIds.getTableName());
    Assert.assertEquals(37L, writeIds.getHighWatermark());
    Assert.assertNull(writeIds.getMinOpenWriteId());
    Assert.assertArrayEquals(new long[]{7L, 9L, 10L}, writeIds.getInvalidWriteIds());
    writeIds = new ValidCompactorWriteIdList(tableName + ":21:" + Long.MAX_VALUE + ":");
    Assert.assertEquals(21L, writeIds.getHighWatermark());
    Assert.assertNull(writeIds.getMinOpenWriteId());
    Assert.assertEquals(0, writeIds.getInvalidWriteIds().length);
  }

  @Test
  public void testAbortedTxn() throws Exception {
    ValidCompactorWriteIdList writeIdList = new ValidCompactorWriteIdList(tableName + ":5:4::1,2,3");
    Assert.assertEquals(5L, writeIdList.getHighWatermark());
    Assert.assertEquals(4, writeIdList.getMinOpenWriteId().longValue());
    Assert.assertArrayEquals(new long[]{1L, 2L, 3L}, writeIdList.getInvalidWriteIds());
  }

  @Test
  public void testAbortedRange() throws Exception {
    ValidCompactorWriteIdList writeIdList = new ValidCompactorWriteIdList(tableName + ":11:4::5,6,7,8");
    ValidWriteIdList.RangeResponse rsp = writeIdList.isWriteIdRangeAborted(1L, 3L);
    Assert.assertEquals(ValidWriteIdList.RangeResponse.NONE, rsp);
    rsp = writeIdList.isWriteIdRangeAborted(9L, 10L);
    Assert.assertEquals(ValidWriteIdList.RangeResponse.NONE, rsp);
    rsp = writeIdList.isWriteIdRangeAborted(6L, 7L);
    Assert.assertEquals(ValidWriteIdList.RangeResponse.ALL, rsp);
    rsp = writeIdList.isWriteIdRangeAborted(4L, 6L);
    Assert.assertEquals(ValidWriteIdList.RangeResponse.SOME, rsp);
    rsp = writeIdList.isWriteIdRangeAborted(6L, 13L);
    Assert.assertEquals(ValidWriteIdList.RangeResponse.SOME, rsp);
  }
}
