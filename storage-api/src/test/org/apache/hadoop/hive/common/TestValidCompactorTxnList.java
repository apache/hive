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

public class TestValidCompactorTxnList {

  @Test
  public void minTxnHigh() {
    BitSet bitSet = new BitSet(2);
    bitSet.set(0, 2);
    ValidTxnList txns = new ValidCompactorTxnList(new long[]{3, 4}, bitSet, 2);
    ValidTxnList.RangeResponse rsp = txns.isTxnRangeValid(7, 9);
    Assert.assertEquals(ValidTxnList.RangeResponse.NONE, rsp);
  }

  @Test
  public void maxTxnLow() {
    BitSet bitSet = new BitSet(2);
    bitSet.set(0, 2);
    ValidTxnList txns = new ValidCompactorTxnList(new long[]{13, 14}, bitSet, 12);
    ValidTxnList.RangeResponse rsp = txns.isTxnRangeValid(7, 9);
    Assert.assertEquals(ValidTxnList.RangeResponse.ALL, rsp);
  }

  @Test
  public void minTxnHighNoExceptions() {
    ValidTxnList txns = new ValidCompactorTxnList(new long[0], new BitSet(), 5);
    ValidTxnList.RangeResponse rsp = txns.isTxnRangeValid(7, 9);
    Assert.assertEquals(ValidTxnList.RangeResponse.NONE, rsp);
  }

  @Test
  public void maxTxnLowNoExceptions() {
    ValidTxnList txns = new ValidCompactorTxnList(new long[0], new BitSet(), 15);
    ValidTxnList.RangeResponse rsp = txns.isTxnRangeValid(7, 9);
    Assert.assertEquals(ValidTxnList.RangeResponse.ALL, rsp);
  }

  @Test
  public void exceptionsAllBelow() {
    BitSet bitSet = new BitSet(2);
    bitSet.set(0, 2);
    ValidTxnList txns = new ValidCompactorTxnList(new long[]{3, 6}, bitSet, 3);
    ValidTxnList.RangeResponse rsp = txns.isTxnRangeValid(7, 9);
    Assert.assertEquals(ValidTxnList.RangeResponse.NONE, rsp);
  }

  @Test
  public void exceptionsInMidst() {
    BitSet bitSet = new BitSet(1);
    bitSet.set(0, 1);
    ValidTxnList txns = new ValidCompactorTxnList(new long[]{8}, bitSet, 7);
    ValidTxnList.RangeResponse rsp = txns.isTxnRangeValid(7, 9);
    Assert.assertEquals(ValidTxnList.RangeResponse.NONE, rsp);
  }
  @Test
  public void exceptionsAbveHighWaterMark() {
    BitSet bitSet = new BitSet(4);
    bitSet.set(0, 4);
    ValidTxnList txns = new ValidCompactorTxnList(new long[]{8, 11, 17, 29}, bitSet, 15);
    Assert.assertArrayEquals("", new long[]{8, 11}, txns.getInvalidTransactions());
    ValidTxnList.RangeResponse rsp = txns.isTxnRangeValid(7, 9);
    Assert.assertEquals(ValidTxnList.RangeResponse.ALL, rsp);
    rsp = txns.isTxnRangeValid(12, 16);
    Assert.assertEquals(ValidTxnList.RangeResponse.NONE, rsp);
  }

  @Test
  public void writeToString() {
    BitSet bitSet = new BitSet(4);
    bitSet.set(0, 4);
    ValidTxnList txns = new ValidCompactorTxnList(new long[]{7, 9, 10, Long.MAX_VALUE}, bitSet, 8);
    Assert.assertEquals("8:" + Long.MAX_VALUE + "::7", txns.writeToString());
    txns = new ValidCompactorTxnList();
    Assert.assertEquals(Long.toString(Long.MAX_VALUE) + ":" + Long.MAX_VALUE + "::", txns.writeToString());
    txns = new ValidCompactorTxnList(new long[0], new BitSet(), 23);
    Assert.assertEquals("23:" + Long.MAX_VALUE + "::", txns.writeToString());
  }

  @Test
  public void readFromString() {
    ValidCompactorTxnList txns = new ValidCompactorTxnList("37:" + Long.MAX_VALUE + "::7,9,10");
    Assert.assertEquals(37L, txns.getHighWatermark());
    Assert.assertNull(txns.getMinOpenTxn());
    Assert.assertArrayEquals(new long[]{7L, 9L, 10L}, txns.getInvalidTransactions());
    txns = new ValidCompactorTxnList("21:" + Long.MAX_VALUE + ":");
    Assert.assertEquals(21L, txns.getHighWatermark());
    Assert.assertNull(txns.getMinOpenTxn());
    Assert.assertEquals(0, txns.getInvalidTransactions().length);
  }

  @Test
  public void testAbortedTxn() throws Exception {
    ValidCompactorTxnList txnList = new ValidCompactorTxnList("5:4::1,2,3");
    Assert.assertEquals(5L, txnList.getHighWatermark());
    Assert.assertEquals(4, txnList.getMinOpenTxn().longValue());
    Assert.assertArrayEquals(new long[]{1L, 2L, 3L}, txnList.getInvalidTransactions());
  }

  @Test
  public void testAbortedRange() throws Exception {
    ValidCompactorTxnList txnList = new ValidCompactorTxnList("11:4::5,6,7,8");
    ValidTxnList.RangeResponse rsp = txnList.isTxnRangeAborted(1L, 3L);
    Assert.assertEquals(ValidTxnList.RangeResponse.NONE, rsp);
    rsp = txnList.isTxnRangeAborted(9L, 10L);
    Assert.assertEquals(ValidTxnList.RangeResponse.NONE, rsp);
    rsp = txnList.isTxnRangeAborted(6L, 7L);
    Assert.assertEquals(ValidTxnList.RangeResponse.ALL, rsp);
    rsp = txnList.isTxnRangeAborted(4L, 6L);
    Assert.assertEquals(ValidTxnList.RangeResponse.SOME, rsp);
    rsp = txnList.isTxnRangeAborted(6L, 13L);
    Assert.assertEquals(ValidTxnList.RangeResponse.SOME, rsp);
  }
}
