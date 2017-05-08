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
package org.apache.hadoop.hive.metastore.txn;

import org.apache.hadoop.hive.common.ValidCompactorTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.junit.Assert;
import org.junit.Test;

public class TestValidCompactorTxnList {

  @Test
  public void minTxnHigh() {
    ValidTxnList txns = new ValidCompactorTxnList(new long[]{3, 4}, 2);
    ValidTxnList.RangeResponse rsp = txns.isTxnRangeValid(7, 9);
    Assert.assertEquals(ValidTxnList.RangeResponse.NONE, rsp);
  }

  @Test
  public void maxTxnLow() {
    ValidTxnList txns = new ValidCompactorTxnList(new long[]{13, 14}, 12);
    ValidTxnList.RangeResponse rsp = txns.isTxnRangeValid(7, 9);
    Assert.assertEquals(ValidTxnList.RangeResponse.ALL, rsp);
  }

  @Test
  public void minTxnHighNoExceptions() {
    ValidTxnList txns = new ValidCompactorTxnList(new long[0], 5);
    ValidTxnList.RangeResponse rsp = txns.isTxnRangeValid(7, 9);
    Assert.assertEquals(ValidTxnList.RangeResponse.NONE, rsp);
  }

  @Test
  public void maxTxnLowNoExceptions() {
    ValidTxnList txns = new ValidCompactorTxnList(new long[0], 15);
    ValidTxnList.RangeResponse rsp = txns.isTxnRangeValid(7, 9);
    Assert.assertEquals(ValidTxnList.RangeResponse.ALL, rsp);
  }

  @Test
  public void exceptionsAllBelow() {
    ValidTxnList txns = new ValidCompactorTxnList(new long[]{3, 6}, 3);
    ValidTxnList.RangeResponse rsp = txns.isTxnRangeValid(7, 9);
    Assert.assertEquals(ValidTxnList.RangeResponse.NONE, rsp);
  }

  @Test
  public void exceptionsInMidst() {
    ValidTxnList txns = new ValidCompactorTxnList(new long[]{8}, 7);
    ValidTxnList.RangeResponse rsp = txns.isTxnRangeValid(7, 9);
    Assert.assertEquals(ValidTxnList.RangeResponse.NONE, rsp);
  }
  @Test
  public void exceptionsAbveHighWaterMark() {
    ValidTxnList txns = new ValidCompactorTxnList(new long[]{8, 11, 17, 29}, 15);
    Assert.assertArrayEquals("", new long[]{8, 11}, txns.getInvalidTransactions());
    ValidTxnList.RangeResponse rsp = txns.isTxnRangeValid(7, 9);
    Assert.assertEquals(ValidTxnList.RangeResponse.ALL, rsp);
    rsp = txns.isTxnRangeValid(12, 16);
    Assert.assertEquals(ValidTxnList.RangeResponse.NONE, rsp);
  }

  @Test
  public void writeToString() {
    ValidTxnList txns = new ValidCompactorTxnList(new long[]{9, 7, 10, Long.MAX_VALUE}, 8);
    Assert.assertEquals("8:" + Long.MAX_VALUE + ":7", txns.writeToString());
    txns = new ValidCompactorTxnList();
    Assert.assertEquals(Long.toString(Long.MAX_VALUE) + ":" + Long.MAX_VALUE + ":", txns.writeToString());
    txns = new ValidCompactorTxnList(new long[0], 23);
    Assert.assertEquals("23:" + Long.MAX_VALUE + ":", txns.writeToString());
  }

  @Test
  public void readFromString() {
    ValidCompactorTxnList txns = new ValidCompactorTxnList("37:" + Long.MAX_VALUE + ":7:9:10");
    Assert.assertEquals(37L, txns.getHighWatermark());
    Assert.assertEquals(Long.MAX_VALUE, txns.getMinOpenTxn());
    Assert.assertArrayEquals(new long[]{7L, 9L, 10L}, txns.getInvalidTransactions());
    txns = new ValidCompactorTxnList("21:" + Long.MAX_VALUE + ":");
    Assert.assertEquals(21L, txns.getHighWatermark());
    Assert.assertEquals(Long.MAX_VALUE, txns.getMinOpenTxn());
    Assert.assertEquals(0, txns.getInvalidTransactions().length);
  }
}
