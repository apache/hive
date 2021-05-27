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

import junit.framework.TestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;

/**
 * Tests for {@link MutableValidReaderWriteIdList}.
 */
public class TestMutableValidReaderWriteIdList {
  private final String tableName = "t1";

  @Test
  public void noExceptions() {
    ValidReaderWriteIdList writeIdList = new ValidReaderWriteIdList(
        tableName, new long[0], new BitSet(), 1, Long.MAX_VALUE);
    MutableValidReaderWriteIdList mutableWriteIdList = new MutableValidReaderWriteIdList(writeIdList);
    String str = mutableWriteIdList.writeToString();
    TestCase.assertEquals(tableName + ":1:" + Long.MAX_VALUE + "::", str);
    TestCase.assertTrue(mutableWriteIdList.isWriteIdValid(1));
    TestCase.assertFalse(mutableWriteIdList.isWriteIdValid(2));
  }

  @Test
  public void exceptions() {
    ValidReaderWriteIdList writeIdList = new ValidReaderWriteIdList(tableName, new long[]{2L, 4L}, new BitSet(), 5, 4L);
    MutableValidReaderWriteIdList mutableWriteIdList = new MutableValidReaderWriteIdList(writeIdList);
    String str = mutableWriteIdList.writeToString();
    TestCase.assertEquals(tableName + ":5:4:2,4:", str);
    TestCase.assertTrue(mutableWriteIdList.isWriteIdValid(1));
    TestCase.assertFalse(mutableWriteIdList.isWriteIdValid(2));
    TestCase.assertTrue(mutableWriteIdList.isWriteIdValid(3));
    TestCase.assertFalse(mutableWriteIdList.isWriteIdValid(4));
    TestCase.assertTrue(mutableWriteIdList.isWriteIdValid(5));
    TestCase.assertFalse(mutableWriteIdList.isWriteIdValid(6));
  }

  @Test
  public void longEnoughToCompress() {
    long[] exceptions = new long[1000];
    for (int i = 0; i < 1000; i++) {
      exceptions[i] = i + 100;
    }
    ValidReaderWriteIdList writeIdList = new ValidReaderWriteIdList(tableName, exceptions, new BitSet(), 2000, 900);
    MutableValidReaderWriteIdList mutableWriteIdList = new MutableValidReaderWriteIdList(writeIdList);
    for (int i = 0; i < 100; i++) {
      TestCase.assertTrue(mutableWriteIdList.isWriteIdValid(i));
    }
    for (int i = 100; i < 1100; i++) {
      TestCase.assertFalse(mutableWriteIdList.isWriteIdValid(i));
    }
    for (int i = 1100; i < 2001; i++) {
      TestCase.assertTrue(mutableWriteIdList.isWriteIdValid(i));
    }
    TestCase.assertFalse(mutableWriteIdList.isWriteIdValid(2001));
  }

  @Test
  public void testAbortedTxn() {
    long[] exceptions = {2L, 4L, 6L, 8L, 10L};
    BitSet bitSet = new BitSet(exceptions.length);
    bitSet.set(0); // mark writeId "2L" aborted
    bitSet.set(3); // mark writeId "8L" aborted
    ValidReaderWriteIdList writeIdList = new ValidReaderWriteIdList(tableName, exceptions, bitSet, 11, 4);
    MutableValidReaderWriteIdList mutableWriteIdList = new MutableValidReaderWriteIdList(writeIdList);

    TestCase.assertTrue(mutableWriteIdList.isWriteIdAborted(2));
    TestCase.assertFalse(mutableWriteIdList.isWriteIdAborted(4));
    TestCase.assertFalse(mutableWriteIdList.isWriteIdAborted(6));
    TestCase.assertTrue(mutableWriteIdList.isWriteIdAborted(8));
    TestCase.assertFalse(mutableWriteIdList.isWriteIdAborted(10));
  }

  @Test
  public void testAddOpenWriteId() {
    long[] exceptions = {2, 4, 6, 8, 10};
    BitSet bitSet = new BitSet(exceptions.length);
    bitSet.set(0); // mark writeId "2L" aborted
    bitSet.set(3); // mark writeId "8L" aborted
    ValidReaderWriteIdList writeIdList = new ValidReaderWriteIdList(tableName, exceptions, bitSet, 11, 4);
    MutableValidReaderWriteIdList mutableWriteIdList = new MutableValidReaderWriteIdList(writeIdList);

    mutableWriteIdList.addOpenWriteId(13);
    String str = mutableWriteIdList.writeToString();
    TestCase.assertEquals(tableName + ":13:4:4,6,10,12,13:2,8", str);
    TestCase.assertTrue(mutableWriteIdList.isWriteIdValid(11));
    TestCase.assertFalse(mutableWriteIdList.isWriteIdValid(12));
    TestCase.assertFalse(mutableWriteIdList.isWriteIdValid(13));
  }

  @Test
  public void testAddAbortedWriteIds() {
    long[] exceptions = {2L, 4L, 6L, 8L, 10L};
    BitSet bitSet = new BitSet(exceptions.length);
    bitSet.set(0); // mark writeId "2L" aborted
    bitSet.set(3); // mark writeId "8L" aborted
    ValidReaderWriteIdList writeIdList = new ValidReaderWriteIdList(tableName, exceptions, bitSet, 11, 4);
    MutableValidReaderWriteIdList mutableWriteIdList = new MutableValidReaderWriteIdList(writeIdList);

    mutableWriteIdList.addOpenWriteId(13);
    mutableWriteIdList.addAbortedWriteIds(Collections.singletonList(4L));
    mutableWriteIdList.addAbortedWriteIds(Collections.singletonList(12L));
    String str = mutableWriteIdList.writeToString();
    TestCase.assertEquals(tableName + ":13:6:6,10,13:2,4,8,12", str);
    TestCase.assertFalse(mutableWriteIdList.isWriteIdValid(4));
    TestCase.assertTrue(mutableWriteIdList.isWriteIdAborted(4));
    TestCase.assertFalse(mutableWriteIdList.isWriteIdValid(12));
    TestCase.assertTrue(mutableWriteIdList.isWriteIdAborted(12));
    TestCase.assertFalse(mutableWriteIdList.isWriteIdValid(13));
    TestCase.assertFalse(mutableWriteIdList.isWriteIdAborted(13));
  }

  @Test
  public void testAddCommittedWriteIds() {
    long[] exceptions = {2L, 4L, 6L, 8L, 10L};
    BitSet bitSet = new BitSet(exceptions.length);
    bitSet.set(0); // mark writeId "2L" aborted
    bitSet.set(3); // mark writeId "8L" aborted
    ValidReaderWriteIdList writeIdList = new ValidReaderWriteIdList(
        tableName, exceptions, bitSet, 11, 4);
    MutableValidReaderWriteIdList mutableWriteIdList = new MutableValidReaderWriteIdList(writeIdList);

    mutableWriteIdList.addCommittedWriteIds(Arrays.asList(4L, 10L));
    String str = mutableWriteIdList.writeToString();
    TestCase.assertEquals(tableName + ":11:6:6:2,8", str);
    TestCase.assertTrue(mutableWriteIdList.isWriteIdAborted(2));
    TestCase.assertTrue(mutableWriteIdList.isWriteIdValid(4));
    TestCase.assertFalse(mutableWriteIdList.isWriteIdValid(6));
    TestCase.assertTrue(mutableWriteIdList.isWriteIdValid(10));
    TestCase.assertTrue(mutableWriteIdList.isWriteIdValid(11));
  }

  @Test(expected = IllegalStateException.class)
  public void testAddAbortedToCommitted() {
    long[] exceptions = {2L, 4L, 6L, 8L, 10L};
    BitSet bitSet = new BitSet(exceptions.length);
    bitSet.set(0); // mark writeId "2L" aborted
    bitSet.set(3); // mark writeId "8L" aborted
    ValidReaderWriteIdList writeIdList = new ValidReaderWriteIdList(tableName, exceptions, bitSet, 11, 4);
    MutableValidReaderWriteIdList mutableWriteIdList = new MutableValidReaderWriteIdList(writeIdList);
    mutableWriteIdList.addCommittedWriteIds(Collections.singletonList(2L));
  }
}
