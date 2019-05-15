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
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.BitSet;

/**
 * Tests for {@link ValidReaderWriteIdList}.
 */
public class TestValidReaderWriteIdList {
  private final String tableName = "t1";

  @Test
  public void noExceptions() throws Exception {
    ValidWriteIdList writeIdList = new ValidReaderWriteIdList(tableName, new long[0], new BitSet(), 1, Long.MAX_VALUE);
    String str = writeIdList.writeToString();
    TestCase.assertEquals(tableName + ":1:" + Long.MAX_VALUE + "::", str);
    ValidWriteIdList newList = new ValidReaderWriteIdList();
    newList.readFromString(str);
    TestCase.assertTrue(newList.isWriteIdValid(1));
    TestCase.assertFalse(newList.isWriteIdValid(2));
  }

  @Test
  public void exceptions() throws Exception {
    ValidWriteIdList writeIdList = new ValidReaderWriteIdList(tableName, new long[]{2L, 4L}, new BitSet(), 5, 4L);
    String str = writeIdList.writeToString();
    TestCase.assertEquals(tableName + ":5:4:2,4:", str);
    ValidWriteIdList newList = new ValidReaderWriteIdList();
    newList.readFromString(str);
    TestCase.assertTrue(newList.isWriteIdValid(1));
    TestCase.assertFalse(newList.isWriteIdValid(2));
    TestCase.assertTrue(newList.isWriteIdValid(3));
    TestCase.assertFalse(newList.isWriteIdValid(4));
    TestCase.assertTrue(newList.isWriteIdValid(5));
    TestCase.assertFalse(newList.isWriteIdValid(6));
  }

  @Test
  public void longEnoughToCompress() throws Exception {
    long[] exceptions = new long[1000];
    for (int i = 0; i < 1000; i++) {
      exceptions[i] = i + 100;
    }
    ValidWriteIdList writeIdList = new ValidReaderWriteIdList(tableName, exceptions, new BitSet(), 2000, 900);
    String str = writeIdList.writeToString();
    ValidWriteIdList newList = new ValidReaderWriteIdList();
    newList.readFromString(str);
    for (int i = 0; i < 100; i++) {
      TestCase.assertTrue(newList.isWriteIdValid(i));
    }
    for (int i = 100; i < 1100; i++) {
      TestCase.assertFalse(newList.isWriteIdValid(i));
    }
    for (int i = 1100; i < 2001; i++) {
      TestCase.assertTrue(newList.isWriteIdValid(i));
    }
    TestCase.assertFalse(newList.isWriteIdValid(2001));
  }

  @Test
  public void readWriteConfig() throws Exception {
    long[] exceptions = new long[1000];
    for (int i = 0; i < 1000; i++) {
      exceptions[i] = i + 100;
    }
    ValidWriteIdList writeIdList = new ValidReaderWriteIdList(tableName, exceptions, new BitSet(), 2000, 900);
    String str = writeIdList.writeToString();
    Configuration conf = new Configuration();
    conf.set(ValidWriteIdList.VALID_WRITEIDS_KEY, str);
    File tmpFile = File.createTempFile("TestValidTxnImpl", "readWriteConfig");
    DataOutputStream out = new DataOutputStream(new FileOutputStream(tmpFile));
    conf.write(out);
    out.close();
    DataInputStream in = new DataInputStream(new FileInputStream(tmpFile));
    Configuration newConf = new Configuration();
    newConf.readFields(in);
    TestCase.assertEquals(str, newConf.get(ValidWriteIdList.VALID_WRITEIDS_KEY));
  }

  @Test
  public void testAbortedTxn() throws Exception {
    long[] exceptions = {2L, 4L, 6L, 8L, 10L};
    BitSet bitSet = new BitSet(exceptions.length);
    bitSet.set(0);  // mark txn "2L" aborted
    bitSet.set(3);  // mark txn "8L" aborted
    ValidWriteIdList writeIdList = new ValidReaderWriteIdList(tableName, exceptions, bitSet, 11, 4L);
    String str = writeIdList.writeToString();
    TestCase.assertEquals(tableName + ":11:4:4,6,10:2,8", str);
    TestCase.assertTrue(writeIdList.isWriteIdAborted(2L));
    TestCase.assertFalse(writeIdList.isWriteIdAborted(4L));
    TestCase.assertFalse(writeIdList.isWriteIdAborted(6L));
    TestCase.assertTrue(writeIdList.isWriteIdAborted(8L));
    TestCase.assertFalse(writeIdList.isWriteIdAborted(10L));
  }
}
