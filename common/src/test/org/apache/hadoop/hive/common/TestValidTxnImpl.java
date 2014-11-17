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
package org.apache.hadoop.hive.common;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * Tests for {@link org.apache.hadoop.hive.common.ValidTxnListImpl}
 */
public class TestValidTxnImpl {

  @Test
  public void noExceptions() throws Exception {
    ValidTxnList txnList = new ValidTxnListImpl(new long[0], 1);
    String str = txnList.writeToString();
    Assert.assertEquals("1:", str);
    ValidTxnList newList = new ValidTxnListImpl();
    newList.readFromString(str);
    Assert.assertTrue(newList.isTxnCommitted(1));
    Assert.assertFalse(newList.isTxnCommitted(2));
  }

  @Test
  public void exceptions() throws Exception {
    ValidTxnList txnList = new ValidTxnListImpl(new long[]{2L,4L}, 5);
    String str = txnList.writeToString();
    Assert.assertEquals("5:2:4", str);
    ValidTxnList newList = new ValidTxnListImpl();
    newList.readFromString(str);
    Assert.assertTrue(newList.isTxnCommitted(1));
    Assert.assertFalse(newList.isTxnCommitted(2));
    Assert.assertTrue(newList.isTxnCommitted(3));
    Assert.assertFalse(newList.isTxnCommitted(4));
    Assert.assertTrue(newList.isTxnCommitted(5));
    Assert.assertFalse(newList.isTxnCommitted(6));
  }

  @Test
  public void longEnoughToCompress() throws Exception {
    long[] exceptions = new long[1000];
    for (int i = 0; i < 1000; i++) exceptions[i] = i + 100;
    ValidTxnList txnList = new ValidTxnListImpl(exceptions, 2000);
    String str = txnList.writeToString();
    ValidTxnList newList = new ValidTxnListImpl();
    newList.readFromString(str);
    for (int i = 0; i < 100; i++) Assert.assertTrue(newList.isTxnCommitted(i));
    for (int i = 100; i < 1100; i++) Assert.assertFalse(newList.isTxnCommitted(i));
    for (int i = 1100; i < 2001; i++) Assert.assertTrue(newList.isTxnCommitted(i));
    Assert.assertFalse(newList.isTxnCommitted(2001));
  }

  @Test
  public void readWriteConfig() throws Exception {
    long[] exceptions = new long[1000];
    for (int i = 0; i < 1000; i++) exceptions[i] = i + 100;
    ValidTxnList txnList = new ValidTxnListImpl(exceptions, 2000);
    String str = txnList.writeToString();
    Configuration conf = new Configuration();
    conf.set(ValidTxnList.VALID_TXNS_KEY, str);
    File tmpFile = File.createTempFile("TestValidTxnImpl", "readWriteConfig");
    DataOutputStream out = new DataOutputStream(new FileOutputStream(tmpFile));
    conf.write(out);
    out.close();
    DataInputStream in = new DataInputStream(new FileInputStream(tmpFile));
    Configuration newConf = new Configuration();
    newConf.readFields(in);
    Assert.assertEquals(str, newConf.get(ValidTxnList.VALID_TXNS_KEY));
  }
}
