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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.hbase.snapshot;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hcatalog.hbase.snapshot.transaction.thrift.StoreFamilyRevision;
import org.apache.hcatalog.hbase.snapshot.transaction.thrift.StoreFamilyRevisionList;
import org.junit.Test;

public class TestThriftSerialization {

  @Test
  public void testLightWeightTransaction() {
    StoreFamilyRevision trxn = new StoreFamilyRevision(0, 1000);
    try {

      byte[] data = ZKUtil.serialize(trxn);
      StoreFamilyRevision newWtx = new StoreFamilyRevision();
      ZKUtil.deserialize(newWtx, data);

      assertTrue(newWtx.getRevision() == trxn.getRevision());
      assertTrue(newWtx.getTimestamp() == trxn.getTimestamp());

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testWriteTransactionList() {
    List<StoreFamilyRevision> txnList = new ArrayList<StoreFamilyRevision>();
    long version;
    long timestamp;
    for (int i = 0; i < 10; i++) {
      version = i;
      timestamp = 1000 + i;
      StoreFamilyRevision wtx = new StoreFamilyRevision(version, timestamp);
      txnList.add(wtx);
    }

    StoreFamilyRevisionList wList = new StoreFamilyRevisionList(txnList);

    try {
      byte[] data = ZKUtil.serialize(wList);
      StoreFamilyRevisionList newList = new StoreFamilyRevisionList();
      ZKUtil.deserialize(newList, data);
      assertTrue(newList.getRevisionListSize() == wList.getRevisionListSize());

      Iterator<StoreFamilyRevision> itr = newList.getRevisionListIterator();
      int i = 0;
      while (itr.hasNext()) {
        StoreFamilyRevision txn = itr.next();
        assertTrue(txn.getRevision() == i);
        assertTrue(txn.getTimestamp() == (i + 1000));
        i++;
      }

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
