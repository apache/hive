/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.thrift.TException;


/**
 * Synchronized MetaStoreClient wrapper
 */
public final class SynchronizedMetaStoreClient {

  private final IMetaStoreClient client;

  public SynchronizedMetaStoreClient(IMetaStoreClient client) {
    this.client = client;
  }

  public synchronized long openTxn(String user) throws TException {
    return client.openTxn(user);
  }

  public synchronized void commitTxn(long txnid) throws TException {
    client.commitTxn(txnid);
  }

  public synchronized void rollbackTxn(long txnid) throws TException {
    client.rollbackTxn(txnid);
  }

  public synchronized void heartbeat(long txnid, long lockid) throws TException {
    client.heartbeat(txnid, lockid);
  }

  public synchronized ValidTxnList getValidTxns(long currentTxn) throws TException {
    return client.getValidTxns(currentTxn);
  }

  public synchronized LockResponse lock(LockRequest request) throws TException {
    return client.lock(request);
  }

  public synchronized Partition add_partition(Partition partition) throws TException {
    return client.add_partition(partition);
  }

  public synchronized void alter_partition(String dbName, String tblName,
      Partition newPart, EnvironmentContext environmentContext) throws TException {
    client.alter_partition(dbName, tblName, newPart, environmentContext);
  }

  public synchronized LockResponse checkLock(long lockid) throws TException {
    return client.checkLock(lockid);
  }

  public synchronized void unlock(long lockid) throws TException {
    client.unlock(lockid);
  }

  public synchronized ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
    return client.showLocks(showLocksRequest);
  }

  public synchronized void close() {
    client.close();
  }
}
