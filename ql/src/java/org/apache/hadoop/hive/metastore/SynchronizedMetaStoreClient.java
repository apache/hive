/*
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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.CmRecycleResponse;
import org.apache.hadoop.hive.metastore.api.CmRecycleRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.WriteNotificationLogRequest;
import org.apache.hadoop.hive.metastore.api.WriteNotificationLogBatchRequest;
import org.apache.hadoop.hive.metastore.txn.TxnErrorMsg;
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
    AbortTxnRequest abortTxnRequest = new AbortTxnRequest(txnid);
    abortTxnRequest.setErrorCode(TxnErrorMsg.ABORT_ROLLBACK.getErrorCode());
    client.rollbackTxn(abortTxnRequest);
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

  public synchronized int add_partitions(List<Partition> partitions) throws TException {
    return client.add_partitions(partitions);
  }

  public synchronized void alter_partition(String catName, String dbName, String tblName,
      Partition newPart, EnvironmentContext environmentContext, String writeIdList) throws TException {
    client.alter_partition(catName, dbName, tblName, newPart, environmentContext, writeIdList);
  }

  public void alter_partitions(String catName, String dbName, String tblName,
                               List<Partition> partitions, EnvironmentContext environmentContext,
                               String writeIdList, long writeId) throws TException {
    client.alter_partitions(catName, dbName, tblName, partitions, environmentContext, writeIdList,
            writeId);
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

  public synchronized Partition getPartitionWithAuthInfo(String dbName, String tableName,
      List<String> pvals, String userName, List<String> groupNames)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    return client.getPartitionWithAuthInfo(dbName, tableName, pvals, userName, groupNames);
  }

  public synchronized Partition appendPartition(String db_name, String table_name, List<String> part_vals)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
	return client.appendPartition(db_name, table_name, part_vals);
  }

  public synchronized FireEventResponse fireListenerEvent(FireEventRequest rqst) throws TException {
    return client.fireListenerEvent(rqst);
  }

  public synchronized void addWriteNotificationLog(WriteNotificationLogRequest rqst) throws TException {
    client.addWriteNotificationLog(rqst);
  }

  public synchronized void addWriteNotificationLogInBatch(WriteNotificationLogBatchRequest rqst) throws TException {
    client.addWriteNotificationLogInBatch(rqst);
  }

  public synchronized CmRecycleResponse recycleDirToCmPath(CmRecycleRequest request) throws MetaException, TException {
    return client.recycleDirToCmPath(request);
  }

  public synchronized void close() {
    client.close();
  }

  public boolean isSameConfObj(Configuration c) {
    return client.isSameConfObj(c);
  }

  public boolean isCompatibleWith(Configuration c) {
    return client.isCompatibleWith(c);
  }
}
