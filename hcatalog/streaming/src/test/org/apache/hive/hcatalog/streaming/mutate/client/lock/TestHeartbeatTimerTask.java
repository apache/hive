/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.hcatalog.streaming.mutate.client.lock;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestHeartbeatTimerTask {

  private static final long TRANSACTION_ID = 10L;
  private static final long LOCK_ID = 1L;
  private static final List<Table> TABLES = createTable();

  @Mock
  private IMetaStoreClient mockMetaStoreClient;
  @Mock
  private LockFailureListener mockListener;

  private HeartbeatTimerTask task;

  @Before
  public void create() throws Exception {
    task = new HeartbeatTimerTask(mockMetaStoreClient, mockListener, TRANSACTION_ID, TABLES, LOCK_ID);
  }

  @Test
  public void testRun() throws Exception {
    task.run();

    verify(mockMetaStoreClient).heartbeat(TRANSACTION_ID, LOCK_ID);
  }

  @Test
  public void testRunNullTransactionId() throws Exception {
    task = new HeartbeatTimerTask(mockMetaStoreClient, mockListener, null, TABLES, LOCK_ID);

    task.run();

    verify(mockMetaStoreClient).heartbeat(0, LOCK_ID);
  }

  @Test
  public void testRunHeartbeatFailsNoSuchLockException() throws Exception {
    NoSuchLockException exception = new NoSuchLockException();
    doThrow(exception).when(mockMetaStoreClient).heartbeat(TRANSACTION_ID, LOCK_ID);

    task.run();

    verify(mockListener).lockFailed(LOCK_ID, TRANSACTION_ID, Arrays.asList("DB.TABLE"), exception);
  }

  @Test
  public void testRunHeartbeatFailsNoSuchTxnException() throws Exception {
    NoSuchTxnException exception = new NoSuchTxnException();
    doThrow(exception).when(mockMetaStoreClient).heartbeat(TRANSACTION_ID, LOCK_ID);

    task.run();

    verify(mockListener).lockFailed(LOCK_ID, TRANSACTION_ID, Arrays.asList("DB.TABLE"), exception);
  }

  @Test
  public void testRunHeartbeatFailsTxnAbortedException() throws Exception {
    TxnAbortedException exception = new TxnAbortedException();
    doThrow(exception).when(mockMetaStoreClient).heartbeat(TRANSACTION_ID, LOCK_ID);

    task.run();

    verify(mockListener).lockFailed(LOCK_ID, TRANSACTION_ID, Arrays.asList("DB.TABLE"), exception);
  }

  @Test
  public void testRunHeartbeatFailsTException() throws Exception {
    TException exception = new TException();
    doThrow(exception).when(mockMetaStoreClient).heartbeat(TRANSACTION_ID, LOCK_ID);

    task.run();
  }

  private static List<Table> createTable() {
    Table table = new Table();
    table.setDbName("DB");
    table.setTableName("TABLE");
    return Arrays.asList(table);
  }
}
