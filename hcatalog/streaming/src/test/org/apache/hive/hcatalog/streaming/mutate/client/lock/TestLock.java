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

import static org.apache.hadoop.hive.metastore.api.LockState.ABORT;
import static org.apache.hadoop.hive.metastore.api.LockState.ACQUIRED;
import static org.apache.hadoop.hive.metastore.api.LockState.NOT_ACQUIRED;
import static org.apache.hadoop.hive.metastore.api.LockState.WAITING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Timer;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSet;

@RunWith(MockitoJUnitRunner.class)
public class TestLock {

  private static final Table SOURCE_TABLE_1 = createTable("DB", "SOURCE_1");
  private static final Table SOURCE_TABLE_2 = createTable("DB", "SOURCE_2");
  private static final Table SINK_TABLE = createTable("DB", "SINK");
  private static final Set<Table> SOURCES = ImmutableSet.of(SOURCE_TABLE_1, SOURCE_TABLE_2);
  private static final Set<Table> SINKS = ImmutableSet.of(SINK_TABLE);
  private static final Set<Table> TABLES = ImmutableSet.of(SOURCE_TABLE_1, SOURCE_TABLE_2, SINK_TABLE);
  private static final long LOCK_ID = 42;
  private static final long TRANSACTION_ID = 109;
  private static final String USER = "ewest";

  @Mock
  private IMetaStoreClient mockMetaStoreClient;
  @Mock
  private LockFailureListener mockListener;
  @Mock
  private LockResponse mockLockResponse;
  @Mock
  private HeartbeatFactory mockHeartbeatFactory;
  @Mock
  private Timer mockHeartbeat;
  @Captor
  private ArgumentCaptor<LockRequest> requestCaptor;

  private Lock readLock;
  private Lock writeLock;
  private HiveConf configuration = new HiveConf();

  @Before
  public void injectMocks() throws Exception {
    when(mockMetaStoreClient.lock(any(LockRequest.class))).thenReturn(mockLockResponse);
    when(mockLockResponse.getLockid()).thenReturn(LOCK_ID);
    when(mockLockResponse.getState()).thenReturn(ACQUIRED);
    when(
        mockHeartbeatFactory.newInstance(any(IMetaStoreClient.class), any(LockFailureListener.class), any(Long.class),
            any(Collection.class), anyLong(), anyInt())).thenReturn(mockHeartbeat);

    readLock = new Lock(mockMetaStoreClient, mockHeartbeatFactory, configuration, mockListener, USER, SOURCES,
        Collections.<Table> emptySet(), 3, 0);
    writeLock = new Lock(mockMetaStoreClient, mockHeartbeatFactory, configuration, mockListener, USER, SOURCES, SINKS,
        3, 0);
  }

  @Test
  public void testAcquireReadLockWithNoIssues() throws Exception {
    readLock.acquire();
    assertEquals(Long.valueOf(LOCK_ID), readLock.getLockId());
    assertNull(readLock.getTransactionId());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAcquireWriteLockWithoutTxn() throws Exception {
    writeLock.acquire();
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testAcquireWriteLockWithInvalidTxn() throws Exception {
    writeLock.acquire(0);
  }

  @Test
  public void testAcquireTxnLockWithNoIssues() throws Exception {
    writeLock.acquire(TRANSACTION_ID);
    assertEquals(Long.valueOf(LOCK_ID), writeLock.getLockId());
    assertEquals(Long.valueOf(TRANSACTION_ID), writeLock.getTransactionId());
  }

  @Test
  public void testAcquireReadLockCheckHeartbeatCreated() throws Exception {
    configuration.set("hive.txn.timeout", "100s");
    readLock.acquire();

    verify(mockHeartbeatFactory).newInstance(eq(mockMetaStoreClient), eq(mockListener), any(Long.class), eq(SOURCES),
        eq(LOCK_ID), eq(75));
  }

  @Test
  public void testAcquireTxnLockCheckHeartbeatCreated() throws Exception {
    configuration.set("hive.txn.timeout", "100s");
    writeLock.acquire(TRANSACTION_ID);

    verify(mockHeartbeatFactory).newInstance(eq(mockMetaStoreClient), eq(mockListener), eq(TRANSACTION_ID),
        eq(TABLES), eq(LOCK_ID), eq(75));
  }

  @Test
  public void testAcquireLockCheckUser() throws Exception {
    readLock.acquire();
    verify(mockMetaStoreClient).lock(requestCaptor.capture());
    LockRequest actualRequest = requestCaptor.getValue();
    assertEquals(USER, actualRequest.getUser());
  }

  @Test
  public void testAcquireReadLockCheckLocks() throws Exception {
    readLock.acquire();
    verify(mockMetaStoreClient).lock(requestCaptor.capture());

    LockRequest request = requestCaptor.getValue();
    assertEquals(0, request.getTxnid());
    assertEquals(USER, request.getUser());
    assertEquals(InetAddress.getLocalHost().getHostName(), request.getHostname());

    List<LockComponent> components = request.getComponent();

    assertEquals(2, components.size());

    LockComponent expected1 = new LockComponent(LockType.SHARED_READ, LockLevel.TABLE, "DB");
    expected1.setTablename("SOURCE_1");
    expected1.setOperationType(DataOperationType.INSERT);
    expected1.setIsAcid(true);
    assertTrue(components.contains(expected1));

    LockComponent expected2 = new LockComponent(LockType.SHARED_READ, LockLevel.TABLE, "DB");
    expected2.setTablename("SOURCE_2");
    expected2.setOperationType(DataOperationType.INSERT);
    expected2.setIsAcid(true);
    assertTrue(components.contains(expected2));
  }

  @Test
  public void testAcquireTxnLockCheckLocks() throws Exception {
    writeLock.acquire(TRANSACTION_ID);
    verify(mockMetaStoreClient).lock(requestCaptor.capture());

    LockRequest request = requestCaptor.getValue();
    assertEquals(TRANSACTION_ID, request.getTxnid());
    assertEquals(USER, request.getUser());
    assertEquals(InetAddress.getLocalHost().getHostName(), request.getHostname());

    List<LockComponent> components = request.getComponent();

    assertEquals(3, components.size());

    LockComponent expected1 = new LockComponent(LockType.SHARED_READ, LockLevel.TABLE, "DB");
    expected1.setTablename("SOURCE_1");
    expected1.setOperationType(DataOperationType.INSERT);
    expected1.setIsAcid(true);
    assertTrue(components.contains(expected1));

    LockComponent expected2 = new LockComponent(LockType.SHARED_READ, LockLevel.TABLE, "DB");
    expected2.setTablename("SOURCE_2");
    expected2.setOperationType(DataOperationType.INSERT);
    expected2.setIsAcid(true);
    assertTrue(components.contains(expected2));

    LockComponent expected3 = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE, "DB");
    expected3.setTablename("SINK");
    expected3.setOperationType(DataOperationType.UPDATE);
    expected3.setIsAcid(true);
    assertTrue(components.contains(expected3));
  }

  @Test(expected = LockException.class)
  public void testAcquireLockNotAcquired() throws Exception {
    when(mockLockResponse.getState()).thenReturn(NOT_ACQUIRED);
    readLock.acquire();
  }

  @Test(expected = LockException.class)
  public void testAcquireLockAborted() throws Exception {
    when(mockLockResponse.getState()).thenReturn(ABORT);
    readLock.acquire();
  }

  @Test(expected = LockException.class)
  public void testAcquireLockWithWaitRetriesExceeded() throws Exception {
    when(mockLockResponse.getState()).thenReturn(WAITING, WAITING, WAITING);
    readLock.acquire();
  }

  @Test
  public void testAcquireLockWithWaitRetries() throws Exception {
    when(mockLockResponse.getState()).thenReturn(WAITING, WAITING, ACQUIRED);
    readLock.acquire();
    assertEquals(Long.valueOf(LOCK_ID), readLock.getLockId());
  }

  @Test
  public void testReleaseLock() throws Exception {
    readLock.acquire();
    readLock.release();
    verify(mockMetaStoreClient).unlock(LOCK_ID);
  }

  @Test
  public void testReleaseLockNoLock() throws Exception {
    readLock.release();
    verifyNoMoreInteractions(mockMetaStoreClient);
  }

  @Test
  public void testReleaseLockCancelsHeartbeat() throws Exception {
    readLock.acquire();
    readLock.release();
    verify(mockHeartbeat).cancel();
  }

  @Test
  public void testReadHeartbeat() throws Exception {
    HeartbeatTimerTask task = new HeartbeatTimerTask(mockMetaStoreClient, mockListener, null, SOURCES, LOCK_ID);
    task.run();
    verify(mockMetaStoreClient).heartbeat(0, LOCK_ID);
  }

  @Test
  public void testTxnHeartbeat() throws Exception {
    HeartbeatTimerTask task = new HeartbeatTimerTask(mockMetaStoreClient, mockListener, TRANSACTION_ID, SOURCES,
        LOCK_ID);
    task.run();
    verify(mockMetaStoreClient).heartbeat(TRANSACTION_ID, LOCK_ID);
  }

  @Test
  public void testReadHeartbeatFailsNoSuchLockException() throws Exception {
    Throwable t = new NoSuchLockException();
    doThrow(t).when(mockMetaStoreClient).heartbeat(0, LOCK_ID);
    HeartbeatTimerTask task = new HeartbeatTimerTask(mockMetaStoreClient, mockListener, null, SOURCES, LOCK_ID);
    task.run();
    verify(mockListener).lockFailed(LOCK_ID, null, Lock.asStrings(SOURCES), t);
  }

  @Test
  public void testTxnHeartbeatFailsNoSuchLockException() throws Exception {
    Throwable t = new NoSuchLockException();
    doThrow(t).when(mockMetaStoreClient).heartbeat(TRANSACTION_ID, LOCK_ID);
    HeartbeatTimerTask task = new HeartbeatTimerTask(mockMetaStoreClient, mockListener, TRANSACTION_ID, SOURCES,
        LOCK_ID);
    task.run();
    verify(mockListener).lockFailed(LOCK_ID, TRANSACTION_ID, Lock.asStrings(SOURCES), t);
  }

  @Test
  public void testHeartbeatFailsNoSuchTxnException() throws Exception {
    Throwable t = new NoSuchTxnException();
    doThrow(t).when(mockMetaStoreClient).heartbeat(TRANSACTION_ID, LOCK_ID);
    HeartbeatTimerTask task = new HeartbeatTimerTask(mockMetaStoreClient, mockListener, TRANSACTION_ID, SOURCES,
        LOCK_ID);
    task.run();
    verify(mockListener).lockFailed(LOCK_ID, TRANSACTION_ID, Lock.asStrings(SOURCES), t);
  }

  @Test
  public void testHeartbeatFailsTxnAbortedException() throws Exception {
    Throwable t = new TxnAbortedException();
    doThrow(t).when(mockMetaStoreClient).heartbeat(TRANSACTION_ID, LOCK_ID);
    HeartbeatTimerTask task = new HeartbeatTimerTask(mockMetaStoreClient, mockListener, TRANSACTION_ID, SOURCES,
        LOCK_ID);
    task.run();
    verify(mockListener).lockFailed(LOCK_ID, TRANSACTION_ID, Lock.asStrings(SOURCES), t);
  }

  @Test
  public void testHeartbeatContinuesTException() throws Exception {
    Throwable t = new TException();
    doThrow(t).when(mockMetaStoreClient).heartbeat(0, LOCK_ID);
    HeartbeatTimerTask task = new HeartbeatTimerTask(mockMetaStoreClient, mockListener, TRANSACTION_ID, SOURCES,
        LOCK_ID);
    task.run();
    verifyZeroInteractions(mockListener);
  }

  private static Table createTable(String databaseName, String tableName) {
    Table table = new Table();
    table.setDbName(databaseName);
    table.setTableName(tableName);
    return table;
  }

}
