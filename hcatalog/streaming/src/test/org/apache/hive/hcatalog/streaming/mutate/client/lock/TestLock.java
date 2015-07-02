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
import java.util.List;
import java.util.Timer;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
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

import com.google.common.collect.ImmutableList;

@RunWith(MockitoJUnitRunner.class)
public class TestLock {

  private static final Table TABLE_1 = createTable("DB", "ONE");
  private static final Table TABLE_2 = createTable("DB", "TWO");
  private static final List<Table> TABLES = ImmutableList.of(TABLE_1, TABLE_2);
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

  private Lock lock;
  private HiveConf configuration = new HiveConf();

  @Before
  public void injectMocks() throws Exception {
    when(mockMetaStoreClient.lock(any(LockRequest.class))).thenReturn(mockLockResponse);
    when(mockLockResponse.getLockid()).thenReturn(LOCK_ID);
    when(mockLockResponse.getState()).thenReturn(ACQUIRED);
    when(
        mockHeartbeatFactory.newInstance(any(IMetaStoreClient.class), any(LockFailureListener.class), any(Long.class),
            any(Collection.class), anyLong(), anyInt())).thenReturn(mockHeartbeat);

    lock = new Lock(mockMetaStoreClient, mockHeartbeatFactory, configuration, mockListener, USER, TABLES, 3, 0);
  }

  @Test
  public void testAcquireReadLockWithNoIssues() throws Exception {
    lock.acquire();
    assertEquals(Long.valueOf(LOCK_ID), lock.getLockId());
    assertNull(lock.getTransactionId());
  }

  @Test
  public void testAcquireTxnLockWithNoIssues() throws Exception {
    lock.acquire(TRANSACTION_ID);
    assertEquals(Long.valueOf(LOCK_ID), lock.getLockId());
    assertEquals(Long.valueOf(TRANSACTION_ID), lock.getTransactionId());
  }

  @Test
  public void testAcquireReadLockCheckHeartbeatCreated() throws Exception {
    configuration.set("hive.txn.timeout", "100s");
    lock.acquire();

    verify(mockHeartbeatFactory).newInstance(eq(mockMetaStoreClient), eq(mockListener), any(Long.class), eq(TABLES),
        eq(LOCK_ID), eq(75));
  }

  @Test
  public void testAcquireTxnLockCheckHeartbeatCreated() throws Exception {
    configuration.set("hive.txn.timeout", "100s");
    lock.acquire(TRANSACTION_ID);

    verify(mockHeartbeatFactory).newInstance(eq(mockMetaStoreClient), eq(mockListener), eq(TRANSACTION_ID), eq(TABLES),
        eq(LOCK_ID), eq(75));
  }

  @Test
  public void testAcquireLockCheckUser() throws Exception {
    lock.acquire();
    verify(mockMetaStoreClient).lock(requestCaptor.capture());
    LockRequest actualRequest = requestCaptor.getValue();
    assertEquals(USER, actualRequest.getUser());
  }

  @Test
  public void testAcquireReadLockCheckLocks() throws Exception {
    lock.acquire();
    verify(mockMetaStoreClient).lock(requestCaptor.capture());

    LockRequest request = requestCaptor.getValue();
    assertEquals(0, request.getTxnid());
    assertEquals(USER, request.getUser());
    assertEquals(InetAddress.getLocalHost().getHostName(), request.getHostname());

    List<LockComponent> components = request.getComponent();

    assertEquals(2, components.size());

    LockComponent expected1 = new LockComponent(LockType.SHARED_READ, LockLevel.TABLE, "DB");
    expected1.setTablename("ONE");
    assertTrue(components.contains(expected1));

    LockComponent expected2 = new LockComponent(LockType.SHARED_READ, LockLevel.TABLE, "DB");
    expected2.setTablename("TWO");
    assertTrue(components.contains(expected2));
  }

  @Test
  public void testAcquireTxnLockCheckLocks() throws Exception {
    lock.acquire(TRANSACTION_ID);
    verify(mockMetaStoreClient).lock(requestCaptor.capture());

    LockRequest request = requestCaptor.getValue();
    assertEquals(TRANSACTION_ID, request.getTxnid());
    assertEquals(USER, request.getUser());
    assertEquals(InetAddress.getLocalHost().getHostName(), request.getHostname());

    List<LockComponent> components = request.getComponent();

    System.out.println(components);
    assertEquals(2, components.size());

    LockComponent expected1 = new LockComponent(LockType.SHARED_READ, LockLevel.TABLE, "DB");
    expected1.setTablename("ONE");
    assertTrue(components.contains(expected1));

    LockComponent expected2 = new LockComponent(LockType.SHARED_READ, LockLevel.TABLE, "DB");
    expected2.setTablename("TWO");
    assertTrue(components.contains(expected2));
  }

  @Test(expected = LockException.class)
  public void testAcquireLockNotAcquired() throws Exception {
    when(mockLockResponse.getState()).thenReturn(NOT_ACQUIRED);
    lock.acquire();
  }

  @Test(expected = LockException.class)
  public void testAcquireLockAborted() throws Exception {
    when(mockLockResponse.getState()).thenReturn(ABORT);
    lock.acquire();
  }

  @Test(expected = LockException.class)
  public void testAcquireLockWithWaitRetriesExceeded() throws Exception {
    when(mockLockResponse.getState()).thenReturn(WAITING, WAITING, WAITING);
    lock.acquire();
  }

  @Test
  public void testAcquireLockWithWaitRetries() throws Exception {
    when(mockLockResponse.getState()).thenReturn(WAITING, WAITING, ACQUIRED);
    lock.acquire();
    assertEquals(Long.valueOf(LOCK_ID), lock.getLockId());
  }

  @Test
  public void testReleaseLock() throws Exception {
    lock.acquire();
    lock.release();
    verify(mockMetaStoreClient).unlock(LOCK_ID);
  }

  @Test
  public void testReleaseLockNoLock() throws Exception {
    lock.release();
    verifyNoMoreInteractions(mockMetaStoreClient);
  }

  @Test
  public void testReleaseLockCancelsHeartbeat() throws Exception {
    lock.acquire();
    lock.release();
    verify(mockHeartbeat).cancel();
  }

  @Test
  public void testReadHeartbeat() throws Exception {
    HeartbeatTimerTask task = new HeartbeatTimerTask(mockMetaStoreClient, mockListener, null, TABLES, LOCK_ID);
    task.run();
    verify(mockMetaStoreClient).heartbeat(0, LOCK_ID);
  }

  @Test
  public void testTxnHeartbeat() throws Exception {
    HeartbeatTimerTask task = new HeartbeatTimerTask(mockMetaStoreClient, mockListener, TRANSACTION_ID, TABLES, LOCK_ID);
    task.run();
    verify(mockMetaStoreClient).heartbeat(TRANSACTION_ID, LOCK_ID);
  }

  @Test
  public void testReadHeartbeatFailsNoSuchLockException() throws Exception {
    Throwable t = new NoSuchLockException();
    doThrow(t).when(mockMetaStoreClient).heartbeat(0, LOCK_ID);
    HeartbeatTimerTask task = new HeartbeatTimerTask(mockMetaStoreClient, mockListener, null, TABLES, LOCK_ID);
    task.run();
    verify(mockListener).lockFailed(LOCK_ID, null, Lock.asStrings(TABLES), t);
  }

  @Test
  public void testTxnHeartbeatFailsNoSuchLockException() throws Exception {
    Throwable t = new NoSuchLockException();
    doThrow(t).when(mockMetaStoreClient).heartbeat(TRANSACTION_ID, LOCK_ID);
    HeartbeatTimerTask task = new HeartbeatTimerTask(mockMetaStoreClient, mockListener, TRANSACTION_ID, TABLES, LOCK_ID);
    task.run();
    verify(mockListener).lockFailed(LOCK_ID, TRANSACTION_ID, Lock.asStrings(TABLES), t);
  }

  @Test
  public void testHeartbeatFailsNoSuchTxnException() throws Exception {
    Throwable t = new NoSuchTxnException();
    doThrow(t).when(mockMetaStoreClient).heartbeat(TRANSACTION_ID, LOCK_ID);
    HeartbeatTimerTask task = new HeartbeatTimerTask(mockMetaStoreClient, mockListener, TRANSACTION_ID, TABLES, LOCK_ID);
    task.run();
    verify(mockListener).lockFailed(LOCK_ID, TRANSACTION_ID, Lock.asStrings(TABLES), t);
  }

  @Test
  public void testHeartbeatFailsTxnAbortedException() throws Exception {
    Throwable t = new TxnAbortedException();
    doThrow(t).when(mockMetaStoreClient).heartbeat(TRANSACTION_ID, LOCK_ID);
    HeartbeatTimerTask task = new HeartbeatTimerTask(mockMetaStoreClient, mockListener, TRANSACTION_ID, TABLES, LOCK_ID);
    task.run();
    verify(mockListener).lockFailed(LOCK_ID, TRANSACTION_ID, Lock.asStrings(TABLES), t);
  }

  @Test
  public void testHeartbeatContinuesTException() throws Exception {
    Throwable t = new TException();
    doThrow(t).when(mockMetaStoreClient).heartbeat(0, LOCK_ID);
    HeartbeatTimerTask task = new HeartbeatTimerTask(mockMetaStoreClient, mockListener, TRANSACTION_ID, TABLES, LOCK_ID);
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
