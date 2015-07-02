package org.apache.hive.hcatalog.streaming.mutate.client.lock;

import java.util.Collection;
import java.util.TimerTask;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hive.hcatalog.streaming.mutate.client.Transaction;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link TimerTask} that sends {@link IMetaStoreClient#heartbeat(long, long) heartbeat} events to the
 * {@link IMetaStoreClient meta store} to keet the {@link Lock} and {@link Transaction} alive. Nofifies the registered
 * {@link LockFailureListener} should the lock fail.
 */
class HeartbeatTimerTask extends TimerTask {

  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatTimerTask.class);

  private final IMetaStoreClient metaStoreClient;
  private final long lockId;
  private final Long transactionId;
  private final LockFailureListener listener;
  private final Collection<Table> tableDescriptors;

  HeartbeatTimerTask(IMetaStoreClient metaStoreClient, LockFailureListener listener, Long transactionId,
      Collection<Table> tableDescriptors, long lockId) {
    this.metaStoreClient = metaStoreClient;
    this.listener = listener;
    this.transactionId = transactionId;
    this.tableDescriptors = tableDescriptors;
    this.lockId = lockId;
    LOG.debug("Reporting to listener {}", listener);
  }

  @Override
  public void run() {
    try {
      // I'm assuming that there is no transaction ID for a read lock.
      metaStoreClient.heartbeat(transactionId == null ? 0 : transactionId, lockId);
      LOG.debug("Sent heartbeat for lock={}, transactionId={}", lockId, transactionId);
    } catch (NoSuchLockException | NoSuchTxnException | TxnAbortedException e) {
      failLock(e);
    } catch (TException e) {
      LOG.warn("Failed to send heartbeat to meta store.", e);
    }
  }

  private void failLock(Exception e) {
    LOG.debug("Lock " + lockId + " failed, cancelling heartbeat and notifiying listener: " + listener, e);
    // Cancel the heartbeat
    cancel();
    listener.lockFailed(lockId, transactionId, Lock.asStrings(tableDescriptors), e);
  }

  @Override
  public String toString() {
    return "HeartbeatTimerTask [lockId=" + lockId + ", transactionId=" + transactionId + "]";
  }

}