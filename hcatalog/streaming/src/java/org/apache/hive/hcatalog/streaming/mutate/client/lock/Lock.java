package org.apache.hive.hcatalog.streaming.mutate.client.lock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the state required to safely read/write from/to an ACID table.
 */
public class Lock {

  private static final Logger LOG = LoggerFactory.getLogger(Lock.class);

  private static final double HEARTBEAT_FACTOR = 0.75;
  private static final int DEFAULT_HEARTBEAT_PERIOD = 275;

  private final IMetaStoreClient metaStoreClient;
  private final HeartbeatFactory heartbeatFactory;
  private final LockFailureListener listener;
  private final Collection<Table> tableDescriptors;
  private final int lockRetries;
  private final int retryWaitSeconds;
  private final String user;
  private final HiveConf hiveConf;

  private Timer heartbeat;
  private Long lockId;
  private Long transactionId;

  public Lock(IMetaStoreClient metaStoreClient, Options options) {
    this(metaStoreClient, new HeartbeatFactory(), options.hiveConf, options.listener, options.user,
        options.descriptors, options.lockRetries, options.retryWaitSeconds);
  }

  /** Visible for testing only. */
  Lock(IMetaStoreClient metaStoreClient, HeartbeatFactory heartbeatFactory, HiveConf hiveConf,
      LockFailureListener listener, String user, Collection<Table> tableDescriptors, int lockRetries,
      int retryWaitSeconds) {
    this.metaStoreClient = metaStoreClient;
    this.heartbeatFactory = heartbeatFactory;
    this.hiveConf = hiveConf;
    this.user = user;
    this.tableDescriptors = tableDescriptors;
    this.listener = listener;
    this.lockRetries = lockRetries;
    this.retryWaitSeconds = retryWaitSeconds;

    if (LockFailureListener.NULL_LISTENER.equals(listener)) {
      LOG.warn("No {} supplied. Data quality and availability cannot be assured.",
          LockFailureListener.class.getSimpleName());
    }
  }

  /** Attempts to acquire a read lock on the table, returns if successful, throws exception otherwise. */
  public void acquire() throws LockException {
    lockId = internalAcquire(null);
    initiateHeartbeat();
  }

  /** Attempts to acquire a read lock on the table, returns if successful, throws exception otherwise. */
  public void acquire(long transactionId) throws LockException {
    lockId = internalAcquire(transactionId);
    this.transactionId = transactionId;
    initiateHeartbeat();
  }

  /** Attempts to release the read lock on the table. Throws an exception if the lock failed at any point. */
  public void release() throws LockException {
    if (heartbeat != null) {
      heartbeat.cancel();
    }
    internalRelease();
  }

  public String getUser() {
    return user;
  }

  @Override
  public String toString() {
    return "Lock [metaStoreClient=" + metaStoreClient + ", lockId=" + lockId + ", transactionId=" + transactionId
        + "]";
  }

  private long internalAcquire(Long transactionId) throws LockException {
    int attempts = 0;
    LockRequest request = buildSharedLockRequest(transactionId);
    do {
      LockResponse response = null;
      try {
        response = metaStoreClient.lock(request);
      } catch (TException e) {
        throw new LockException("Unable to acquire lock for tables: [" + join(tableDescriptors) + "]", e);
      }
      if (response != null) {
        LockState state = response.getState();
        if (state == LockState.NOT_ACQUIRED || state == LockState.ABORT) {
          // I expect we'll only see NOT_ACQUIRED here?
          break;
        }
        if (state == LockState.ACQUIRED) {
          LOG.debug("Acquired lock {}", response.getLockid());
          return response.getLockid();
        }
        if (state == LockState.WAITING) {
          try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(retryWaitSeconds));
          } catch (InterruptedException e) {
          }
        }
      }
      attempts++;
    } while (attempts < lockRetries);
    throw new LockException("Could not acquire lock on tables: [" + join(tableDescriptors) + "]");
  }

  private void internalRelease() {
    try {
      // if there is a transaction then this lock will be released on commit/abort/rollback instead.
      if (lockId != null && transactionId == null) {
        metaStoreClient.unlock(lockId);
        LOG.debug("Released lock {}", lockId);
        lockId = null;
      }
    } catch (TException e) {
      LOG.error("Lock " + lockId + " failed.", e);
      listener.lockFailed(lockId, transactionId, asStrings(tableDescriptors), e);
    }
  }

  private LockRequest buildSharedLockRequest(Long transactionId) {
    LockRequestBuilder requestBuilder = new LockRequestBuilder();
    for (Table descriptor : tableDescriptors) {
      LockComponent component = new LockComponentBuilder()
          .setDbName(descriptor.getDbName())
          .setTableName(descriptor.getTableName())
          .setShared()
          .build();
      requestBuilder.addLockComponent(component);
    }
    if (transactionId != null) {
      requestBuilder.setTransactionId(transactionId);
    }
    LockRequest request = requestBuilder.setUser(user).build();
    return request;
  }

  private void initiateHeartbeat() {
    int heartbeatPeriod = getHeartbeatPeriod();
    LOG.debug("Heartbeat period {}s", heartbeatPeriod);
    heartbeat = heartbeatFactory.newInstance(metaStoreClient, listener, transactionId, tableDescriptors, lockId,
        heartbeatPeriod);
  }

  private int getHeartbeatPeriod() {
    int heartbeatPeriod = DEFAULT_HEARTBEAT_PERIOD;
    if (hiveConf != null) {
      // This value is always in seconds and includes an 's' suffix.
      String txTimeoutSeconds = hiveConf.getVar(HiveConf.ConfVars.HIVE_TXN_TIMEOUT);
      if (txTimeoutSeconds != null) {
        // We want to send the heartbeat at an interval that is less than the timeout.
        heartbeatPeriod = Math.max(1,
            (int) (Integer.parseInt(txTimeoutSeconds.substring(0, txTimeoutSeconds.length() - 1)) * HEARTBEAT_FACTOR));
      }
    }
    return heartbeatPeriod;
  }

  /** Visible for testing only. */
  Long getLockId() {
    return lockId;
  }

  /** Visible for testing only. */
  Long getTransactionId() {
    return transactionId;
  }

  /** Visible for testing only. */
  static String join(Iterable<? extends Object> values) {
    return StringUtils.join(values, ",");
  }

  /** Visible for testing only. */
  static List<String> asStrings(Collection<Table> tables) {
    List<String> strings = new ArrayList<>(tables.size());
    for (Table descriptor : tables) {
      strings.add(descriptor.getDbName() + "." + descriptor.getTableName());
    }
    return strings;
  }

  /** Constructs a lock options for a set of Hive ACID tables from which we wish to read. */
  public static final class Options {
    Set<Table> descriptors = new LinkedHashSet<>();
    LockFailureListener listener = LockFailureListener.NULL_LISTENER;
    int lockRetries = 5;
    int retryWaitSeconds = 30;
    String user;
    HiveConf hiveConf;

    /** Adds a table for which a shared read lock will be requested. */
    public Options addTable(String databaseName, String tableName) {
      checkNotNullOrEmpty(databaseName);
      checkNotNullOrEmpty(tableName);
      Table table = new Table();
      table.setDbName(databaseName);
      table.setTableName(tableName);
      descriptors.add(table);
      return this;
    }

    public Options user(String user) {
      checkNotNullOrEmpty(user);
      this.user = user;
      return this;
    }

    public Options configuration(HiveConf hiveConf) {
      checkNotNull(hiveConf);
      this.hiveConf = hiveConf;
      return this;
    }

    /** Sets a listener to handle failures of locks that were previously acquired. */
    public Options lockFailureListener(LockFailureListener listener) {
      checkNotNull(listener);
      this.listener = listener;
      return this;
    }

    public Options lockRetries(int lockRetries) {
      checkArgument(lockRetries > 0);
      this.lockRetries = lockRetries;
      return this;
    }

    public Options retryWaitSeconds(int retryWaitSeconds) {
      checkArgument(retryWaitSeconds > 0);
      this.retryWaitSeconds = retryWaitSeconds;
      return this;
    }

    private static void checkArgument(boolean value) {
      if (!value) {
        throw new IllegalArgumentException();
      }
    }

    private static void checkNotNull(Object value) {
      if (value == null) {
        throw new IllegalArgumentException();
      }
    }

    private static void checkNotNullOrEmpty(String value) {
      if (StringUtils.isBlank(value)) {
        throw new IllegalArgumentException();
      }
    }

  }

}
