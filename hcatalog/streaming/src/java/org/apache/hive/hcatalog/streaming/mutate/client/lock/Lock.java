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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
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
import org.apache.hadoop.hive.metastore.api.DataOperationType;
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
  private final Collection<Table> sinks;
  private final Collection<Table> tables = new HashSet<>();
  private final int lockRetries;
  private final int retryWaitSeconds;
  private final String user;
  private final HiveConf hiveConf;

  private Timer heartbeat;
  private Long lockId;
  private Long transactionId;

  public Lock(IMetaStoreClient metaStoreClient, Options options) {
    this(metaStoreClient, new HeartbeatFactory(), options.hiveConf, options.listener, options.user, options.sources,
        options.sinks, options.lockRetries, options.retryWaitSeconds);
  }

  /** Visible for testing only. */
  Lock(IMetaStoreClient metaStoreClient, HeartbeatFactory heartbeatFactory, HiveConf hiveConf,
      LockFailureListener listener, String user, Collection<Table> sources, Collection<Table> sinks, int lockRetries,
      int retryWaitSeconds) {
    this.metaStoreClient = metaStoreClient;
    this.heartbeatFactory = heartbeatFactory;
    this.hiveConf = hiveConf;
    this.user = user;
    this.listener = listener;
    this.lockRetries = lockRetries;
    this.retryWaitSeconds = retryWaitSeconds;

    this.sinks = sinks;
    tables.addAll(sources);
    tables.addAll(sinks);

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
    if (transactionId <= 0) {
      throw new IllegalArgumentException("Invalid transaction id: " + transactionId);
    }
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
    return "Lock [metaStoreClient=" + metaStoreClient + ", lockId=" + lockId + ", transactionId=" + transactionId + "]";
  }

  private long internalAcquire(Long transactionId) throws LockException {
    int attempts = 0;
    LockRequest request = buildLockRequest(transactionId);
    do {
      LockResponse response = null;
      try {
        response = metaStoreClient.lock(request);
      } catch (TException e) {
        throw new LockException("Unable to acquire lock for tables: [" + join(tables) + "]", e);
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
    throw new LockException("Could not acquire lock on tables: [" + join(tables) + "]");
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
      listener.lockFailed(lockId, transactionId, asStrings(tables), e);
    }
  }

  private LockRequest buildLockRequest(Long transactionId) {
    if (transactionId == null && !sinks.isEmpty()) {
      throw new IllegalArgumentException("Cannot sink to tables outside of a transaction: sinks=" + asStrings(sinks));
    }
    LockRequestBuilder requestBuilder = new LockRequestBuilder();
    for (Table table : tables) {
      LockComponentBuilder componentBuilder = new LockComponentBuilder().setDbName(table.getDbName()).setTableName(
          table.getTableName());
      //todo: DataOperationType is set conservatively here, we'd really want to distinguish update/delete
      //and insert/select and if resource (that is written to) is ACID or not
      if (sinks.contains(table)) {
        componentBuilder.setSemiShared().setOperationType(DataOperationType.UPDATE).setIsFullAcid(true);
      } else {
        componentBuilder.setShared().setOperationType(DataOperationType.INSERT).setIsFullAcid(true);
      }
      LockComponent component = componentBuilder.build();
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
    heartbeat = heartbeatFactory.newInstance(metaStoreClient, listener, transactionId, tables, lockId, heartbeatPeriod);
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
    Set<Table> sources = new LinkedHashSet<>();
    Set<Table> sinks = new LinkedHashSet<>();
    LockFailureListener listener = LockFailureListener.NULL_LISTENER;
    int lockRetries = 5;
    int retryWaitSeconds = 30;
    String user;
    HiveConf hiveConf;

    /** Adds a table for which a shared lock will be requested. */
    public Options addSourceTable(String databaseName, String tableName) {
      addTable(databaseName, tableName, sources);
      return this;
    }

    /** Adds a table for which a semi-shared lock will be requested. */
    public Options addSinkTable(String databaseName, String tableName) {
      addTable(databaseName, tableName, sinks);
      return this;
    }

    private void addTable(String databaseName, String tableName, Set<Table> tables) {
      checkNotNullOrEmpty(databaseName);
      checkNotNullOrEmpty(tableName);
      Table table = new Table();
      table.setDbName(databaseName);
      table.setTableName(tableName);
      tables.add(table);
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
