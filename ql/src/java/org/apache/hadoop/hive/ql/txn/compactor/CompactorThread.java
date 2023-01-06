/*
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
package org.apache.hadoop.hive.ql.txn.compactor;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hive.common.ServerUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;

import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Superclass for all threads in the compactor.
 */
public abstract class CompactorThread extends Thread implements Configurable {
  static final private String CLASS_NAME = CompactorThread.class.getName();
  protected static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  protected HiveConf conf;

  protected AtomicBoolean stop;

  protected String hostName;
  protected String runtimeVersion;

  //Time threshold for compactor thread log
  //In milliseconds:
  private static final Integer MAX_WARN_LOG_TIME = 1200000; //20 min

  protected long checkInterval = 0;

  @Override
  public void setConf(Configuration configuration) {
    // TODO MS-SPLIT for now, keep a copy of HiveConf around as we need to call other methods with
    // it. This should be changed to Configuration once everything that this calls that requires
    // HiveConf is moved to the standalone metastore.
    //clone the conf - compactor needs to set properties in it which we don't
    // want to bleed into the caller
    conf = new HiveConf(configuration, HiveConf.class);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  public void init(AtomicBoolean stop) throws Exception {
    setPriority(MIN_PRIORITY);
    setDaemon(false);
    this.stop = stop;
    this.hostName = ServerUtils.hostname();
    this.runtimeVersion = getRuntimeVersion();
  }

  protected void checkInterrupt() throws InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException(getClass().getName() + " execution is interrupted.");
    }
  }

  /**
   * Find the table being compacted
   * @param ci compaction info returned from the compaction queue
   * @return metastore table
   * @throws org.apache.hadoop.hive.metastore.api.MetaException if the table cannot be found.
   */
  abstract Table resolveTable(CompactionInfo ci) throws MetaException;

  abstract boolean replIsCompactionDisabledForDatabase(String dbName) throws TException;

  /**
   * Get list of partitions by name.
   * @param ci compaction info.
   * @return list of partitions
   * @throws MetaException if an error occurs.
   */
  abstract List<Partition> getPartitionsByNames(CompactionInfo ci) throws MetaException;

  /**
   * Get the partition being compacted.
   * @param ci compaction info returned from the compaction queue
   * @return metastore partition, or null if there is not partition in this compaction info
   * @throws MetaException if underlying calls throw, or if the partition name resolves to more than
   * one partition.
   */
  protected Partition resolvePartition(CompactionInfo ci) throws MetaException {
    if (ci.partName != null) {
      List<Partition> parts;
      try {
        parts = getPartitionsByNames(ci);
        if (parts == null || parts.size() == 0) {
          // The partition got dropped before we went looking for it.
          return null;
        }
      } catch (Exception e) {
        LOG.error("Unable to find partition " + ci.getFullPartitionName(), e);
        throw e;
      }
      if (parts.size() != 1) {
        LOG.error(ci.getFullPartitionName() + " does not refer to a single partition. " +
                      Arrays.toString(parts.toArray()));
        throw new MetaException("Too many partitions for : " + ci.getFullPartitionName());
      }
      return parts.get(0);
    } else {
      return null;
    }
  }

  /**
   * Check for that special case when minor compaction is supported or not.
   * <ul>
   *   <li>The table is Insert-only OR</li>
   *   <li>Query based compaction is not enabled OR</li>
   *   <li>The table has only acid data in it.</li>
   * </ul>
   * @param tblproperties The properties of the table to check
   * @param dir The {@link AcidDirectory} instance pointing to the table's folder on the filesystem.
   * @return Returns true if minor compaction is supported based on the given parameters, false otherwise.
   */
  protected boolean isMinorCompactionSupported(Map<String, String> tblproperties, AcidDirectory dir) {
    //Query based Minor compaction is not possible for full acid tables having raw format (non-acid) data in them.
    return AcidUtils.isInsertOnlyTable(tblproperties) || !conf.getBoolVar(HiveConf.ConfVars.COMPACTOR_CRUD_QUERY_BASED)
            || !(dir.getOriginalFiles().size() > 0 || dir.getCurrentDirectories().stream().anyMatch(AcidUtils.ParsedDelta::isRawFormat));
  }

  /**
   * Get the storage descriptor for a compaction.
   * @param t table from {@link #resolveTable(org.apache.hadoop.hive.metastore.txn.CompactionInfo)}
   * @param p table from {@link #resolvePartition(org.apache.hadoop.hive.metastore.txn.CompactionInfo)}
   * @return metastore storage descriptor.
   */
  protected StorageDescriptor resolveStorageDescriptor(Table t, Partition p) {
    return (p == null) ? t.getSd() : p.getSd();
  }

  /**
   * Determine whether to run this job as the current user or whether we need a doAs to switch
   * users.
   * @param owner of the directory we will be working in, as determined by
   * {@link org.apache.hadoop.hive.metastore.txn.TxnUtils#findUserToRunAs(String, Table, Configuration)}
   * @return true if the job should run as the current user, false if a doAs is needed.
   */
  protected boolean runJobAsSelf(String owner) {
    return (owner.equals(System.getProperty("user.name")));
  }

  protected String tableName(Table t) {
    return Warehouse.getQualifiedName(t);
  }

  public static void initializeAndStartThread(CompactorThread thread,
      Configuration conf) throws Exception {
    LOG.info("Starting compactor thread of type " + thread.getClass().getName());
    thread.setConf(conf);
    thread.init(new AtomicBoolean());
    thread.start();
  }

  protected boolean replIsCompactionDisabledForTable(Table tbl) {
    // Compaction is disabled until after first successful incremental load. Check HIVE-21197 for more detail.
    boolean isCompactDisabled = ReplUtils.isFirstIncPending(tbl.getParameters());
    if (isCompactDisabled) {
      LOG.info("Compaction is disabled for table " + tbl.getTableName());
    }
    return isCompactDisabled;
  }

  @VisibleForTesting
  protected String getRuntimeVersion() {
    return this.getClass().getPackage().getImplementationVersion();
  }

  protected LockRequest createLockRequest(CompactionInfo ci, long txnId, LockType lockType, DataOperationType opType) {
    String agentInfo = Thread.currentThread().getName();
    LockRequestBuilder requestBuilder = new LockRequestBuilder(agentInfo);
    requestBuilder.setUser(ci.runAs);
    requestBuilder.setTransactionId(txnId);

    LockComponentBuilder lockCompBuilder = new LockComponentBuilder()
      .setLock(lockType)
      .setOperationType(opType)
      .setDbName(ci.dbname)
      .setTableName(ci.tableName)
      .setIsTransactional(true);

    if (ci.partName != null) {
      lockCompBuilder.setPartitionName(ci.partName);
    }
    requestBuilder.addLockComponent(lockCompBuilder.build());

    requestBuilder.setZeroWaitReadEnabled(!conf.getBoolVar(HiveConf.ConfVars.TXN_OVERWRITE_X_LOCK) ||
      !conf.getBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK));
    return requestBuilder.build();
  }

  protected void doPostLoopActions(long elapsedTime) throws InterruptedException {
    String threadTypeName = getClass().getName();
    if (elapsedTime < checkInterval && !stop.get()) {
      Thread.sleep(checkInterval - elapsedTime);
    }

    if (elapsedTime < MAX_WARN_LOG_TIME) {
      LOG.debug("{} loop took {} seconds to finish.", threadTypeName, elapsedTime/1000);
    } else {
      LOG.warn("Possible {} slowdown, loop took {} seconds to finish.", threadTypeName, elapsedTime/1000);
    }

  }
}
