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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.ValidCompactorWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreThread;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A class to do compactions.  This will run in a separate thread.  It will spin on the
 * compaction queue and look for new work to do.
 */
public class Worker extends RemoteCompactorThread implements MetaStoreThread {
  static final private String CLASS_NAME = Worker.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  static final private long SLEEP_TIME = 10000;

  private String workerName;
  private JobConf mrJob; // the MR job for compaction

  /**
   * Get the hostname that this worker is run on.  Made static and public so that other classes
   * can use the same method to know what host their worker threads are running on.
   * @return hostname
   */
  public static String hostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Unable to resolve my host name " + e.getMessage());
      throw new RuntimeException(e);
    }
  }
//todo: this doesn;t check if compaction is already running (even though Initiator does but we
// don't go  through Initiator for user initiated compactions)
  @Override
  public void run() {
    LOG.info("Starting Worker thread");
    do {
      boolean launchedJob = false;
      // Make sure nothing escapes this run method and kills the metastore at large,
      // so wrap it in a big catch Throwable statement.
      CompactionHeartbeater heartbeater = null;
      try {
        if (msc == null) {
          msc = HiveMetaStoreUtils.getHiveMetastoreClient(conf);
        }
        final CompactionInfo ci = CompactionInfo.optionalCompactionInfoStructToInfo(
            msc.findNextCompact(workerName));
        LOG.debug("Processing compaction request " + ci);

        if (ci == null && !stop.get()) {
          try {
            Thread.sleep(SLEEP_TIME);
            continue;
          } catch (InterruptedException e) {
            LOG.warn("Worker thread sleep interrupted " + e.getMessage());
            continue;
          }
        }

        // Find the table we will be working with.
        Table t1 = null;
        try {
          t1 = resolveTable(ci);
          if (t1 == null) {
            LOG.info("Unable to find table " + ci.getFullTableName() +
                ", assuming it was dropped and moving on.");
            msc.markCleaned(CompactionInfo.compactionInfoToStruct(ci));
            continue;
          }
        } catch (MetaException e) {
          msc.markCleaned(CompactionInfo.compactionInfoToStruct(ci));
          continue;
        }
        // This chicanery is to get around the fact that the table needs to be final in order to
        // go into the doAs below.
        final Table t = t1;

        // Find the partition we will be working with, if there is one.
        Partition p = null;
        try {
          p = resolvePartition(ci);
          if (p == null && ci.partName != null) {
            LOG.info("Unable to find partition " + ci.getFullPartitionName() +
                ", assuming it was dropped and moving on.");
            msc.markCleaned(CompactionInfo.compactionInfoToStruct(ci));
            continue;
          }
        } catch (Exception e) {
          msc.markCleaned(CompactionInfo.compactionInfoToStruct(ci));
          continue;
        }

        // Find the appropriate storage descriptor
        final StorageDescriptor sd =  resolveStorageDescriptor(t, p);

        // Check that the table or partition isn't sorted, as we don't yet support that.
        if (sd.getSortCols() != null && !sd.getSortCols().isEmpty()) {
          LOG.error("Attempt to compact sorted table "+ci.getFullTableName()+", which is not yet supported!");
          msc.markCleaned(CompactionInfo.compactionInfoToStruct(ci));
          continue;
        }
        String fullTableName = TxnUtils.getFullTableName(t.getDbName(), t.getTableName());
        if (ci.runAs == null) {
          ci.runAs = findUserToRunAs(sd.getLocation(), t);
        }
        /**
         * we cannot have Worker use HiveTxnManager (which is on ThreadLocal) since
         * then the Driver would already have the an open txn but then this txn would have
         * multiple statements in it (for query based compactor) which is not supported (and since
         * this case some of the statements are DDL, even in the future will not be allowed in a
         * multi-stmt txn. {@link Driver#setCompactionWriteIds(ValidWriteIdList, long)} */
        long compactorTxnId = msc.openTxn(ci.runAs, TxnType.COMPACTION);

        heartbeater = new CompactionHeartbeater(compactorTxnId, fullTableName, conf);
        heartbeater.start();

        ValidTxnList validTxnList = msc.getValidTxns(compactorTxnId);
        //with this ValidWriteIdList is capped at whatever HWM validTxnList has
        final ValidCompactorWriteIdList tblValidWriteIds =
                TxnUtils.createValidCompactWriteIdList(msc.getValidWriteIds(
                    Collections.singletonList(fullTableName), validTxnList.writeToString()).get(0));
        LOG.debug("ValidCompactWriteIdList: " + tblValidWriteIds.writeToString());
        conf.set(ValidTxnList.VALID_TXNS_KEY, validTxnList.writeToString());

        ci.highestWriteId = tblValidWriteIds.getHighWatermark();
        //this writes TXN_COMPONENTS to ensure that if compactorTxnId fails, we keep metadata about
        //it until after any data written by it are physically removed
        msc.updateCompactorState(CompactionInfo.compactionInfoToStruct(ci), compactorTxnId);
        final StringBuilder jobName = new StringBuilder(workerName);
        jobName.append("-compactor-");
        jobName.append(ci.getFullPartitionName());

        LOG.info("Starting " + ci.type.toString() + " compaction for " + ci.getFullPartitionName() + " in " + JavaUtils.txnIdToString(compactorTxnId));
        final StatsUpdater su = StatsUpdater.init(ci, msc.findColumnsWithStats(
            CompactionInfo.compactionInfoToStruct(ci)), conf,
          runJobAsSelf(ci.runAs) ? ci.runAs : t.getOwner());
        final CompactorMR mr = new CompactorMR();
        launchedJob = true;
        try {
          if (runJobAsSelf(ci.runAs)) {
            mr.run(conf, jobName.toString(), t, p, sd, tblValidWriteIds, ci, su, msc);
          } else {
            UserGroupInformation ugi = UserGroupInformation.createProxyUser(t.getOwner(),
              UserGroupInformation.getLoginUser());
            final Partition fp = p;
            ugi.doAs(new PrivilegedExceptionAction<Object>() {
              @Override
              public Object run() throws Exception {
                mr.run(conf, jobName.toString(), t, fp, sd, tblValidWriteIds, ci, su, msc);
                return null;
              }
            });
            try {
              FileSystem.closeAllForUGI(ugi);
            } catch (IOException exception) {
              LOG.error("Could not clean up file-system handles for UGI: " + ugi + " for " +
                  ci.getFullPartitionName(), exception);
            }
          }
          heartbeater.cancel();
          msc.markCompacted(CompactionInfo.compactionInfoToStruct(ci));
          msc.commitTxn(compactorTxnId);
          if (conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST)) {
            mrJob = mr.getMrJob();
          }
        } catch (Throwable e) {
          LOG.error("Caught exception while trying to compact " + ci +
              ".  Marking failed to avoid repeated failures, " + StringUtils.stringifyException(e));
          msc.markFailed(CompactionInfo.compactionInfoToStruct(ci));
          msc.abortTxns(Collections.singletonList(compactorTxnId));
        }
      } catch (TException | IOException t) {
        LOG.error("Caught an exception in the main loop of compactor worker " + workerName + ", " +
            StringUtils.stringifyException(t));
        if (msc != null) {
          msc.close();
        }
        msc = null;
        try {
          Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e) {
          LOG.error("Interrupted while sleeping to instantiate metastore client");
        }
      } catch (Throwable t) {
        LOG.error("Caught an exception in the main loop of compactor worker " + workerName + ", " +
            StringUtils.stringifyException(t));
      } finally {
        if(heartbeater != null) {
          heartbeater.cancel();
        }
      }

      // If we didn't try to launch a job it either means there was no work to do or we got
      // here as the result of a communication failure with the DB.  Either way we want to wait
      // a bit before we restart the loop.
      if (!launchedJob && !stop.get()) {
        try {
          Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e) {
        }
      }
    } while (!stop.get());
  }

  @Override
  public void init(AtomicBoolean stop, AtomicBoolean looped) throws Exception {
    super.init(stop, looped);

    StringBuilder name = new StringBuilder(hostname());
    name.append("-");
    name.append(getId());
    this.workerName = name.toString();
    setName(name.toString());
  }

  public JobConf getMrJob() {
    return mrJob;
  }

  static final class StatsUpdater {
    static final private Logger LOG = LoggerFactory.getLogger(StatsUpdater.class);

    public static StatsUpdater init(CompactionInfo ci, List<String> columnListForStats,
        HiveConf conf, String userName) {
      return new StatsUpdater(ci, columnListForStats, conf, userName);
    }

    /**
     * list columns for which to compute stats.  This maybe empty which means no stats gathering
     * is needed.
     */
    private final List<String> columnList;
    private final HiveConf conf;
    private final String userName;
    private final CompactionInfo ci;

    private StatsUpdater(CompactionInfo ci, List<String> columnListForStats,
        HiveConf conf, String userName) {
      this.conf = new HiveConf(conf);
      //so that Driver doesn't think it's arleady in a transaction
      this.conf.unset(ValidTxnList.VALID_TXNS_KEY);
      this.userName = userName;
      this.ci = ci;
      if (!ci.isMajorCompaction() || columnListForStats == null || columnListForStats.isEmpty()) {
        columnList = Collections.emptyList();
        return;
      }
      columnList = columnListForStats;
    }

    /**
     * This doesn't throw any exceptions because we don't want the Compaction to appear as failed
     * if stats gathering fails since this prevents Cleaner from doing it's job and if there are
     * multiple failures, auto initiated compactions will stop which leads to problems that are
     * much worse than stale stats.
     *
     * todo: longer term we should write something COMPACTION_QUEUE.CQ_META_INFO.  This is a binary
     * field so need to figure out the msg format and how to surface it in SHOW COMPACTIONS, etc
     */
    void gatherStats() {
      try {
        if (!ci.isMajorCompaction()) {
          return;
        }
        if (columnList.isEmpty()) {
          LOG.debug(ci + ": No existing stats found.  Will not run analyze.");
          return;//nothing to do
        }
        //e.g. analyze table page_view partition(dt='10/15/2014',country=’US’)
        // compute statistics for columns viewtime
        StringBuilder sb = new StringBuilder("analyze table ")
            .append(StatsUtils.getFullyQualifiedTableName(ci.dbname, ci.tableName));
        if (ci.partName != null) {
          sb.append(" partition(");
          Map<String, String> partitionColumnValues = Warehouse.makeEscSpecFromName(ci.partName);
          for (Map.Entry<String, String> ent : partitionColumnValues.entrySet()) {
            sb.append(ent.getKey()).append("='").append(ent.getValue()).append("',");
          }
          sb.setLength(sb.length() - 1); //remove trailing ,
          sb.append(")");
        }
        sb.append(" compute statistics for columns ");
        for (String colName : columnList) {
          sb.append(colName).append(",");
        }
        sb.setLength(sb.length() - 1); //remove trailing ,
        LOG.info(ci + ": running '" + sb.toString() + "'");
        conf.setVar(HiveConf.ConfVars.METASTOREURIS,"");

        //todo: use DriverUtils.runOnDriver() here
        QueryState queryState = new QueryState.Builder().withGenerateNewQueryId(true).withHiveConf(conf).build();
        SessionState localSession = null;
        try (Driver d = new Driver(queryState, userName)) {
          if (SessionState.get() == null) {
            localSession = new SessionState(conf);
            SessionState.start(localSession);
          }
          try {
            d.run(sb.toString());
          } catch (CommandProcessorException e) {
            LOG.warn(ci + ": " + sb.toString() + " failed due to: " + e);
          }
        } finally {
          if (localSession != null) {
            try {
              localSession.close();
            } catch (IOException ex) {
              LOG.warn(ci + ": localSession.close() failed due to: " + ex.getMessage(), ex);
            }
          }
        }
      } catch (Throwable t) {
        LOG.error(ci + ": gatherStats(" + ci.dbname + "," + ci.tableName + "," + ci.partName +
                      ") failed due to: " + t.getMessage(), t);
      }
    }
  }

  static final class CompactionHeartbeater extends Thread {
    static final private Logger LOG = LoggerFactory.getLogger(CompactionHeartbeater.class);
    private final AtomicBoolean stop = new AtomicBoolean();
    private final long compactorTxnId;
    private final String tableName;
    private final HiveConf conf;
    private final long interval;
    public CompactionHeartbeater(long compactorTxnId, String tableName, HiveConf conf) {
      this.tableName = tableName;
      this.compactorTxnId = compactorTxnId;
      this.conf = conf;

      this.interval =
          MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.TXN_TIMEOUT, TimeUnit.MILLISECONDS) / 2;
      setDaemon(true);
      setPriority(MIN_PRIORITY);
      setName("CompactionHeartbeater-" + compactorTxnId);
    }
    @Override
    public void run() {
      try {
        // We need to create our own metastore client since the thrifts clients
        // are not thread safe.
        IMetaStoreClient msc = HiveMetaStoreUtils.getHiveMetastoreClient(conf);
        LOG.debug("Heartbeating compaction transaction id {} for table: {}", compactorTxnId, tableName);
        while(!stop.get()) {
          msc.heartbeat(compactorTxnId, 0);
          Thread.sleep(interval);
        }
      } catch (Exception e) {
        LOG.error("Error while heartbeating txn {} in {}, error: ", compactorTxnId, Thread.currentThread().getName(), e.getMessage());
      }
    }

    public void cancel() {
      if(!this.stop.get()) {
        LOG.debug("Successfully stop the heartbeating the transaction {}", this.compactorTxnId);
        this.stop.set(true);
      }
    }
  }
}
