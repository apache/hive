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
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A class to do compactions.  This will run in a separate thread.  It will spin on the
 * compaction queue and look for new work to do.
 */
public class Worker extends CompactorThread {
  static final private String CLASS_NAME = Worker.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  static final private long SLEEP_TIME = 5000;
  static final private int baseThreadNum = 10002;

  private String name;
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
    do {
      boolean launchedJob = false;
      // Make sure nothing escapes this run method and kills the metastore at large,
      // so wrap it in a big catch Throwable statement.
      try {
        final CompactionInfo ci = txnHandler.findNextToCompact(name);

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
            txnHandler.markCleaned(ci);
            continue;
          }
        } catch (MetaException e) {
          txnHandler.markCleaned(ci);
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
            txnHandler.markCleaned(ci);
            continue;
          }
        } catch (Exception e) {
          txnHandler.markCleaned(ci);
          continue;
        }

        // Find the appropriate storage descriptor
        final StorageDescriptor sd =  resolveStorageDescriptor(t, p);

        // Check that the table or partition isn't sorted, as we don't yet support that.
        if (sd.getSortCols() != null && !sd.getSortCols().isEmpty()) {
          LOG.error("Attempt to compact sorted table, which is not yet supported!");
          txnHandler.markCleaned(ci);
          continue;
        }

        final boolean isMajor = ci.isMajorCompaction();
        final ValidTxnList txns =
            TxnUtils.createValidCompactTxnList(txnHandler.getOpenTxnsInfo());
        LOG.debug("ValidCompactTxnList: " + txns.writeToString());
        txnHandler.setCompactionHighestTxnId(ci, txns.getHighWatermark());
        final StringBuilder jobName = new StringBuilder(name);
        jobName.append("-compactor-");
        jobName.append(ci.getFullPartitionName());

        // Determine who to run as
        String runAs;
        if (ci.runAs == null) {
          runAs = findUserToRunAs(sd.getLocation(), t);
          txnHandler.setRunAs(ci.id, runAs);
        } else {
          runAs = ci.runAs;
        }

        LOG.info("Starting " + ci.type.toString() + " compaction for " +
            ci.getFullPartitionName());

        final StatsUpdater su = StatsUpdater.init(ci, txnHandler.findColumnsWithStats(ci), conf,
          runJobAsSelf(runAs) ? runAs : t.getOwner());
        final CompactorMR mr = new CompactorMR();
        launchedJob = true;
        try {
          if (runJobAsSelf(runAs)) {
            mr.run(conf, jobName.toString(), t, sd, txns, ci, su, txnHandler);
          } else {
            UserGroupInformation ugi = UserGroupInformation.createProxyUser(t.getOwner(),
              UserGroupInformation.getLoginUser());
            ugi.doAs(new PrivilegedExceptionAction<Object>() {
              @Override
              public Object run() throws Exception {
                mr.run(conf, jobName.toString(), t, sd, txns, ci, su, txnHandler);
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
          txnHandler.markCompacted(ci);
          if (conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST)) {
            mrJob = mr.getMrJob();
          }
        } catch (Exception e) {
          LOG.error("Caught exception while trying to compact " + ci +
              ".  Marking failed to avoid repeated failures, " + StringUtils.stringifyException(e));
          txnHandler.markFailed(ci);
        }
      } catch (Throwable t) {
        LOG.error("Caught an exception in the main loop of compactor worker " + name + ", " +
            StringUtils.stringifyException(t));
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
  public void init(AtomicBoolean stop, AtomicBoolean looped) throws MetaException {
    super.init(stop, looped);

    StringBuilder name = new StringBuilder(hostname());
    name.append("-");
    name.append(getId());
    this.name = name.toString();
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
      this.conf = conf;
      this.userName = userName;
      this.ci = ci;
      if(!ci.isMajorCompaction() || columnListForStats == null || columnListForStats.isEmpty()) {
        columnList = Collections.emptyList();
        return;
      }
      columnList = columnListForStats;
    }

    /**
     * todo: what should this do on failure?  Should it rethrow? Invalidate stats?
     */
    void gatherStats() throws IOException {
      if(!ci.isMajorCompaction()) {
        return;
      }
      if(columnList.isEmpty()) {
        LOG.debug("No existing stats for "
            + StatsUtils.getFullyQualifiedTableName(ci.dbname, ci.tableName)
            + " found.  Will not run analyze.");
        return;//nothing to do
      }
      //e.g. analyze table page_view partition(dt='10/15/2014',country=’US’)
      // compute statistics for columns viewtime
      StringBuilder sb = new StringBuilder("analyze table ")
          .append(StatsUtils.getFullyQualifiedTableName(ci.dbname, ci.tableName));
      if(ci.partName != null) {
        try {
          sb.append(" partition(");
          Map<String, String> partitionColumnValues = Warehouse.makeEscSpecFromName(ci.partName);
          for(Map.Entry<String, String> ent : partitionColumnValues.entrySet()) {
            sb.append(ent.getKey()).append("='").append(ent.getValue()).append("',");
          }
          sb.setLength(sb.length() - 1); //remove trailing ,
          sb.append(")");
        }
        catch(MetaException ex) {
          throw new IOException(ex);
        }
      }
      sb.append(" compute statistics for columns ");
      for(String colName : columnList) {
        sb.append(colName).append(",");
      }
      sb.setLength(sb.length() - 1); //remove trailing ,
      LOG.info("running '" + sb.toString() + "'");
      Driver d = new Driver(new QueryState.Builder().withGenerateNewQueryId(true).withHiveConf(conf).build(), userName);
      SessionState localSession = null;
      if(SessionState.get() == null) {
         localSession = SessionState.start(new SessionState(conf));
      }
      try {
        CommandProcessorResponse cpr = d.run(sb.toString());
        if (cpr.getResponseCode() != 0) {
          throw new IOException("Could not update stats for table " + ci.getFullTableName() +
            (ci.partName == null ? "" : "/" + ci.partName) + " due to: " + cpr);
        }
      }
      finally {
        if(localSession != null) {
          localSession.close();
        }
      }
    }
  }
}
