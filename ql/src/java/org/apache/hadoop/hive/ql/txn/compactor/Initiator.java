/**
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnHandler;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Set;

/**
 * A class to initiate compactions.  This will run in a separate thread.
 */
public class Initiator extends CompactorThread {
  static final private String CLASS_NAME = Initiator.class.getName();
  static final private Log LOG = LogFactory.getLog(CLASS_NAME);
  static final private int threadId = 10000;

  static final private String NO_COMPACTION = "NO_AUTO_COMPACTION";

  private long checkInterval;

  @Override
  public void run() {
    // Make sure nothing escapes this run method and kills the metastore at large,
    // so wrap it in a big catch Throwable statement.
    try {
      recoverFailedCompactions(false);

      int abortedThreashold = HiveConf.getIntVar(conf,
          HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD);

      // Make sure we run through the loop once before checking to stop as this makes testing
      // much easier.  The stop value is only for testing anyway and not used when called from
      // HiveMetaStore.
      do {
        long startedAt = System.currentTimeMillis();

        // Wrap the inner parts of the loop in a catch throwable so that any errors in the loop
        // don't doom the entire thread.
        try {
          ShowCompactResponse currentCompactions = txnHandler.showCompact(new ShowCompactRequest());
          ValidTxnList txns = TxnHandler.createValidTxnList(txnHandler.getOpenTxns());
          Set<CompactionInfo> potentials = txnHandler.findPotentialCompactions(abortedThreashold);
          LOG.debug("Found " + potentials.size() + " potential compactions, " +
              "checking to see if we should compact any of them");
          for (CompactionInfo ci : potentials) {
            LOG.debug("Checking to see if we should compact " + ci.getFullPartitionName());
            try {
              Table t = resolveTable(ci);
              // check if no compaction set for this table
              if (t.getParameters().get(NO_COMPACTION) != null) {
                LOG.info("Table " + tableName(t) + " marked " +  NO_COMPACTION +
                    " so we will not compact it.");
                continue;
              }

              // Check if we already have initiated or are working on a compaction for this partition
              // or table.  If so, skip it.  If we are just waiting on cleaning we can still check,
              // as it may be time to compact again even though we haven't cleaned.
              if (lookForCurrentCompactions(currentCompactions, ci)) {
                LOG.debug("Found currently initiated or working compaction for " +
                    ci.getFullPartitionName() + " so we will not initiate another compaction");
                continue;
              }

              // Figure out who we should run the file operations as
              Partition p = resolvePartition(ci);
              StorageDescriptor sd = resolveStorageDescriptor(t, p);
              String runAs = findUserToRunAs(sd.getLocation(), t);

              CompactionType compactionNeeded = checkForCompaction(ci, txns, sd, runAs);
              if (compactionNeeded != null) requestCompaction(ci, runAs, compactionNeeded);
            } catch (Throwable t) {
              LOG.error("Caught exception while trying to determine if we should compact " +
                  ci.getFullPartitionName() + ".  Marking clean to avoid repeated failures, " +
                  "" + StringUtils.stringifyException(t));
              txnHandler.markCleaned(ci);
            }
          }

          // Check for timed out remote workers.
          recoverFailedCompactions(true);

          // Clean anything from the txns table that has no components left in txn_components.
          txnHandler.cleanEmptyAbortedTxns();
        } catch (Throwable t) {
          LOG.error("Initiator loop caught unexpected exception this time through the loop: " +
              StringUtils.stringifyException(t));
        }

        long elapsedTime = System.currentTimeMillis() - startedAt;
        if (elapsedTime >= checkInterval || stop.boolVal)  continue;
        else Thread.sleep(checkInterval - elapsedTime);

      } while (!stop.boolVal);
    } catch (Throwable t) {
      LOG.error("Caught an exception in the main loop of compactor initiator, exiting " +
          StringUtils.stringifyException(t));
    }
  }

  @Override
  public void init(BooleanPointer stop) throws MetaException {
    super.init(stop);
    checkInterval =
        HiveConf.getLongVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_CHECK_INTERVAL) * 1000;
  }

  private void recoverFailedCompactions(boolean remoteOnly) throws MetaException {
    if (!remoteOnly) txnHandler.revokeFromLocalWorkers(Worker.hostname());
    txnHandler.revokeTimedoutWorkers(HiveConf.getLongVar(conf,
        HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_TIMEOUT));
  }

  // Figure out if there are any currently running compactions on the same table or partition.
  private boolean lookForCurrentCompactions(ShowCompactResponse compactions,
                                            CompactionInfo ci) {
    if (compactions.getCompacts() != null) {
      for (ShowCompactResponseElement e : compactions.getCompacts()) {
        if (!e.getState().equals(TxnHandler.CLEANING_RESPONSE) &&
            e.getDbname().equals(ci.dbname) &&
            e.getTablename().equals(ci.tableName) &&
            (e.getPartitionname() == null && ci.partName == null ||
                  e.getPartitionname().equals(ci.partName))) {
          return true;
        }
      }
    }
    return false;
  }

  private CompactionType checkForCompaction(final CompactionInfo ci,
                                            final ValidTxnList txns,
                                            final StorageDescriptor sd,
                                            final String runAs)
      throws IOException, InterruptedException {
    // If it's marked as too many aborted, we already know we need to compact
    if (ci.tooManyAborts) {
      LOG.debug("Found too many aborted transactions for " + ci.getFullPartitionName() + ", " +
          "initiating major compaction");
      return CompactionType.MAJOR;
    }
    if (runJobAsSelf(runAs)) {
      return determineCompactionType(ci, txns, sd);
    } else {
      LOG.info("Going to initiate as user " + runAs);
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(runAs,
        UserGroupInformation.getLoginUser());
      return ugi.doAs(new PrivilegedExceptionAction<CompactionType>() {
        @Override
        public CompactionType run() throws Exception {
          return determineCompactionType(ci, txns, sd);
        }
      });
    }
  }

  private CompactionType determineCompactionType(CompactionInfo ci, ValidTxnList txns,
                                                 StorageDescriptor sd)
      throws IOException, InterruptedException {
    boolean noBase = false;
    Path location = new Path(sd.getLocation());
    FileSystem fs = location.getFileSystem(conf);
    AcidUtils.Directory dir = AcidUtils.getAcidState(location, conf, txns);
    Path base = dir.getBaseDirectory();
    long baseSize = 0;
    FileStatus stat = null;
    if (base != null) {
      stat = fs.getFileStatus(base);
      if (!stat.isDir()) {
        LOG.error("Was assuming base " + base.toString() + " is directory, but it's a file!");
        return null;
      }
      baseSize = sumDirSize(fs, base);
    }

    List<FileStatus> originals = dir.getOriginalFiles();
    for (FileStatus origStat : originals) {
      baseSize += origStat.getLen();
    }

    long deltaSize = 0;
    List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories();
    for (AcidUtils.ParsedDelta delta : deltas) {
      stat = fs.getFileStatus(delta.getPath());
      if (!stat.isDir()) {
        LOG.error("Was assuming delta " + delta.getPath().toString() + " is a directory, " +
            "but it's a file!");
        return null;
      }
      deltaSize += sumDirSize(fs, delta.getPath());
    }

    if (baseSize == 0 && deltaSize > 0) {
      noBase = true;
    } else {
      float deltaPctThreshold = HiveConf.getFloatVar(conf,
          HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_PCT_THRESHOLD);
      boolean bigEnough =   (float)deltaSize/(float)baseSize > deltaPctThreshold;
      if (LOG.isDebugEnabled()) {
        StringBuffer msg = new StringBuffer("delta size: ");
        msg.append(deltaSize);
        msg.append(" base size: ");
        msg.append(baseSize);
        msg.append(" threshold: ");
        msg.append(deltaPctThreshold);
        msg.append(" will major compact: ");
        msg.append(bigEnough);
        LOG.debug(msg);
      }
      if (bigEnough) return CompactionType.MAJOR;
    }

    int deltaNumThreshold = HiveConf.getIntVar(conf,
        HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD);
    boolean enough = deltas.size() > deltaNumThreshold;
    if (enough) {
      LOG.debug("Found " + deltas.size() + " delta files, threshold is " + deltaNumThreshold +
          (enough ? "" : "not") + " and no base, requesting " + (noBase ? "major" : "minor") +
          " compaction");
      // If there's no base file, do a major compaction
      return noBase ? CompactionType.MAJOR : CompactionType.MINOR;
    }
    return null;
  }

  private long sumDirSize(FileSystem fs, Path dir) throws IOException {
    long size = 0;
    FileStatus[] buckets = fs.listStatus(dir);
    for (int i = 0; i < buckets.length; i++) {
      size += buckets[i].getLen();
    }
    return size;
  }

  private void requestCompaction(CompactionInfo ci, String runAs, CompactionType type) throws MetaException {
    String s = "Requesting " + type.toString() + " compaction for " + ci.getFullPartitionName();
    LOG.info(s);
    CompactionRequest rqst = new CompactionRequest(ci.dbname, ci.tableName, type);
    if (ci.partName != null) rqst.setPartitionname(ci.partName);
    rqst.setRunas(runAs);
    txnHandler.compact(rqst);
  }
}
