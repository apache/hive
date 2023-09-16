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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsFileStatusWithId;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A class to initiate compactions.  This will run in a separate thread.
 * It's critical that there exactly 1 of these in a given warehouse.
 */
public class Initiator extends CompactorThread {
  static final private String CLASS_NAME = Initiator.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  static final private String COMPACTORTHRESHOLD_PREFIX = "compactorthreshold.";

  private long checkInterval;

  @Override
  public void run() {
    // Make sure nothing escapes this run method and kills the metastore at large,
    // so wrap it in a big catch Throwable statement.
    try {
      recoverFailedCompactions(false);

      int abortedThreshold = HiveConf.getIntVar(conf,
          HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_THRESHOLD);

      // Make sure we run through the loop once before checking to stop as this makes testing
      // much easier.  The stop value is only for testing anyway and not used when called from
      // HiveMetaStore.
      do {
        long startedAt = -1;
        TxnStore.MutexAPI.LockHandle handle = null;

        // Wrap the inner parts of the loop in a catch throwable so that any errors in the loop
        // don't doom the entire thread.
        try {
          handle = txnHandler.getMutexAPI().acquireLock(TxnStore.MUTEX_KEY.Initiator.name());
          startedAt = System.currentTimeMillis();
          //todo: add method to only get current i.e. skip history - more efficient
          ShowCompactResponse currentCompactions = txnHandler.showCompact(new ShowCompactRequest());
          Set<CompactionInfo> potentials = txnHandler.findPotentialCompactions(abortedThreshold);
          LOG.debug("Found " + potentials.size() + " potential compactions, " +
              "checking to see if we should compact any of them");
          for (CompactionInfo ci : potentials) {
            LOG.info("Checking to see if we should compact " + ci.getFullPartitionName());
            try {
              Table t = resolveTable(ci);
              if (t == null) {
                // Most likely this means it's a temp table
                LOG.info("Can't find table " + ci.getFullTableName() + ", assuming it's a temp " +
                    "table or has been dropped and moving on.");
                continue;
              }

              // check if no compaction set for this table
              if (noAutoCompactSet(t)) {
                LOG.info("Table " + tableName(t) + " marked " + hive_metastoreConstants.TABLE_NO_AUTO_COMPACT + "=true so we will not compact it.");
                continue;
              }

              // Check to see if this is a table level request on a partitioned table.  If so,
              // then it's a dynamic partitioning case and we shouldn't check the table itself.
              if (t.getPartitionKeys() != null && t.getPartitionKeys().size() > 0 &&
                  ci.partName  == null) {
                LOG.debug("Skipping entry for " + ci.getFullTableName() + " as it is from dynamic" +
                    " partitioning");
                continue;
              }

              // Check if we already have initiated or are working on a compaction for this partition
              // or table.  If so, skip it.  If we are just waiting on cleaning we can still check,
              // as it may be time to compact again even though we haven't cleaned.
              //todo: this is not robust.  You can easily run Alter Table to start a compaction between
              //the time currentCompactions is generated and now
              if (lookForCurrentCompactions(currentCompactions, ci)) {
                LOG.debug("Found currently initiated or working compaction for " +
                    ci.getFullPartitionName() + " so we will not initiate another compaction");
                continue;
              }
              if(txnHandler.checkFailedCompactions(ci)) {
                LOG.warn("Will not initiate compaction for " + ci.getFullPartitionName() + " since last "
                  + HiveConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD + " attempts to compact it failed.");
                txnHandler.markFailed(ci);
                continue;
              }

              // Figure out who we should run the file operations as
              Partition p = resolvePartition(ci);
              if (p == null && ci.partName != null) {
                LOG.info("Can't find partition " + ci.getFullPartitionName() +
                    ", assuming it has been dropped and moving on.");
                continue;
              }

              // Compaction doesn't work under a transaction and hence pass null for validTxnList
              // The response will have one entry per table and hence we get only one ValidWriteIdList
              String fullTableName = TxnUtils.getFullTableName(t.getDbName(), t.getTableName());
              GetValidWriteIdsRequest rqst
                      = new GetValidWriteIdsRequest(Collections.singletonList(fullTableName), null);
              ValidWriteIdList tblValidWriteIds = TxnUtils.createValidCompactWriteIdList(
                      txnHandler.getValidWriteIds(rqst).getTblValidWriteIds().get(0));

              StorageDescriptor sd = resolveStorageDescriptor(t, p);
              String runAs = findUserToRunAs(sd.getLocation(), t);
              /*Future thought: checkForCompaction will check a lot of file metadata and may be expensive.
              * Long term we should consider having a thread pool here and running checkForCompactionS
              * in parallel*/
              CompactionType compactionNeeded
                      = checkForCompaction(ci, tblValidWriteIds, sd, t.getParameters(), runAs);
              if (compactionNeeded != null) requestCompaction(ci, runAs, compactionNeeded);
            } catch (Throwable t) {
              LOG.error("Caught exception while trying to determine if we should compact " +
                  ci + ".  Marking failed to avoid repeated failures, " +
                  "" + StringUtils.stringifyException(t));
              txnHandler.markFailed(ci);
            }
          }

          // Check for timed out remote workers.
          recoverFailedCompactions(true);

          // Clean anything from the txns table that has no components left in txn_components.
          txnHandler.cleanEmptyAbortedTxns();

          // Clean TXN_TO_WRITE_ID table for entries under min_uncommitted_txn referred by any open txns.
          txnHandler.cleanTxnToWriteIdTable();
        } catch (Throwable t) {
          LOG.error("Initiator loop caught unexpected exception this time through the loop: " +
              StringUtils.stringifyException(t));
        }
        finally {
          if(handle != null) {
            handle.releaseLocks();
          }
        }

        long elapsedTime = System.currentTimeMillis() - startedAt;
        if (elapsedTime >= checkInterval || stop.get())  continue;
        else Thread.sleep(checkInterval - elapsedTime);

      } while (!stop.get());
    } catch (Throwable t) {
      LOG.error("Caught an exception in the main loop of compactor initiator, exiting " +
          StringUtils.stringifyException(t));
    }
  }

  @Override
  public void init(AtomicBoolean stop, AtomicBoolean looped) throws MetaException {
    super.init(stop, looped);
    checkInterval =
        conf.getTimeVar(HiveConf.ConfVars.HIVE_COMPACTOR_CHECK_INTERVAL, TimeUnit.MILLISECONDS) ;
  }

  private void recoverFailedCompactions(boolean remoteOnly) throws MetaException {
    if (!remoteOnly) txnHandler.revokeFromLocalWorkers(Worker.hostname());
    txnHandler.revokeTimedoutWorkers(HiveConf.getTimeVar(conf,
        HiveConf.ConfVars.HIVE_COMPACTOR_WORKER_TIMEOUT, TimeUnit.MILLISECONDS));
  }

  // Figure out if there are any currently running compactions on the same table or partition.
  private boolean lookForCurrentCompactions(ShowCompactResponse compactions,
                                            CompactionInfo ci) {
    if (compactions.getCompacts() != null) {
      for (ShowCompactResponseElement e : compactions.getCompacts()) {
         if ((e.getState().equals(TxnStore.WORKING_RESPONSE) || e.getState().equals(TxnStore.INITIATED_RESPONSE)) &&
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
                                            final ValidWriteIdList writeIds,
                                            final StorageDescriptor sd,
                                            final Map<String, String> tblproperties,
                                            final String runAs)
      throws IOException, InterruptedException {
    // If it's marked as too many aborted, we already know we need to compact
    if (ci.tooManyAborts) {
      LOG.debug("Found too many aborted transactions for " + ci.getFullPartitionName() + ", " +
          "initiating major compaction");
      return CompactionType.MAJOR;
    }

    if (runJobAsSelf(runAs)) {
      return determineCompactionType(ci, writeIds, sd, tblproperties);
    } else {
      LOG.info("Going to initiate as user " + runAs + " for " + ci.getFullPartitionName());
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(runAs,
        UserGroupInformation.getLoginUser());
      CompactionType compactionType = ugi.doAs(new PrivilegedExceptionAction<CompactionType>() {
        @Override
        public CompactionType run() throws Exception {
          return determineCompactionType(ci, writeIds, sd, tblproperties);
        }
      });
      try {
        FileSystem.closeAllForUGI(ugi);
      } catch (IOException exception) {
        LOG.error("Could not clean up file-system handles for UGI: " + ugi + " for " +
            ci.getFullPartitionName(), exception);
      }
      return compactionType;
    }
  }

  private CompactionType determineCompactionType(CompactionInfo ci, ValidWriteIdList writeIds,
                                                 StorageDescriptor sd, Map<String, String> tblproperties)
      throws IOException, InterruptedException {

    boolean noBase = false;
    Path location = new Path(sd.getLocation());
    FileSystem fs = location.getFileSystem(conf);
    AcidUtils.Directory dir = AcidUtils.getAcidState(fs, location, conf, writeIds, false, false);
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

    List<HdfsFileStatusWithId> originals = dir.getOriginalFiles();
    for (HdfsFileStatusWithId origStat : originals) {
      baseSize += origStat.getFileStatus().getLen();
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
      String deltaPctProp = tblproperties.get(COMPACTORTHRESHOLD_PREFIX +
          HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_PCT_THRESHOLD);
      float deltaPctThreshold = deltaPctProp == null ?
          HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_PCT_THRESHOLD) :
          Float.parseFloat(deltaPctProp);
      boolean bigEnough =   (float)deltaSize/(float)baseSize > deltaPctThreshold;
      if (LOG.isDebugEnabled()) {
        StringBuilder msg = new StringBuilder("delta size: ");
        msg.append(deltaSize);
        msg.append(" base size: ");
        msg.append(baseSize);
        msg.append(" threshold: ");
        msg.append(deltaPctThreshold);
        msg.append(" will major compact: ");
        msg.append(bigEnough);
        LOG.debug(msg.toString());
      }
      if (bigEnough) return CompactionType.MAJOR;
    }

    String deltaNumProp = tblproperties.get(COMPACTORTHRESHOLD_PREFIX +
        HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD);
    int deltaNumThreshold = deltaNumProp == null ?
        HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD) :
        Integer.parseInt(deltaNumProp);
    boolean enough = deltas.size() > deltaNumThreshold;
    if (!enough) {
      return null;
    }
    if (AcidUtils.isInsertOnlyTable(tblproperties)) {
      LOG.debug("Requesting a major compaction for a MM table; found " + deltas.size()
          + " delta files, threshold is " + deltaNumThreshold);
      return CompactionType.MAJOR;
    }
    // TODO: this log statement looks wrong
    LOG.debug("Found " + deltas.size() + " delta files, threshold is " + deltaNumThreshold +
        (enough ? "" : "not") + " and no base, requesting " + (noBase ? "major" : "minor") +
        " compaction");
    // If there's no base file, do a major compaction
    return noBase ? CompactionType.MAJOR : CompactionType.MINOR;
  }

  private long sumDirSize(FileSystem fs, Path dir) throws IOException {
    long size = 0;
    FileStatus[] buckets = fs.listStatus(dir, FileUtils.HIDDEN_FILES_PATH_FILTER);
    for (int i = 0; i < buckets.length; i++) {
      size += buckets[i].getLen();
    }
    return size;
  }

  private void requestCompaction(CompactionInfo ci, String runAs, CompactionType type) throws MetaException {
    CompactionRequest rqst = new CompactionRequest(ci.dbname, ci.tableName, type);
    if (ci.partName != null) rqst.setPartitionname(ci.partName);
    rqst.setRunas(runAs);
    LOG.info("Requesting compaction: " + rqst);
    CompactionResponse resp = txnHandler.compact(rqst);
    if(resp.isAccepted()) {
      ci.id = resp.getId();
    }
  }

  // Because TABLE_NO_AUTO_COMPACT was originally assumed to be NO_AUTO_COMPACT and then was moved
  // to no_auto_compact, we need to check it in both cases.
  private boolean noAutoCompactSet(Table t) {
    String noAutoCompact =
        t.getParameters().get(hive_metastoreConstants.TABLE_NO_AUTO_COMPACT);
    if (noAutoCompact == null) {
      noAutoCompact =
          t.getParameters().get(hive_metastoreConstants.TABLE_NO_AUTO_COMPACT.toUpperCase());
    }
    return noAutoCompact != null && noAutoCompact.equalsIgnoreCase("true");
  }
}
