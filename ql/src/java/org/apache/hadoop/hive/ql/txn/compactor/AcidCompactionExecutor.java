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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidCompactorWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.metrics.AcidMetricService;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.Ref;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class AcidCompactionExecutor extends CompactionExecutor {
  static final private String CLASS_NAME = AcidCompactionExecutor.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  
  final private Worker.CompactionTxn compactionTxn;
  private StorageDescriptor sd;
  private ValidCompactorWriteIdList tblValidWriteIds;
  private AcidDirectory dir;
  private CompactionInfo ci;

  public AcidCompactionExecutor(HiveConf conf, IMetaStoreClient msc, CompactorFactory compactorFactory,
                                Worker.CompactionTxn compactionTxn, boolean collectGenericStats, boolean collectMrStats) {
    super(conf, msc, compactorFactory, collectGenericStats, collectMrStats);
    this.compactionTxn = compactionTxn;
  }
  
  /**
   * Just AcidUtils.getAcidState, but with impersonation if needed.
   */
  private AcidDirectory getAcidStateForWorker(CompactionInfo ci, StorageDescriptor sd,
                                              ValidCompactorWriteIdList tblValidWriteIds) throws IOException, InterruptedException {
    if (CompactorUtil.runJobAsSelf(ci.runAs)) {
      return AcidUtils.getAcidState(null, new Path(sd.getLocation()), conf,
          tblValidWriteIds, Ref.from(false), true);
    }

    UserGroupInformation ugi = UserGroupInformation.createProxyUser(ci.runAs, UserGroupInformation.getLoginUser());
    try {
      return ugi.doAs((PrivilegedExceptionAction<AcidDirectory>) () ->
          AcidUtils.getAcidState(null, new Path(sd.getLocation()), conf, tblValidWriteIds,
              Ref.from(false), true));
    } finally {
      try {
        FileSystem.closeAllForUGI(ugi);
      } catch (IOException exception) {
        LOG.error("Could not clean up file-system handles for UGI: " + ugi + " for " + ci.getFullPartitionName(),
            exception);
      }
    }
  }

  public void cleanupResultDirs() {
    // result directory for compactor to write new files
    Path resultDir = QueryCompactor.Util.getCompactionResultDir(sd, tblValidWriteIds, conf,
        ci.type == CompactionType.MAJOR, false, false, dir);
    LOG.info("Deleting result directories created by the compactor:\n");
    try {
      FileSystem fs = resultDir.getFileSystem(conf);
      LOG.info(resultDir.toString());
      fs.delete(resultDir, true);

      if (ci.type == CompactionType.MINOR) {
        Path deleteDeltaDir = QueryCompactor.Util.getCompactionResultDir(sd, tblValidWriteIds, conf,
            false, true, false, dir);

        LOG.info(deleteDeltaDir.toString());
        fs.delete(deleteDeltaDir, true);
      }
    } catch (IOException ex) {
      LOG.error("Caught exception while cleaning result directories:", ex);
    }
  }
  
  public Boolean compact(Table table, CompactionInfo ci) throws InterruptedException, TException, IOException, HiveException {

    this.ci = ci;
    String fullTableName = TxnUtils.getFullTableName(table.getDbName(), table.getTableName());

    // Find the partition we will be working with, if there is one.
    Partition p;
    try {
      p = CompactorUtil.resolvePartition(conf, msc, ci.dbname, ci.tableName, ci.partName, 
          CompactorUtil.METADATA_FETCH_MODE.REMOTE);
      if (p == null && ci.partName != null) {
        ci.errorMessage = "Unable to find partition " + ci.getFullPartitionName() + ", assuming it was dropped and moving on.";
        LOG.warn(ci.errorMessage + " Compaction info: {}", ci);
        msc.markRefused(CompactionInfo.compactionInfoToStruct(ci));
        return false;
      }
    } catch (Exception e) {
      LOG.error("Unexpected error during resolving partition.", e);
      ci.errorMessage = e.getMessage();
      msc.markFailed(CompactionInfo.compactionInfoToStruct(ci));
      return false;
    }

    CompactorUtil.checkInterrupt(CLASS_NAME);

    // Find the appropriate storage descriptor
    sd =  CompactorUtil.resolveStorageDescriptor(table, p);

    if (isTableSorted(sd, ci)) {
      return false;  
    }

    if (ci.runAs == null) {
      ci.runAs = TxnUtils.findUserToRunAs(sd.getLocation(), table, conf);
    }

    CompactorUtil.checkInterrupt(CLASS_NAME);

    /**
     * we cannot have Worker use HiveTxnManager (which is on ThreadLocal) since
     * then the Driver would already have the an open txn but then this txn would have
     * multiple statements in it (for query based compactor) which is not supported (and since
     * this case some of the statements are DDL, even in the future will not be allowed in a
     * multi-stmt txn. {@link Driver#setCompactionWriteIds(ValidWriteIdList, long)} */
    compactionTxn.open(ci);

    ValidTxnList validTxnList = msc.getValidTxns(compactionTxn.getTxnId());
    //with this ValidWriteIdList is capped at whatever HWM validTxnList has
    tblValidWriteIds = TxnUtils.createValidCompactWriteIdList(msc.getValidWriteIds(
            Collections.singletonList(fullTableName), validTxnList.writeToString()).get(0));
    LOG.debug("ValidCompactWriteIdList: " + tblValidWriteIds.writeToString());
    conf.set(ValidTxnList.VALID_TXNS_KEY, validTxnList.writeToString());

    ci.highestWriteId = tblValidWriteIds.getHighWatermark();
    //this writes TXN_COMPONENTS to ensure that if compactorTxnId fails, we keep metadata about
    //it until after any data written by it are physically removed
    msc.updateCompactorState(CompactionInfo.compactionInfoToStruct(ci), compactionTxn.getTxnId());

    CompactorUtil.checkInterrupt(CLASS_NAME);

    // Don't start compaction or cleaning if not necessary
    if (isDynPartAbort(table, ci)) {
      msc.markCompacted(CompactionInfo.compactionInfoToStruct(ci));
      compactionTxn.wasSuccessful();
      return false;
    }
    dir = getAcidStateForWorker(ci, sd, tblValidWriteIds);
    if (!isEnoughToCompact(ci, dir, sd)) {
      if (needsCleaning(dir, sd)) {
        msc.markCompacted(CompactionInfo.compactionInfoToStruct(ci));
      } else {
        // do nothing
        ci.errorMessage = "None of the compaction thresholds met, compaction request is refused!";
        LOG.debug(ci.errorMessage + " Compaction info: {}", ci);
        msc.markRefused(CompactionInfo.compactionInfoToStruct(ci));
      }
      compactionTxn.wasSuccessful();
      return false;
    }
    if (!ci.isMajorCompaction() && !CompactorUtil.isMinorCompactionSupported(conf, table.getParameters(), dir)) {
      ci.errorMessage = "Query based Minor compaction is not possible for full acid tables having raw format " +
          "(non-acid) data in them.";
      LOG.error(ci.errorMessage + " Compaction info: {}", ci);
      try {
        msc.markRefused(CompactionInfo.compactionInfoToStruct(ci));
      } catch (Throwable tr) {
        LOG.error("Caught an exception while trying to mark compaction {} as failed: {}", ci, tr);
      }
      return false;
    }
    CompactorUtil.checkInterrupt(CLASS_NAME);

    try {
      failCompactionIfSetForTest();

      /*
      First try to run compaction via HiveQL queries.
      Compaction for MM tables happens here, or run compaction for Crud tables if query-based compaction is enabled.
      todo Find a more generic approach to collecting files in the same logical bucket to compact within the same
      task (currently we're using Tez split grouping).
      */
      CompactorPipeline compactorPipeline = compactorFactory.getCompactorPipeline(table, conf, ci, msc);
      computeStats = (compactorPipeline.isMRCompaction() && collectMrStats) || collectGenericStats;

      LOG.info("Starting " + ci.type.toString() + " compaction for " + ci.getFullPartitionName() + ", id:" +
              ci.id + " in " + compactionTxn + " with compute stats set to " + computeStats);

      CompactorContext compactorContext = new CompactorContext(conf, table, p, sd, tblValidWriteIds, ci, dir);
      compactorPipeline.execute(compactorContext);

      LOG.info("Completed " + ci.type.toString() + " compaction for " + ci.getFullPartitionName() + " in "
          + compactionTxn + ", marking as compacted.");
      msc.markCompacted(CompactionInfo.compactionInfoToStruct(ci));
      compactionTxn.wasSuccessful();

      AcidMetricService.updateMetricsFromWorker(ci.dbname, ci.tableName, ci.partName, ci.type,
          dir.getCurrentDirectories().size(), dir.getDeleteDeltas().size(), conf, msc);
    } catch (Throwable e) {
      computeStats = false;
      throw e;
    }

    return true;
  }

  /**
   * Determine if compaction can run in a specified directory.
   * @param ci  {@link CompactionInfo}
   * @param dir the delta directory
   * @param sd resolved storage descriptor
   * @return true, if compaction can run.
   */
  static boolean isEnoughToCompact(CompactionInfo ci, AcidDirectory dir, StorageDescriptor sd) {
    int deltaCount = dir.getCurrentDirectories().size();
    int origCount = dir.getOriginalFiles().size();

    StringBuilder deltaInfo = new StringBuilder().append(deltaCount);
    boolean isEnoughToCompact;

    if (ci.isRebalanceCompaction()) {
      //TODO: For now, we are allowing rebalance compaction regardless of the table state. Thresholds will be added later.
      return true;
    } else if (ci.isMajorCompaction()) {
      isEnoughToCompact =
          (origCount > 0 || deltaCount + (dir.getBaseDirectory() == null ? 0 : 1) > 1);

    } else {
      isEnoughToCompact = (deltaCount > 1);

      if (deltaCount == 2) {
        Map<String, Long> deltaByType = dir.getCurrentDirectories().stream().collect(Collectors
            .groupingBy(delta -> (delta
                    .isDeleteDelta() ? AcidUtils.DELETE_DELTA_PREFIX : AcidUtils.DELTA_PREFIX),
                Collectors.counting()));

        isEnoughToCompact = (deltaByType.size() != deltaCount);
        deltaInfo.append(" ").append(deltaByType);
      }
    }

    if (!isEnoughToCompact) {
      LOG.info("Not enough files in {} to compact; current base: {}, delta files: {}, originals: {}",
          sd.getLocation(), dir.getBaseDirectory(), deltaInfo, origCount);
    }
    return isEnoughToCompact;
  }

  /**
   * Check for obsolete directories, and return true if any exist and Cleaner should be
   * run. For example if we insert overwrite into a table with only deltas, a new base file with
   * the highest writeId is created so there will be no live delta directories, only obsolete
   * ones. Compaction is not needed, but the cleaner should still be run.
   *
   * @return true if cleaning is needed
   */
  public static boolean needsCleaning(AcidDirectory dir, StorageDescriptor sd) {
    int numObsoleteDirs = dir.getObsolete().size() + dir.getAbortedDirectories().size();
    boolean needsJustCleaning = numObsoleteDirs > 0;
    if (needsJustCleaning) {
      LOG.info("{} obsolete directories in {} found; marked for cleaning.", numObsoleteDirs,
          sd.getLocation());
    }
    return needsJustCleaning;
  }
}
