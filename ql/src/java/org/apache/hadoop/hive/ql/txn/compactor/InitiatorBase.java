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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.metrics.AcidMetricService;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.ql.ddl.table.storage.compact.AlterTableCompactDesc;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.Ref;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.ql.io.AcidUtils.compactionTypeStr2ThriftType;

public class InitiatorBase extends MetaStoreCompactorThread{

  static final private String COMPACTORTHRESHOLD_PREFIX = "compactorthreshold.";

  public InitiatorBase() {
  }

  public InitiatorBase(Table table, List<Partition> partitions, AlterTableCompactDesc desc, HiveConf hiveConf)
      throws Exception {
    setConf(hiveConf);
    AtomicBoolean flag = new AtomicBoolean();
    init(flag);
    initiateCompactionForTable(table,partitions,desc);
  }

  private void initiateCompactionForTable(Table table, List<Partition> partitions, AlterTableCompactDesc desc){
    partitions.parallelStream().forEach(partition -> {
      try {
        ValidTxnList validTxnList = TxnCommonUtils.createValidReadTxnList(
            txnHandler.getOpenTxns(), 0);
        conf.set(ValidTxnList.VALID_TXNS_KEY, validTxnList.writeToString());
        StorageDescriptor sd = resolveStorageDescriptor(table.getTTable(), partition.getTPartition());
        String runAs = TxnUtils.findUserToRunAs(sd.getLocation(), table.getTTable(), conf);
        CompactionInfo ci = new CompactionInfo(table.getDbName(), table.getTableName(), partition.getName(),
            compactionTypeStr2ThriftType(desc.getCompactionType()));
        ci.initiatorId = JavaUtils.hostname() + "-" + HiveMetaStoreClient.MANUALLY_INITIATED_COMPACTION;
        scheduleCompactionIfRequired(ci, table.getTTable(), partition.getTPartition(), desc.getPoolName(), runAs, false);
      } catch (IOException | InterruptedException | SemanticException | MetaException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  protected boolean isCacheEnabled() {
    return false;
  }

  private String getInitiatorId(long threadId) {
    StringBuilder name = new StringBuilder(this.hostName);
    name.append("-");
    name.append(threadId);
    return name.toString();
  }

  private void requestCompaction(CompactionInfo ci, String runAs) throws MetaException {
    CompactionRequest rqst = new CompactionRequest(ci.dbname, ci.tableName, ci.type);
    if (ci.partName != null) rqst.setPartitionname(ci.partName);
    rqst.setRunas(runAs);
    if (StringUtils.isEmpty(ci.initiatorId)) {
      rqst.setInitiatorId(getInitiatorId(Thread.currentThread().getId()));
    } else {
      rqst.setInitiatorId(ci.initiatorId);
    }
    rqst.setInitiatorVersion(this.runtimeVersion);
    rqst.setPoolName(ci.poolName);
    LOG.info("Requesting compaction: " + rqst);
    CompactionResponse resp = txnHandler.compact(rqst);
    if(resp.isAccepted()) {
      ci.id = resp.getId();
    }
  }

  private AcidDirectory getAcidDirectory(StorageDescriptor sd, ValidWriteIdList writeIds) throws IOException {
    Path location = new Path(sd.getLocation());
    FileSystem fs = location.getFileSystem(conf);
    return AcidUtils.getAcidState(fs, location, conf, writeIds, Ref.from(false), false);
  }

  private CompactionType determineCompactionType(CompactionInfo ci, AcidDirectory dir, Map<String,
      String> tblproperties, long baseSize, long deltaSize) {
    boolean noBase = false;
    List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories();
    if (baseSize == 0 && deltaSize > 0) {
      noBase = true;
    } else {
      String deltaPctProp = tblproperties.get(COMPACTORTHRESHOLD_PREFIX +
          HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_PCT_THRESHOLD);
      float deltaPctThreshold = deltaPctProp == null ?
          HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_PCT_THRESHOLD) :
          Float.parseFloat(deltaPctProp);
      boolean bigEnough =   (float)deltaSize/(float)baseSize > deltaPctThreshold;
      boolean multiBase = dir.getObsolete().stream()
          .anyMatch(path -> path.getName().startsWith(AcidUtils.BASE_PREFIX));

      boolean initiateMajor =  bigEnough || (deltaSize == 0  && multiBase);
      if (LOG.isDebugEnabled()) {
        StringBuilder msg = new StringBuilder("delta size: ");
        msg.append(deltaSize);
        msg.append(" base size: ");
        msg.append(baseSize);
        msg.append(" multiBase ");
        msg.append(multiBase);
        msg.append(" deltaSize ");
        msg.append(deltaSize);
        msg.append(" threshold: ");
        msg.append(deltaPctThreshold);
        msg.append(" delta/base ratio > ").append(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_PCT_THRESHOLD.varname)
            .append(": ");
        msg.append(bigEnough);
        msg.append(".");
        if (!initiateMajor) {
          msg.append("not");
        }
        msg.append(" initiating major compaction.");
        LOG.debug(msg.toString());
      }
      if (initiateMajor) return CompactionType.MAJOR;
    }

    String deltaNumProp = tblproperties.get(COMPACTORTHRESHOLD_PREFIX +
        HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD);
    int deltaNumThreshold = deltaNumProp == null ?
        HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD) :
        Integer.parseInt(deltaNumProp);
    boolean enough = deltas.size() > deltaNumThreshold;
    if (!enough) {
      LOG.debug("Not enough deltas to initiate compaction for table=" + ci.tableName + "partition=" + ci.partName
          + ". Found: " + deltas.size() + " deltas, threshold is " + deltaNumThreshold);
      return null;
    }
    // If there's no base file, do a major compaction
    LOG.debug("Found " + deltas.size() + " delta files, and " + (noBase ? "no" : "has") + " base," +
        "requesting " + (noBase ? "major" : "minor") + " compaction");

    return noBase || !isMinorCompactionSupported(tblproperties, dir) ?
        CompactionType.MAJOR : CompactionType.MINOR;
  }

  private long getBaseSize(AcidDirectory dir) throws IOException {
    long baseSize = 0;
    if (dir.getBase() != null) {
      baseSize = getDirSize(dir.getFs(), dir.getBase());
    } else {
      for (HadoopShims.HdfsFileStatusWithId origStat : dir.getOriginalFiles()) {
        baseSize += origStat.getFileStatus().getLen();
      }
    }
    return baseSize;
  }

  private long getDirSize(FileSystem fs, AcidUtils.ParsedDirectory dir) throws IOException {
    return dir.getFiles(fs, Ref.from(false)).stream()
        .map(HadoopShims.HdfsFileStatusWithId::getFileStatus)
        .mapToLong(FileStatus::getLen)
        .sum();
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

    if (ci.hasOldAbort) {
      HiveConf.ConfVars oldAbortedTimeoutProp =
          HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_TIME_THRESHOLD;
      LOG.debug("Found an aborted transaction for " + ci.getFullPartitionName()
          + " with age older than threshold " + oldAbortedTimeoutProp + ": " + conf
          .getTimeVar(oldAbortedTimeoutProp, TimeUnit.HOURS) + " hours. "
          + "Initiating minor compaction.");
      return CompactionType.MINOR;
    }
    AcidDirectory acidDirectory = getAcidDirectory(sd, writeIds);
    long baseSize = getBaseSize(acidDirectory);
    FileSystem fs = acidDirectory.getFs();
    Map<Path, Long> deltaSizes = new HashMap<>();
    for (AcidUtils.ParsedDelta delta : acidDirectory.getCurrentDirectories()) {
      deltaSizes.put(delta.getPath(), getDirSize(fs, delta));
    }
    long deltaSize = deltaSizes.values().stream().reduce(0L, Long::sum);
    AcidMetricService.updateMetricsFromInitiator(ci.dbname, ci.tableName, ci.partName, conf, txnHandler,
        baseSize, deltaSizes, acidDirectory.getObsolete());

    if (runJobAsSelf(runAs)) {
      return determineCompactionType(ci, acidDirectory, tblproperties, baseSize, deltaSize);
    } else {
      LOG.info("Going to initiate as user " + runAs + " for " + ci.getFullPartitionName());
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(runAs,
          UserGroupInformation.getLoginUser());
      CompactionType compactionType;
      try {
        compactionType = ugi.doAs(
            (PrivilegedExceptionAction<CompactionType>) () -> determineCompactionType(ci, acidDirectory, tblproperties, baseSize, deltaSize));
      } finally {
        try {
          FileSystem.closeAllForUGI(ugi);
        } catch (IOException exception) {
          LOG.error("Could not clean up file-system handles for UGI: " + ugi + " for " +
              ci.getFullPartitionName(), exception);
        }
      }
      return compactionType;
    }
  }

  private ValidWriteIdList resolveValidWriteIds(org.apache.hadoop.hive.metastore.api.Table t) throws NoSuchTxnException, MetaException {
    ValidTxnList validTxnList = new ValidReadTxnList(conf.get(ValidTxnList.VALID_TXNS_KEY));
    // The response will have one entry per table and hence we get only one ValidWriteIdList
    String fullTableName = TxnUtils.getFullTableName(t.getDbName(), t.getTableName());
    GetValidWriteIdsRequest rqst = new GetValidWriteIdsRequest(Collections.singletonList(fullTableName));
    rqst.setValidTxnList(validTxnList.writeToString());

    return TxnUtils.createValidCompactWriteIdList(
        txnHandler.getValidWriteIds(rqst).getTblValidWriteIds().get(0));
  }

  protected void scheduleCompactionIfRequired(CompactionInfo ci, org.apache.hadoop.hive.metastore.api.Table t, org.apache.hadoop.hive.metastore.api.Partition p, String poolName,
      String runAs, boolean metricsEnabled)
      throws MetaException {
    StorageDescriptor sd = resolveStorageDescriptor(t, p);
    try {
      ValidWriteIdList validWriteIds = resolveValidWriteIds(t);

      checkInterrupt();

      CompactionType type = checkForCompaction(ci, validWriteIds, sd, t.getParameters(), runAs);
      if (type != null) {
        ci.type = type;
        ci.poolName = poolName;
        requestCompaction(ci, runAs);
      }
    } catch (InterruptedException e) {
      //Handle InterruptedException separately so the compactioninfo won't be marked as failed.
      LOG.info("Initiator pool is being shut down, task received interruption.");
    } catch (Throwable ex) {
      String errorMessage = "Caught exception while trying to determine if we should compact " + ci + ". Marking "
          + "failed to avoid repeated failures, " + ex;
      LOG.error(errorMessage);
      ci.errorMessage = errorMessage;
      if (metricsEnabled) {
        Metrics.getOrCreateCounter(MetricsConstants.COMPACTION_INITIATOR_FAILURE_COUNTER).inc();
      }
      txnHandler.markFailed(ci);
    }
  }
}
