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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.metrics.AcidMetricService;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.Ref;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InitiatorBase extends MetaStoreCompactorThread {

  static final private String COMPACTOR_THRESHOLD_PREFIX = "compactorthreshold.";

  private List<CompactionResponse> initiateCompactionForMultiplePartitions(Table table,
      Map<String, Partition> partitions, CompactionRequest request) {
    List<CompactionResponse> compactionResponses = new ArrayList<>();
    partitions.entrySet().parallelStream().forEach(entry -> {
      try {
        StorageDescriptor sd = CompactorUtil.resolveStorageDescriptor(table, entry.getValue());
        String runAs = TxnUtils.findUserToRunAs(sd.getLocation(), table, conf);
        CompactionInfo ci =
            new CompactionInfo(table.getDbName(), table.getTableName(), entry.getKey(), request.getType());
        ci.initiatorId = request.getInitiatorId();
        ci.orderByClause = request.getOrderByClause();
        ci.initiatorVersion = request.getInitiatorVersion();
        if (request.getNumberOfBuckets() > 0) {
          ci.numberOfBuckets = request.getNumberOfBuckets();
        }
        ci.poolName = request.getPoolName();
        LOG.info(
            "Checking to see if we should compact partition " + entry.getKey() + " of table " + table.getDbName() + "."
                + table.getTableName());
        CollectionUtils.addIgnoreNull(compactionResponses,
            scheduleCompactionIfRequired(ci, table, entry.getValue(), runAs, false));
      } catch (IOException | InterruptedException | MetaException e) {
        LOG.error(
            "Error occurred while Checking if we should compact partition " + entry.getKey() + " of table " + table.getDbName() + "."
                + table.getTableName() + " Exception: " + e.getMessage());
        throw new RuntimeException(e);
      }
    });
    return compactionResponses;
  }

  public List<CompactionResponse> initiateCompactionForTable(CompactionRequest request, Table table, Map<String, Partition> partitions) throws Exception {
    ValidTxnList validTxnList = TxnCommonUtils.createValidReadTxnList(txnHandler.getOpenTxns(), 0);
    conf.set(ValidTxnList.VALID_TXNS_KEY, validTxnList.writeToString());

    if (request.getPartitionname()!= null || partitions.isEmpty()) {
      List<CompactionResponse> responses = new ArrayList<>();
      responses.add(txnHandler.compact(request));
      return responses;
    } else {
      return initiateCompactionForMultiplePartitions(table, partitions, request);
    }
  }

  @Override protected boolean isCacheEnabled() {
    return false;
  }

  private String getInitiatorId(long threadId) {
    return this.hostName + "-" + threadId;
  }

  private CompactionResponse requestCompaction(CompactionInfo ci, String runAs) throws MetaException {
    CompactionRequest compactionRequest = new CompactionRequest(ci.dbname, ci.tableName, ci.type);
    if (ci.partName != null)
      compactionRequest.setPartitionname(ci.partName);
    compactionRequest.setRunas(runAs);
    if (StringUtils.isEmpty(ci.initiatorId)) {
      compactionRequest.setInitiatorId(getInitiatorId(Thread.currentThread().getId()));
    } else {
      compactionRequest.setInitiatorId(ci.initiatorId);
    }
    compactionRequest.setInitiatorVersion(this.runtimeVersion);
    compactionRequest.setPoolName(ci.poolName);
    LOG.info("Requesting compaction: " + compactionRequest);
    CompactionResponse resp = txnHandler.compact(compactionRequest);
    if (resp.isAccepted()) {
      ci.id = resp.getId();
    }
    return resp;
  }

  private AcidDirectory getAcidDirectory(StorageDescriptor sd, ValidWriteIdList writeIds) throws IOException {
    Path location = new Path(sd.getLocation());
    FileSystem fs = location.getFileSystem(conf);
    return AcidUtils.getAcidState(fs, location, conf, writeIds, Ref.from(false), false);
  }

  private CompactionType determineCompactionType(CompactionInfo ci, AcidDirectory dir,
      Map<String, String> tblProperties, long baseSize, long deltaSize) {
    boolean noBase = false;
    List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories();
    if (baseSize == 0 && deltaSize > 0) {
      noBase = true;
    } else {
      String deltaPctProp =
          tblProperties.get(COMPACTOR_THRESHOLD_PREFIX + HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_PCT_THRESHOLD);
      float deltaPctThreshold = deltaPctProp == null ? HiveConf.getFloatVar(conf,
          HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_PCT_THRESHOLD) : Float.parseFloat(deltaPctProp);
      boolean bigEnough = (float) deltaSize / (float) baseSize > deltaPctThreshold;
      boolean multiBase = dir.getObsolete().stream().anyMatch(path -> path.getName().startsWith(AcidUtils.BASE_PREFIX));

      boolean initiateMajor = bigEnough || (deltaSize == 0 && multiBase);
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
      if (initiateMajor)
        return CompactionType.MAJOR;
    }

    String deltaNumProp =
        tblProperties.get(COMPACTOR_THRESHOLD_PREFIX + HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD);
    int deltaNumThreshold = deltaNumProp == null ? HiveConf.getIntVar(conf,
        HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD) : Integer.parseInt(deltaNumProp);
    boolean enough = deltas.size() > deltaNumThreshold;
    if (!enough) {
      LOG.debug("Not enough deltas to initiate compaction for table=" + ci.tableName + "partition=" + ci.partName
          + ". Found: " + deltas.size() + " deltas, threshold is " + deltaNumThreshold);
      return null;
    }
    // If there's no base file, do a major compaction
    LOG.debug("Found " + deltas.size() + " delta files, and " + (noBase ? "no" : "has") + " base," + "requesting "
        + (noBase ? "major" : "minor") + " compaction");

    return noBase || !CompactorUtil.isMinorCompactionSupported(conf, tblProperties, dir) ? CompactionType.MAJOR : CompactionType.MINOR;
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
    return dir.getFiles(fs, Ref.from(false)).stream().map(HadoopShims.HdfsFileStatusWithId::getFileStatus)
        .mapToLong(FileStatus::getLen).sum();
  }

  private CompactionType checkForCompaction(final CompactionInfo ci, final ValidWriteIdList writeIds,
      final StorageDescriptor sd, final Map<String, String> tblProperties, final String runAs)
      throws IOException, InterruptedException {
    // If it's marked as too many aborted, we already know we need to compact
    if (ci.tooManyAborts) {
      LOG.debug("Found too many aborted transactions for " + ci.getFullPartitionName() + ", "
          + "initiating major compaction");
      return CompactionType.MAJOR;
    }

    if (ci.hasOldAbort) {
      HiveConf.ConfVars oldAbortedTimeoutProp = HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_TIME_THRESHOLD;
      LOG.debug("Found an aborted transaction for " + ci.getFullPartitionName() + " with age older than threshold "
          + oldAbortedTimeoutProp + ": " + conf.getTimeVar(oldAbortedTimeoutProp, TimeUnit.HOURS) + " hours. "
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
    AcidMetricService.updateMetricsFromInitiator(ci.dbname, ci.tableName, ci.partName, conf, txnHandler, baseSize,
        deltaSizes, acidDirectory.getObsolete());

    if (CompactorUtil.runJobAsSelf(runAs)) {
      return determineCompactionType(ci, acidDirectory, tblProperties, baseSize, deltaSize);
    } else {
      LOG.info("Going to initiate as user " + runAs + " for " + ci.getFullPartitionName());
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(runAs, UserGroupInformation.getLoginUser());
      CompactionType compactionType;
      try {
        compactionType = ugi.doAs(
            (PrivilegedExceptionAction<CompactionType>) () -> determineCompactionType(ci, acidDirectory, tblProperties,
                baseSize, deltaSize));
      } finally {
        try {
          FileSystem.closeAllForUGI(ugi);
        } catch (IOException exception) {
          LOG.error("Could not clean up file-system handles for UGI: " + ugi + " for " + ci.getFullPartitionName(),
              exception);
        }
      }
      return compactionType;
    }
  }

  private ValidWriteIdList resolveValidWriteIds(Table t)
      throws NoSuchTxnException, MetaException {
    ValidTxnList validTxnList = new ValidReadTxnList(conf.get(ValidTxnList.VALID_TXNS_KEY));
    // The response will have one entry per table and hence we get only one ValidWriteIdList
    String fullTableName = TxnUtils.getFullTableName(t.getDbName(), t.getTableName());
    GetValidWriteIdsRequest validWriteIdsRequest = new GetValidWriteIdsRequest(Collections.singletonList(fullTableName));
    validWriteIdsRequest.setValidTxnList(validTxnList.writeToString());

    return TxnUtils.createValidCompactWriteIdList(txnHandler.getValidWriteIds(validWriteIdsRequest).getTblValidWriteIds().get(0));
  }

  protected CompactionResponse scheduleCompactionIfRequired(CompactionInfo ci, Table t,
      Partition p, String runAs, boolean metricsEnabled)
      throws MetaException {
    StorageDescriptor sd = CompactorUtil.resolveStorageDescriptor(t, p);
    try {
      ValidWriteIdList validWriteIds = resolveValidWriteIds(t);

      CompactorUtil.checkInterrupt(InitiatorBase.class.getName());

      CompactionType type = checkForCompaction(ci, validWriteIds, sd, t.getParameters(), runAs);
      if (type != null) {
        ci.type = type;
        return requestCompaction(ci, runAs);
      }
    } catch (InterruptedException e) {
      //Handle InterruptedException separately so the compactionInfo won't be marked as failed.
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
    return null;
  }

}
