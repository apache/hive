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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import org.apache.hadoop.hive.metastore.metrics.AcidMetricService;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.StringableMap;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.hadoop.hive.metastore.HMSHandler.getMSForConf;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

public class CompactorUtil {
  private static final Logger LOG = LoggerFactory.getLogger(CompactorUtil.class);
  public static final String COMPACTOR = "compactor";
  private static final String COMPACTOR_THRESHOLD_PREFIX = "compactorthreshold.";

  /**
   * List of accepted properties for defining the compactor's job queue.
   *
   * The order is important and defines which property has precedence over the other if multiple properties are defined
   * at the same time.
   */
  private static final List<String> QUEUE_PROPERTIES = Arrays.asList(
      "compactor." + HiveConf.ConfVars.COMPACTOR_JOB_QUEUE.varname,
      "compactor.mapreduce.job.queuename",
      "compactor.mapred.job.queue.name"
  );

  public interface ThrowingRunnable<E extends Exception> {
    void run() throws E;

    static Runnable unchecked(ThrowingRunnable<?> r) {
      return () -> {
        try {
          r.run();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };
    }
  }

  public static ExecutorService createExecutorWithThreadFactory(int parallelism, String threadNameFormat) {
    return new ForkJoinPool(parallelism,
      pool -> {
        ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
        worker.setName(format(threadNameFormat, worker.getPoolIndex()));
        return worker;
      },
      null, false);
  }

  /**
   * Get the compactor queue name if it's defined.
   * @param conf global hive conf
   * @param ci compaction info object
   * @param table instance of table
   * @return name of the queue
   */
  static String getCompactorJobQueueName(HiveConf conf, CompactionInfo ci, Table table) {
    // Get queue name from the ci. This is passed through
    // ALTER TABLE table_name COMPACT 'major' WITH OVERWRITE TBLPROPERTIES('compactor.hive.compactor.job.queue'='some_queue')
    List<Function<String, String>> propertyGetters = new ArrayList<>(2);
    if (ci.properties != null) {
      StringableMap ciProperties = new StringableMap(ci.properties);
      propertyGetters.add(ciProperties::get);
    }
    if (table.getParameters() != null) {
      propertyGetters.add(table.getParameters()::get);
    }

    for (Function<String, String> getter : propertyGetters) {
      for (String p : QUEUE_PROPERTIES) {
        String queueName = getter.apply(p);
        if (queueName != null && !queueName.isEmpty()) {
          return queueName;
        }
      }
    }
    return conf.getVar(HiveConf.ConfVars.COMPACTOR_JOB_QUEUE);
  }

  public static StorageDescriptor resolveStorageDescriptor(Table t, Partition p) {
    return (p == null) ? t.getSd() : p.getSd();
  }

  public static StorageDescriptor resolveStorageDescriptor(Table t) {
    return resolveStorageDescriptor(t, null);
  }

  public static boolean isDynPartAbort(Table t, String partName) {
    return Optional.ofNullable(t).map(Table::getPartitionKeys).filter(pk -> !pk.isEmpty()).isPresent()
            && partName == null;
  }

  public static List<Partition> getPartitionsByNames(HiveConf conf, String dbName, String tableName, String partName) throws MetaException {
    try {
      return getMSForConf(conf).getPartitionsByNames(getDefaultCatalog(conf), dbName, tableName,
              Collections.singletonList(partName));
    } catch (Exception e) {
      LOG.error("Unable to get partitions by name = {}.{}.{}", dbName, tableName, partName);
      throw new MetaException(e.toString());
    }
  }

  public static Database resolveDatabase(HiveConf conf, String dbName) throws MetaException, NoSuchObjectException {
    try {
      return getMSForConf(conf).getDatabase(MetaStoreUtils.getDefaultCatalog(conf), dbName);
    } catch (NoSuchObjectException e) {
      LOG.error("Unable to find database {}, {}", dbName, e.getMessage());
      throw e;
    }
  }

  public static Table resolveTable(HiveConf conf, String dbName, String tableName) throws MetaException {
    try {
      return getMSForConf(conf).getTable(MetaStoreUtils.getDefaultCatalog(conf), dbName, tableName);
    } catch (MetaException e) {
      LOG.error("Unable to find table {}.{}, {}", dbName, tableName, e.getMessage());
      throw e;
    }
  }

  public static String getDebugInfo(List<Path> paths) {
    return "[" + paths.stream().map(Path::getName).collect(Collectors.joining(",")) + ']';
  }

  /**
   * Determine whether to run this job as the current user or whether we need a doAs to switch
   * users.
   * @param owner of the directory we will be working in, as determined by
   * {@link org.apache.hadoop.hive.metastore.txn.TxnUtils#findUserToRunAs(String, Table, Configuration)}
   * @return true if the job should run as the current user, false if a doAs is needed.
   */
  public static boolean runJobAsSelf(String owner) {
    return (owner.equals(System.getProperty("user.name")));
  }

  public static List<Path> getObsoleteDirs(AcidDirectory dir, boolean isDynPartAbort) {
    List<Path> obsoleteDirs = dir.getObsolete();
    /*
     * add anything in 'dir'  that only has data from aborted transactions - no one should be
     * trying to read anything in that dir (except getAcidState() that only reads the name of
     * this dir itself)
     * So this may run ahead of {@link CompactionInfo#highestWriteId} but it's ok (suppose there
     * are no active txns when cleaner runs).  The key is to not delete metadata about aborted
     * txns with write IDs > {@link CompactionInfo#highestWriteId}.
     * See {@link TxnStore#markCleaned(CompactionInfo)}
     */
    obsoleteDirs.addAll(dir.getAbortedDirectories());
    if (isDynPartAbort) {
      // In the event of an aborted DP operation, we should only consider the aborted directories for cleanup.
      // Including obsolete directories for partitioned tables can result in data loss.
      obsoleteDirs = dir.getAbortedDirectories();
    }
    return obsoleteDirs;
  }

  public static Partition resolvePartition(HiveConf conf, IMetaStoreClient msc, String dbName, String tableName, 
      String partName, METADATA_FETCH_MODE fetchMode) throws MetaException {
    if (partName != null) {
      List<Partition> parts = null;
      try {

        switch (fetchMode) {
          case LOCAL: parts = CompactorUtil.getPartitionsByNames(conf, dbName, tableName, partName);
                      break;
          case REMOTE: parts = RemoteCompactorUtil.getPartitionsByNames(msc, dbName, tableName, partName);
            break;
        }

        if (parts == null || parts.size() == 0) {
          // The partition got dropped before we went looking for it.
          return null;
        }
      } catch (Exception e) {
        LOG.error("Unable to find partition " + getFullPartitionName(dbName, tableName, partName), e);
        throw e;
      }
      if (parts.size() != 1) {
        LOG.error(getFullPartitionName(dbName, tableName, partName) + " does not refer to a single partition. " +
            Arrays.toString(parts.toArray()));
        throw new MetaException("Too many partitions for : " + getFullPartitionName(dbName, tableName, partName));
      }
      return parts.get(0);
    } else {
      return null;
    }
  }

  public static String getFullPartitionName(String dbName, String tableName, String partName) {
    StringBuilder buf = new StringBuilder();
    buf.append(dbName);
    buf.append('.');
    buf.append(tableName);
    if (partName != null) {
      buf.append('.');
      buf.append(partName);
    }
    return buf.toString();
  }
  
  public enum METADATA_FETCH_MODE {
    LOCAL,
    REMOTE
  }

  public static void checkInterrupt(String callerClassName) throws InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException(callerClassName + " execution is interrupted.");
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
  public static boolean isMinorCompactionSupported(HiveConf conf, Map<String, String> tblproperties, AcidDirectory dir) {
    //Query based Minor compaction is not possible for full acid tables having raw format (non-acid) data in them.
    return AcidUtils.isInsertOnlyTable(tblproperties) || !conf.getBoolVar(HiveConf.ConfVars.COMPACTOR_CRUD_QUERY_BASED)
        || !(dir.getOriginalFiles().size() > 0 || dir.getCurrentDirectories().stream().anyMatch(AcidUtils.ParsedDelta::isRawFormat));
  }

  public static LockRequest createLockRequest(HiveConf conf, CompactionInfo ci, long txnId, LockType lockType, DataOperationType opType) {
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

  private static CompactionResponse requestCompaction(CompactionInfo ci, String runAs, String hostname,
      TxnStore txnHandler) throws MetaException {
    CompactionRequest compactionRequest = new CompactionRequest(ci.dbname, ci.tableName, ci.type);
    if (ci.partName != null)
      compactionRequest.setPartitionname(ci.partName);
    compactionRequest.setRunas(runAs);
    if (StringUtils.isEmpty(ci.initiatorId)) {
      compactionRequest.setInitiatorId(hostname + "-" + Thread.currentThread().getId());
    } else {
      compactionRequest.setInitiatorId(ci.initiatorId);
    }
    compactionRequest.setInitiatorVersion(ci.initiatorVersion);
    compactionRequest.setPoolName(ci.poolName);
    LOG.info("Requesting compaction: " + compactionRequest);
    CompactionResponse resp = txnHandler.compact(compactionRequest);
    if (resp.isAccepted()) {
      ci.id = resp.getId();
    }
    return resp;
  }

  private static CompactionType determineCompactionType(CompactionInfo ci, AcidDirectory dir,
      Map<String, String> tblProperties, long baseSize, long deltaSize, HiveConf conf) {
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
      LOG.debug(
          "Not enough deltas to initiate compaction for table=" + ci.tableName + "partition=" + ci.partName
              + ". Found: " + deltas.size() + " deltas, threshold is " + deltaNumThreshold);
      return null;
    }
    // If there's no base file, do a major compaction
    LOG.debug("Found " + deltas.size() + " delta files, and " + (noBase ? "no" : "has") + " base," + "requesting "
        + (noBase ? "major" : "minor") + " compaction");

    return noBase || !isMinorCompactionSupported(conf, tblProperties,
        dir) ? CompactionType.MAJOR : CompactionType.MINOR;
  }

  private static long getBaseSize(AcidDirectory dir) throws IOException {
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

  private static long getDirSize(FileSystem fs, AcidUtils.ParsedDirectory dir) throws IOException {
    return dir.getFiles(fs, Ref.from(false)).stream().map(HadoopShims.HdfsFileStatusWithId::getFileStatus)
        .mapToLong(FileStatus::getLen).sum();
  }

  private static CompactionType checkForCompaction(final CompactionInfo ci, final ValidWriteIdList writeIds,
      final StorageDescriptor sd, final Map<String, String> tblProperties, final String runAs, TxnStore txnHandler,
      HiveConf conf) throws IOException, InterruptedException {
    // If it's marked as too many aborted, we already know we need to compact
    if (ci.tooManyAborts) {
      LOG.debug("Found too many aborted transactions for "
          + ci.getFullPartitionName() + ", " + "initiating major compaction");
      return CompactionType.MAJOR;
    }

    if (ci.hasOldAbort) {
      HiveConf.ConfVars oldAbortedTimeoutProp = HiveConf.ConfVars.HIVE_COMPACTOR_ABORTEDTXN_TIME_THRESHOLD;
      LOG.debug(
          "Found an aborted transaction for " + ci.getFullPartitionName() + " with age older than threshold "
              + oldAbortedTimeoutProp + ": " + conf.getTimeVar(oldAbortedTimeoutProp, TimeUnit.HOURS) + " hours. "
              + "Initiating minor compaction.");
      return CompactionType.MINOR;
    }
    AcidDirectory acidDirectory = AcidUtils.getAcidState(sd, writeIds, conf);
    long baseSize = getBaseSize(acidDirectory);
    FileSystem fs = acidDirectory.getFs();
    Map<Path, Long> deltaSizes = new HashMap<>();
    for (AcidUtils.ParsedDelta delta : acidDirectory.getCurrentDirectories()) {
      deltaSizes.put(delta.getPath(), getDirSize(fs, delta));
    }
    long deltaSize = deltaSizes.values().stream().reduce(0L, Long::sum);
    AcidMetricService.updateMetricsFromInitiator(ci.dbname, ci.tableName, ci.partName, conf, txnHandler, baseSize,
        deltaSizes, acidDirectory.getObsolete());

    if (runJobAsSelf(runAs)) {
      return determineCompactionType(ci, acidDirectory, tblProperties, baseSize, deltaSize, conf);
    } else {
      LOG.info("Going to initiate as user " + runAs + " for " + ci.getFullPartitionName());
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(runAs, UserGroupInformation.getLoginUser());
      CompactionType compactionType;
      try {
        compactionType = ugi.doAs(
            (PrivilegedExceptionAction<CompactionType>) () -> determineCompactionType(ci, acidDirectory, tblProperties,
                baseSize, deltaSize, conf));
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

  private static ValidWriteIdList resolveValidWriteIds(Table t, TxnStore txnHandler, HiveConf conf)
      throws NoSuchTxnException, MetaException {
    ValidTxnList validTxnList = new ValidReadTxnList(conf.get(ValidTxnList.VALID_TXNS_KEY));
    // The response will have one entry per table and hence we get only one ValidWriteIdList
    String fullTableName = TxnUtils.getFullTableName(t.getDbName(), t.getTableName());
    GetValidWriteIdsRequest validWriteIdsRequest =
        new GetValidWriteIdsRequest(Collections.singletonList(fullTableName));
    validWriteIdsRequest.setValidTxnList(validTxnList.writeToString());

    return TxnUtils.createValidCompactWriteIdList(
        txnHandler.getValidWriteIds(validWriteIdsRequest).getTblValidWriteIds().get(0));
  }

  public static CompactionResponse scheduleCompactionIfRequired(CompactionInfo ci, Table t, Partition p, String runAs,
      boolean metricsEnabled, String hostName, TxnStore txnHandler, HiveConf conf) throws MetaException {
    StorageDescriptor sd = resolveStorageDescriptor(t, p);
    try {
      ValidWriteIdList validWriteIds = resolveValidWriteIds(t, txnHandler, conf);

      checkInterrupt(Initiator.class.getName());

      CompactionType type = checkForCompaction(ci, validWriteIds, sd, t.getParameters(), runAs, txnHandler, conf);
      if (type != null) {
        ci.type = type;
        return requestCompaction(ci, runAs, hostName, txnHandler);
      }
    } catch (InterruptedException e) {
      //Handle InterruptedException separately so the compactionInfo won't be marked as failed.
      LOG.info("Initiator pool is being shut down, task received interruption.");
    } catch (Throwable ex) {
      String errorMessage = "Caught exception while trying to determine if we should compact " + ci
              + ". Marking " + "failed to avoid repeated failures, " + ex;
      LOG.error(errorMessage);
      ci.errorMessage = errorMessage;
      if (metricsEnabled) {
        Metrics.getOrCreateCounter(MetricsConstants.COMPACTION_INITIATOR_FAILURE_COUNTER).inc();
      }
      txnHandler.markFailed(ci);
    }
    return null;
  }

  public static CompactionResponse initiateCompactionForPartition(Table table, Partition partition,
      CompactionRequest compactionRequest, String hostName, TxnStore txnHandler, HiveConf inputConf) throws MetaException {
    ValidTxnList validTxnList = TxnCommonUtils.createValidReadTxnList(txnHandler.getOpenTxns(), 0);
    HiveConf conf = new HiveConf(inputConf);
    conf.set(ValidTxnList.VALID_TXNS_KEY, validTxnList.writeToString());
    CompactionResponse compactionResponse;
    CompactionInfo compactionInfo =
        new CompactionInfo(table.getDbName(), table.getTableName(), compactionRequest.getPartitionname(),
            compactionRequest.getType());
    compactionInfo.initiatorId = compactionRequest.getInitiatorId();
    compactionInfo.orderByClause = compactionRequest.getOrderByClause();
    compactionInfo.initiatorVersion = compactionRequest.getInitiatorVersion();
    if (compactionRequest.getNumberOfBuckets() > 0) {
      compactionInfo.numberOfBuckets = compactionRequest.getNumberOfBuckets();
    }
    compactionInfo.poolName = compactionRequest.getPoolName();
    try {
      StorageDescriptor sd = resolveStorageDescriptor(table, partition);
      String runAs = TxnUtils.findUserToRunAs(sd.getLocation(), table, conf);
      LOG.info("Checking to see if we should compact partition {} of table {}.{}", compactionInfo.partName,
          table.getDbName(), table.getTableName());
      compactionResponse =
          scheduleCompactionIfRequired(compactionInfo, table, partition, runAs, false, hostName,
              txnHandler, conf);
    } catch (IOException | InterruptedException | MetaException e) {
      LOG.error("Error occurred while Checking if we should compact partition {} of table {}.{} Exception: {}",
          compactionInfo.partName, table.getDbName(), table.getTableName(), e.getMessage());
      throw new RuntimeException(e);
    }
    return compactionResponse;
  }
}
