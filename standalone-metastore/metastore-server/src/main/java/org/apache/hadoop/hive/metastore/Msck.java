/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetastoreException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnErrorMsg;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.RetryUtilities;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Msck repairs table metadata specifically related to partition information to be in-sync with directories in table
 * location.
 */
public class Msck {
  public static final Logger LOG = LoggerFactory.getLogger(Msck.class);
  public static final int separator = 9; // tabCode
  private static final int terminator = 10; // newLineCode
  private static final String TABLES_NOT_IN_METASTORE = "Tables not in metastore:";
  private static final String TABLES_MISSING_ON_FILESYSTEM = "Tables missing on filesystem:";
  private static final String PARTITIONS_NOT_IN_METASTORE = "Partitions not in metastore:";
  private static final String PARTITIONS_MISSING_FROM_FILESYSTEM = "Partitions missing from filesystem:";
  private static final String EXPIRED_PARTITIONS = "Expired partitions:";
  private static final String EXPIRED_PARTITIONS_RETENTION = "Expired partitions (retention period: %ss) :";
  private boolean acquireLock;
  private boolean deleteData;

  private Configuration conf;
  private IMetaStoreClient msc;


  public Msck(boolean acquireLock, boolean deleteData) {
    this.acquireLock = acquireLock;
    this.deleteData = deleteData;
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(final Configuration conf) {
    this.conf = conf;
  }

  public void init(Configuration conf) throws MetaException {
    if (msc == null) {
      setConf(conf);
      this.msc = new HiveMetaStoreClient(conf);
    }
  }

  public static Configuration getMsckConf(Configuration conf) {
    // the only reason we are using new conf here is to override EXPRESSION_PROXY_CLASS
    Configuration metastoreConf = MetastoreConf.newMetastoreConf(new Configuration(conf));
    metastoreConf.set(MetastoreConf.ConfVars.EXPRESSION_PROXY_CLASS.getVarname(),
            getProxyClass(metastoreConf));
    return metastoreConf;
  }

  public static String getProxyClass(Configuration metastoreConf) {
    return metastoreConf.get(MetastoreConf.ConfVars.EXPRESSION_PROXY_CLASS.getVarname(),
            MsckPartitionExpressionProxy.class.getCanonicalName());
  }

  public void updateExpressionProxy(String proxyClass) throws TException {
      msc.setMetaConf(MetastoreConf.ConfVars.EXPRESSION_PROXY_CLASS.getVarname(), proxyClass);
  }

  /**
   * MetastoreCheck, see if the data in the metastore matches what is on the
   * dfs. Current version checks for tables and partitions that are either
   * missing on disk on in the metastore.
   *
   * @param msckInfo Information about the tables and partitions we want to check for.
   * @return Returns 0 when execution succeeds and above 0 if it fails.
   */
  public int repair(MsckInfo msckInfo) {
    CheckResult result = null;
    List<String> repairOutput = new ArrayList<>();
    String qualifiedTableName = null;
    boolean success = false;
    long txnId = -1;
    long partitionExpirySeconds = -1;
    try {
      Table table = getMsc().getTable(msckInfo.getCatalogName(), msckInfo.getDbName(), msckInfo.getTableName());
      partitionExpirySeconds = PartitionManagementTask.getRetentionPeriodInSeconds(table);
      qualifiedTableName = Warehouse.getCatalogQualifiedTableName(table);
      HiveMetaStoreChecker checker = new HiveMetaStoreChecker(getMsc(), getConf(), partitionExpirySeconds);
      // checkMetastore call will fill in result with partitions that are present in filesystem
      // and missing in metastore - accessed through getPartitionsNotInMs
      // And partitions that are not present in filesystem and metadata exists in metastore -
      // accessed through getPartitionNotOnFS
      result = checker.checkMetastore(msckInfo.getCatalogName(), msckInfo.getDbName(), msckInfo.getTableName(),
        msckInfo.getFilterExp(), table);
      Set<CheckResult.PartitionResult> partsNotInMs = result.getPartitionsNotInMs();
      Set<CheckResult.PartitionResult> partsNotInFs = result.getPartitionsNotOnFs();
      Set<CheckResult.PartitionResult> expiredPartitions = result.getExpiredPartitions();
      int totalPartsToFix = partsNotInMs.size() + partsNotInFs.size() + expiredPartitions.size();
      // if nothing changed to partitions and if we are not repairing (add or drop) don't acquire for lock unnecessarily
      boolean lockRequired = totalPartsToFix > 0 &&
        msckInfo.isRepairPartitions() &&
        (msckInfo.isAddPartitions() || msckInfo.isDropPartitions());
      LOG.info("{} - #partsNotInMs: {} #partsNotInFs: {} #expiredPartitions: {} lockRequired: {} (R: {} A: {} D: {})",
        qualifiedTableName, partsNotInMs.size(), partsNotInFs.size(), expiredPartitions.size(), lockRequired,
        msckInfo.isRepairPartitions(), msckInfo.isAddPartitions(), msckInfo.isDropPartitions());

      if (msckInfo.isRepairPartitions()) {
        boolean transactionalTable = TxnUtils.isTransactionalTable(table);
        // Repair metadata in HMS
        long lockId;
        if (acquireLock && lockRequired && table.getParameters() != null && transactionalTable) {
          // Running MSCK from beeline/cli will make DDL task acquire X lock when repair is enabled, since we are directly
          // invoking msck.repair() without SQL statement, we need to do the same and acquire X lock (repair is default)
          LockRequest lockRequest = createLockRequest(msckInfo.getDbName(), msckInfo.getTableName());
          txnId = lockRequest.getTxnid();
          try {
            LockResponse res = getMsc().lock(lockRequest);
            if (res.getState() != LockState.ACQUIRED) {
              throw new MetastoreException("Unable to acquire lock(X) on " + qualifiedTableName);
            }
            lockId = res.getLockid();
          } catch (TException e) {
            throw new MetastoreException("Unable to acquire lock(X) on " + qualifiedTableName, e);
          }
          LOG.info("Acquired lock(X) on {}. LockId: {}", qualifiedTableName, lockId);
        }
        int maxRetries = MetastoreConf.getIntVar(getConf(), MetastoreConf.ConfVars.MSCK_REPAIR_BATCH_MAX_RETRIES);
        int decayingFactor = 2;

        if (msckInfo.isAddPartitions() && !partsNotInMs.isEmpty()) {
          // MSCK called to add missing paritions into metastore and there are
          // missing partitions.

          int batchSize = MetastoreConf.getIntVar(getConf(), MetastoreConf.ConfVars.MSCK_REPAIR_BATCH_SIZE);
          if (batchSize == 0) {
            //batching is not enabled. Try to add all the partitions in one call
            batchSize = partsNotInMs.size();
          }

          AbstractList<String> vals = null;
          String settingStr = MetastoreConf.getVar(getConf(), MetastoreConf.ConfVars.MSCK_PATH_VALIDATION);
          boolean doValidate = !("ignore".equals(settingStr));
          boolean doSkip = doValidate && "skip".equals(settingStr);
          // The default setting is "throw"; assume doValidate && !doSkip means throw.
          if (doValidate) {
            // Validate that we can add partition without escaping. Escaping was originally intended
            // to avoid creating invalid HDFS paths; however, if we escape the HDFS path (that we
            // deem invalid but HDFS actually supports - it is possible to create HDFS paths with
            // unprintable characters like ASCII 7), metastore will create another directory instead
            // of the one we are trying to "repair" here.
            Iterator<CheckResult.PartitionResult> iter = partsNotInMs.iterator();
            while (iter.hasNext()) {
              CheckResult.PartitionResult part = iter.next();
              try {
                vals = Warehouse.makeValsFromName(part.getPartitionName(), vals);
              } catch (MetaException ex) {
                throw new MetastoreException(ex);
              }
              for (String val : vals) {
                String escapedPath = FileUtils.escapePathName(val);
                assert escapedPath != null;
                if (escapedPath.equals(val)) {
                  continue;
                }
                String errorMsg = "Repair: Cannot add partition " + msckInfo.getTableName() + ':' +
                  part.getPartitionName() + " due to invalid characters in the name";
                if (doSkip) {
                  repairOutput.add(errorMsg);
                  iter.remove();
                } else {
                  throw new MetastoreException(errorMsg);
                }
              }
            }
          }
          if (transactionalTable) {
            if (txnId < 0) {
              // We need the txnId to check against even if we didn't do the locking
              txnId = getMsc().openTxn(getUserName());
            }
            // If we add new partitions to a transactional table, we have to repair the writeIds in the HMS
            validateAndAddMaxTxnIdAndWriteId(partsNotInMs, table.getDbName(), table.getTableName(), txnId);
          }
          try {
            createPartitionsInBatches(getMsc(), repairOutput, partsNotInMs, table, batchSize,
              decayingFactor, maxRetries);
          } catch (Exception e) {
            throw new MetastoreException(e);
          }
        }

        if (msckInfo.isDropPartitions() && (!partsNotInFs.isEmpty() || !expiredPartitions.isEmpty())) {
          // MSCK called to drop stale paritions from metastore and there are
          // stale partitions.

          int batchSize = MetastoreConf.getIntVar(getConf(), MetastoreConf.ConfVars.MSCK_REPAIR_BATCH_SIZE);
          if (batchSize == 0) {
            //batching is not enabled. Try to drop all the partitions in one call
            batchSize = partsNotInFs.size() + expiredPartitions.size();
          }

          try {
            dropPartitionsInBatches(getMsc(), repairOutput, partsNotInFs, expiredPartitions, table, batchSize,
              decayingFactor, maxRetries);
          } catch (Exception e) {
            throw new MetastoreException(e);
          }
        }
        if (transactionalTable && !MetaStoreServerUtils.isPartitioned(table) && result.getMaxWriteId() > 0) {
          if (txnId < 0) {
            // We need the txnId to check against even if we didn't do the locking
            txnId = getMsc().openTxn(getUserName());
          }

          validateAndAddMaxTxnIdAndWriteId(result.getMaxWriteId(), result.getMaxTxnId(), table.getDbName(),
              table.getTableName(), txnId);
        }
      }
      success = true;
    } catch (Exception e) {
      LOG.warn("Failed to run metacheck: ", e);
      success = false;
    } finally {
      if (result != null) {
        logResult(result);
        if (msckInfo.getResFile() != null) {
          // sorting to stabilize qfile output (msck_repair_drop.q)
          Collections.sort(repairOutput);
          success &= writeResultToFile(msckInfo, result, repairOutput, partitionExpirySeconds);
        }
      }

      if (txnId > 0) {
        success &= closeTxn(qualifiedTableName, success, txnId);
      }
      if (getMsc() != null) {
        getMsc().close();
        msc = null;
      }
    }
    return success ? 0 : 1;
  }

  private boolean closeTxn(String qualifiedTableName, boolean success, long txnId) {
    boolean ret = true;
    if (success) {
      try {
        LOG.info("txnId: {} succeeded. Committing..", txnId);
        getMsc().commitTxn(txnId);
      } catch (Exception e) {
        LOG.error("Error while committing txnId: {} for table: {}", txnId, qualifiedTableName, e);
        ret = false;
      }
    } else {
      LOG.info("txnId: {} failed. Aborting..", txnId);
      AbortTxnRequest abortTxnRequest = new AbortTxnRequest(txnId);
      abortTxnRequest.setErrorCode(TxnErrorMsg.ABORT_MSCK_TXN.getErrorCode());
      try {
        getMsc().rollbackTxn(abortTxnRequest);
      } catch (Exception e) {
        LOG.error("Error while aborting txnId: {} for table: {}", txnId, qualifiedTableName, e);
        ret = false;
      }
    }
    return ret;
  }

  private void logResult(CheckResult result) {
    StringBuilder sb = new StringBuilder();
    sb.append(TABLES_NOT_IN_METASTORE).append(" ").append(result.getTablesNotInMs()).append("\n")
        .append(TABLES_MISSING_ON_FILESYSTEM).append(" ").append(result.getTablesNotOnFs()).append("\n")
        .append(PARTITIONS_NOT_IN_METASTORE).append(" ").append(result.getPartitionsNotInMs()).append("\n")
        .append(PARTITIONS_MISSING_FROM_FILESYSTEM).append(" ").append(result.getPartitionsNotOnFs()).append("\n")
        .append(EXPIRED_PARTITIONS).append(" ").append(result.getExpiredPartitions()).append("\n");
    LOG.info(sb.toString());
  }

  private boolean writeResultToFile(MsckInfo msckInfo, CheckResult result, List<String> repairOutput,
      long partitionExpirySeconds) {
    boolean success = true;
    try {
      Path resFile = new Path(msckInfo.getResFile());
      FileSystem fs = resFile.getFileSystem(getConf());

      try (BufferedWriter resultOut = new BufferedWriter(new OutputStreamWriter(fs.create(resFile)))) {

        boolean firstWritten = false;
        firstWritten |= writeMsckResult(result.getTablesNotInMs(), TABLES_NOT_IN_METASTORE, resultOut, firstWritten);
        firstWritten |=
            writeMsckResult(result.getTablesNotOnFs(), TABLES_MISSING_ON_FILESYSTEM, resultOut, firstWritten);
        firstWritten |=
            writeMsckResult(result.getPartitionsNotInMs(), PARTITIONS_NOT_IN_METASTORE, resultOut, firstWritten);
        firstWritten |= writeMsckResult(result.getPartitionsNotOnFs(), PARTITIONS_MISSING_FROM_FILESYSTEM, resultOut,
            firstWritten);
        firstWritten |= writeMsckResult(result.getExpiredPartitions(),
            String.format(EXPIRED_PARTITIONS_RETENTION, partitionExpirySeconds), resultOut, firstWritten);

        for (String rout : repairOutput) {
          if (firstWritten) {
            resultOut.write(terminator);
          } else {
            firstWritten = true;
          }
          resultOut.write(rout);
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to save metacheck output: ", e);
      success = false;
    }
    return success;
  }

  /**
   * When we add new partitions to a transactional table, we have check the writeIds.
   * For every newly added partitions, we read the maximum writeId form the directory structure
   * and compare it to the maximum allocated writeId in the metastore.
   * If the metastore has never allocated any were are good, the use case would be initialize a table with
   * existing data. The HMS will be initialized with the maximum writeId. The system will handle every delta directory
   * as committed ones.
   * If the writeId is higher in the metastore we can still accept the data, the use case would be after some dataloss
   * some older data backup was used. The system would able to read the old data.
   * If however the writeId in the new partition is greater than the maximum allocated in the HMS
   * we must raise an error. The writedId in the HMS should be increased to match the writeIds in the data files,
   * but it would most likely cause a lot of problem since the transactional data would become inconsistent
   * between the HMS and the filesystem.
   * Further more we need to check for the visibilityTransactionIds written by the compaction.
   * If we have a higher visibilityId in the directory structure than the current transactionid we need to set
   * the transactionId sequence higher in the HMS so the next reads may read the content of the
   * compacted base/delta folders.
   * @param partsNotInMs partitions only in the FileSystem
   * @param dbName database name
   * @param tableName table name
   * @param txnId actual transactionId
   */
  private void validateAndAddMaxTxnIdAndWriteId(Set<CheckResult.PartitionResult> partsNotInMs, String dbName,
      String tableName, long txnId) throws TException {
    long maxWriteIdOnFilesystem =
        partsNotInMs.stream().map(CheckResult.PartitionResult::getMaxWriteId).max(Long::compareTo).orElse(0L);
    long maxVisibilityTxnId =
        partsNotInMs.stream().map(CheckResult.PartitionResult::getMaxTxnId).max(Long::compareTo).orElse(0L);
    validateAndAddMaxTxnIdAndWriteId(maxWriteIdOnFilesystem, maxVisibilityTxnId, dbName, tableName, txnId);
  }

  private void validateAndAddMaxTxnIdAndWriteId(long maxWriteIdOnFilesystem, long maxVisibilityTxnId, String dbName,
      String tableName, long txnId) throws TException {
    long maxAllocatedWriteId = getMsc().getMaxAllocatedWriteId(dbName, tableName);
    if (maxAllocatedWriteId > 0 && maxWriteIdOnFilesystem > maxAllocatedWriteId) {
      throw new MetaException(MessageFormat
          .format("The maximum writeId {} in table {} is greater than the maximum allocated in the metastore {}",
              maxWriteIdOnFilesystem, tableName, maxAllocatedWriteId));
    }
    if (maxAllocatedWriteId == 0 && maxWriteIdOnFilesystem > 0) {
      LOG.debug("Seeding table {} with writeId {}", tableName, maxWriteIdOnFilesystem);
      getMsc().seedWriteId(dbName, tableName, maxWriteIdOnFilesystem);
    }
    if (maxVisibilityTxnId > 0 && maxVisibilityTxnId > txnId) {
      LOG.debug("Seeding global txns sequence with  {}", maxVisibilityTxnId + 1);
      getMsc().seedTxnId(maxVisibilityTxnId + 1);
    }
  }

  private LockRequest createLockRequest(final String dbName, final String tableName) throws TException {
    String username = getUserName();
    long txnId = getMsc().openTxn(username);
    String agentInfo = Thread.currentThread().getName();
    LockRequestBuilder requestBuilder = new LockRequestBuilder(agentInfo);
    requestBuilder.setUser(username);
    requestBuilder.setTransactionId(txnId);

    LockComponentBuilder lockCompBuilder = new LockComponentBuilder()
      .setDbName(dbName)
      .setTableName(tableName)
      .setIsTransactional(true)
      .setExclusive()
      // WriteType is DDL_EXCLUSIVE for MSCK REPAIR so we need NO_TXN. Refer AcidUtils.makeLockComponents
      .setOperationType(DataOperationType.NO_TXN);
    requestBuilder.addLockComponent(lockCompBuilder.build());

    LOG.info("Created lock(X) request with info - user: {} txnId: {} agentInfo: {} dbName: {} tableName: {}",
      username, txnId, agentInfo, dbName, tableName);
    return requestBuilder.build();
  }

  private String getUserName() {
    UserGroupInformation loggedInUser = null;
    String username;
    try {
      loggedInUser = UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      LOG.warn("Unable to get logged in user via UGI. err: {}", e.getMessage());
    }
    if (loggedInUser == null) {
      username = System.getProperty("user.name");
    } else {
      username = loggedInUser.getShortUserName();
    }
    return username;
  }

  public IMetaStoreClient getMsc() {
    return msc;
  }

  @VisibleForTesting
  public void createPartitionsInBatches(final IMetaStoreClient metastoreClient, List<String> repairOutput,
    Set<CheckResult.PartitionResult> partsNotInMs, Table table, int batchSize, int decayingFactor, int maxRetries)
    throws Exception {
    String addMsgFormat = "Repair: Added partition to metastore "
      + table.getTableName() + ":%s";
    Set<CheckResult.PartitionResult> batchWork = new HashSet<>(partsNotInMs);
    new RetryUtilities.ExponentiallyDecayingBatchWork<Void>(batchSize, decayingFactor, maxRetries) {
      @Override
      public Void execute(int size) throws MetastoreException {
        try {
          while (!batchWork.isEmpty()) {
            List<Partition> partsToAdd = new ArrayList<>();
            //get the current batch size
            int currentBatchSize = size;
            //store the partitions temporarily until processed
            List<CheckResult.PartitionResult> lastBatch = new ArrayList<>(currentBatchSize);
            List<String> addMsgs = new ArrayList<>(currentBatchSize);
            //add the number of partitions given by the current batchsize
            for (CheckResult.PartitionResult part : batchWork) {
              if (currentBatchSize == 0) {
                break;
              }
              Path tablePath = MetaStoreServerUtils.getPath(table);
              if (tablePath == null) {
                continue;
              }
              Map<String, String> partSpec = Warehouse.makeSpecFromName(part.getPartitionName());
              Path location = part.getLocation(tablePath, partSpec);
              Partition partition = MetaStoreServerUtils.createMetaPartitionObject(table, partSpec, location);
              partition.setWriteId(table.getWriteId());
              partsToAdd.add(partition);
              lastBatch.add(part);
              String msg = String.format(addMsgFormat, part.getPartitionName());
              addMsgs.add(msg);
              LOG.debug(msg);
              currentBatchSize--;
            }
            metastoreClient.add_partitions(partsToAdd, true, false);
            // if last batch is successful remove it from partsNotInMs
            batchWork.removeAll(lastBatch);
            repairOutput.addAll(addMsgs);
          }
          return null;
        } catch (TException e) {
          throw new MetastoreException(e);
        }
      }
    }.run();
  }

  private static String makePartExpr(Map<String, String> spec)
    throws MetaException {
    StringBuilder suffixBuf = new StringBuilder("(");
    int i = 0;
    for (Map.Entry<String, String> e : spec.entrySet()) {
      if (e.getValue() == null || e.getValue().length() == 0) {
        throw new MetaException("Partition spec is incorrect. " + spec);
      }
      if (i > 0) {
        suffixBuf.append(" AND ");
      }
      suffixBuf.append(Warehouse.escapePathName(e.getKey()));
      suffixBuf.append('=');
      suffixBuf.append("'").append(Warehouse.escapePathName(e.getValue())).append("'");
      i++;
    }
    suffixBuf.append(")");
    return suffixBuf.toString();
  }

  // Drops partitions in batches.  partNotInFs is split into batches based on batchSize
  // and dropped.  The dropping will be through RetryUtilities which will retry when there is a
  // failure after reducing the batchSize by decayingFactor.  Retrying will cease when maxRetries
  // limit is reached or batchSize reduces to 0, whichever comes earlier.
  @VisibleForTesting
  public void dropPartitionsInBatches(final IMetaStoreClient metastoreClient, List<String> repairOutput,
    Set<CheckResult.PartitionResult> partsNotInFs, Set<CheckResult.PartitionResult> expiredPartitions,
    Table table, int batchSize, int decayingFactor, int maxRetries) throws Exception {
    String dropMsgFormat =
      "Repair: Dropped partition from metastore " + Warehouse.getCatalogQualifiedTableName(table) + ":%s";
    // Copy of partitions that will be split into batches
    Set<CheckResult.PartitionResult> batchWork = new HashSet<>(partsNotInFs);
    if (expiredPartitions != null && !expiredPartitions.isEmpty()) {
      batchWork.addAll(expiredPartitions);
    }
    PartitionDropOptions dropOptions = new PartitionDropOptions().deleteData(deleteData)
        .ifExists(true).returnResults(false);
    new RetryUtilities.ExponentiallyDecayingBatchWork<Void>(batchSize, decayingFactor, maxRetries) {
      @Override
      public Void execute(int size) throws MetastoreException {
        try {
          while (!batchWork.isEmpty()) {
            int currentBatchSize = size;

            // to store the partitions that are currently being processed
            List<CheckResult.PartitionResult> lastBatch = new ArrayList<>(currentBatchSize);

            // drop messages for the dropped partitions
            List<String> dropMsgs = new ArrayList<>(currentBatchSize);

            // Partitions to be dropped
            List<String> dropParts = new ArrayList<>(currentBatchSize);

            for (CheckResult.PartitionResult part : batchWork) {
              // This batch is full: break out of for loop to execute
              if (currentBatchSize == 0) {
                break;
              }

              dropParts.add(part.getPartitionName());

              // Add the part to lastBatch to track the parition being dropped
              lastBatch.add(part);

              // Update messages
              dropMsgs.add(String.format(dropMsgFormat, part.getPartitionName()));

              // Decrement batch size.  When this gets to 0, the batch will be executed
              currentBatchSize--;
            }

            // this call is deleting partitions that are already missing from filesystem
            // so 3rd parameter (deleteData) is set to false
            // msck is doing a clean up of hms.  if for some reason the partition is already
            // deleted, then it is good.  So, the last parameter ifexists is set to true
            List<Pair<Integer, byte[]>> partExprs = getPartitionExpr(dropParts);
            metastoreClient.dropPartitions(table.getCatName(), table.getDbName(), table.getTableName(), partExprs, dropOptions);

            // if last batch is successful remove it from partsNotInFs
            batchWork.removeAll(lastBatch);
            repairOutput.addAll(dropMsgs);
          }
          return null;
        } catch (TException e) {
          throw new MetastoreException(e);
        }
      }

      private List<Pair<Integer, byte[]>> getPartitionExpr(final List<String> parts) throws MetaException {
        StringBuilder exprBuilder = new StringBuilder();
        for (int i = 0; i < parts.size(); i++) {
          String partName = parts.get(i);
          Map<String, String> partSpec = Warehouse.makeSpecFromName(partName);
          String partExpr = makePartExpr(partSpec);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Generated partExpr: {} for partName: {}", partExpr, partName);
          }
          if (i > 0) {
            exprBuilder.append(" OR ");
          }
          exprBuilder.append(partExpr);
        }
        return Lists.newArrayList(Pair.of(parts.size(),
            exprBuilder.toString().getBytes(StandardCharsets.UTF_8)));
      }
    }.run();
  }

  /**
   * Write the result of msck to a writer.
   *
   * @param result The result we're going to write
   * @param msg    Message to write.
   * @param out    Writer to write to
   * @param wrote  if any previous call wrote data
   * @return true if something was written
   * @throws IOException In case the writing fails
   */
  private boolean writeMsckResult(Set<?> result, String msg,
    Writer out, boolean wrote) throws IOException {

    if (!result.isEmpty()) {
      if (wrote) {
        out.write(terminator);
      }

      out.write(msg);
      for (Object entry : result) {
        out.write(separator);
        out.write(entry.toString());
      }
      return true;
    }

    return false;
  }
}
