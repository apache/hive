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
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.MetastoreException;
import org.apache.hadoop.hive.metastore.utils.ObjectPair;
import org.apache.hadoop.hive.metastore.utils.RetryUtilities;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

/**
 * Msck repairs table metadata specifically related to partition information to be in-sync with directories in table
 * location.
 */
public class Msck {
  public static final Logger LOG = LoggerFactory.getLogger(Msck.class);
  public static final int separator = 9; // tabCode
  private static final int terminator = 10; // newLineCode
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
      // the only reason we are using new conf here is to override EXPRESSION_PROXY_CLASS
      Configuration metastoreConf = MetastoreConf.newMetastoreConf();
      metastoreConf.addResource(conf);
      metastoreConf.set(MetastoreConf.ConfVars.EXPRESSION_PROXY_CLASS.getVarname(),
        MsckPartitionExpressionProxy.class.getCanonicalName());
      setConf(metastoreConf);
      this.msc = new HiveMetaStoreClient(metastoreConf);
    }
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
    CheckResult result = new CheckResult();
    List<String> repairOutput = new ArrayList<>();
    String qualifiedTableName = null;
    boolean success = false;
    long txnId = -1;
    int ret = 0;
    try {
      Table table = getMsc().getTable(msckInfo.getCatalogName(), msckInfo.getDbName(), msckInfo.getTableName());
      qualifiedTableName = Warehouse.getCatalogQualifiedTableName(table);
      if (getConf().getBoolean(MetastoreConf.ConfVars.MSCK_REPAIR_ENABLE_PARTITION_RETENTION.getHiveName(), false)) {
        msckInfo.setPartitionExpirySeconds(PartitionManagementTask.getRetentionPeriodInSeconds(table));
        LOG.info("{} - Retention period ({}s) for partition is enabled for MSCK REPAIR..",
          qualifiedTableName, msckInfo.getPartitionExpirySeconds());
      }
      HiveMetaStoreChecker checker = new HiveMetaStoreChecker(getMsc(), getConf(), msckInfo.getPartitionExpirySeconds());
      // checkMetastore call will fill in result with partitions that are present in filesystem
      // and missing in metastore - accessed through getPartitionsNotInMs
      // And partitions that are not present in filesystem and metadata exists in metastore -
      // accessed through getPartitionNotOnFS
      checker.checkMetastore(msckInfo.getCatalogName(), msckInfo.getDbName(), msckInfo.getTableName(),
        msckInfo.getPartSpecs(), result);
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
        // Repair metadata in HMS
        long lockId;
        if (acquireLock && lockRequired && table.getParameters() != null &&
          MetaStoreUtils.isTransactionalTable(table.getParameters())) {
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
      }
      success = true;
    } catch (Exception e) {
      LOG.warn("Failed to run metacheck: ", e);
      success = false;
      ret = 1;
    } finally {
      if (msckInfo.getResFile() != null) {
        BufferedWriter resultOut = null;
        try {
          Path resFile = new Path(msckInfo.getResFile());
          FileSystem fs = resFile.getFileSystem(getConf());
          resultOut = new BufferedWriter(new OutputStreamWriter(fs.create(resFile)));

          boolean firstWritten = false;
          firstWritten |= writeMsckResult(result.getTablesNotInMs(),
            "Tables not in metastore:", resultOut, firstWritten);
          firstWritten |= writeMsckResult(result.getTablesNotOnFs(),
            "Tables missing on filesystem:", resultOut, firstWritten);
          firstWritten |= writeMsckResult(result.getPartitionsNotInMs(),
            "Partitions not in metastore:", resultOut, firstWritten);
          firstWritten |= writeMsckResult(result.getPartitionsNotOnFs(),
            "Partitions missing from filesystem:", resultOut, firstWritten);
          firstWritten |= writeMsckResult(result.getExpiredPartitions(),
            "Expired partitions (retention period: " + msckInfo.getPartitionExpirySeconds() + "s) :", resultOut, firstWritten);
          // sorting to stabilize qfile output (msck_repair_drop.q)
          Collections.sort(repairOutput);
          for (String rout : repairOutput) {
            if (firstWritten) {
              resultOut.write(terminator);
            } else {
              firstWritten = true;
            }
            resultOut.write(rout);
          }
        } catch (IOException e) {
          LOG.warn("Failed to save metacheck output: ", e);
          ret = 1;
        } finally {
          if (resultOut != null) {
            try {
              resultOut.close();
            } catch (IOException e) {
              LOG.warn("Failed to close output file: ", e);
              ret = 1;
            }
          }
        }
      }

      LOG.info("Tables not in metastore: {}", result.getTablesNotInMs());
      LOG.info("Tables missing on filesystem: {}", result.getTablesNotOnFs());
      LOG.info("Partitions not in metastore: {}", result.getPartitionsNotInMs());
      LOG.info("Partitions missing from filesystem: {}", result.getPartitionsNotOnFs());
      LOG.info("Expired partitions: {}", result.getExpiredPartitions());
      if (acquireLock && txnId > 0) {
          if (success) {
            try {
              LOG.info("txnId: {} succeeded. Committing..", txnId);
              getMsc().commitTxn(txnId);
            } catch (Exception e) {
              LOG.warn("Error while committing txnId: {} for table: {}", txnId, qualifiedTableName, e);
              ret = 1;
            }
          } else {
            try {
              LOG.info("txnId: {} failed. Aborting..", txnId);
              getMsc().abortTxns(Lists.newArrayList(txnId));
            } catch (Exception e) {
              LOG.warn("Error while aborting txnId: {} for table: {}", txnId, qualifiedTableName, e);
              ret = 1;
            }
          }
      }
      if (getMsc() != null) {
        getMsc().close();
        msc = null;
      }
    }

    return ret;
  }

  private LockRequest createLockRequest(final String dbName, final String tableName) throws TException {
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
              Path tablePath = MetaStoreUtils.getPath(table);
              if (tablePath == null) {
                continue;
              }
              Map<String, String> partSpec = Warehouse.makeSpecFromName(part.getPartitionName());
              Path location = new Path(tablePath, Warehouse.makePartPath(partSpec));
              Partition partition = MetaStoreUtils.createMetaPartitionObject(table, partSpec, location);
              partsToAdd.add(partition);
              lastBatch.add(part);
              addMsgs.add(String.format(addMsgFormat, part.getPartitionName()));
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
    StringBuilder suffixBuf = new StringBuilder();
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
    PartitionDropOptions dropOptions = new PartitionDropOptions().deleteData(deleteData).ifExists(true);
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
            List<ObjectPair<Integer, byte[]>> partExprs = getPartitionExpr(dropParts);
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

      private List<ObjectPair<Integer, byte[]>> getPartitionExpr(final List<String> parts) throws MetaException {
        List<ObjectPair<Integer, byte[]>> expr = new ArrayList<>(parts.size());
        for (int i = 0; i < parts.size(); i++) {
          String partName = parts.get(i);
          Map<String, String> partSpec = Warehouse.makeSpecFromName(partName);
          String partExpr = makePartExpr(partSpec);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Generated partExpr: {} for partName: {}", partExpr, partName);
          }
          expr.add(new ObjectPair<>(i, partExpr.getBytes(StandardCharsets.UTF_8)));
        }
        return expr;
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
