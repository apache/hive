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
package org.apache.hadoop.hive.ql.txn.compactor.handler;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidCleanerWriteIdList;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.metrics.AcidMetricService;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.txn.compactor.CleanupRequest;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorUtil;
import org.apache.hadoop.hive.ql.txn.compactor.FSRemover;
import org.apache.hadoop.hive.ql.txn.compactor.MetadataCache;
import org.apache.hive.common.util.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.collections.ListUtils.subtract;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_COMPACTOR_CLEANER_MAX_RETRY_ATTEMPTS;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_COMPACTOR_CLEANER_RETRY_RETENTION_TIME;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.getIntVar;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.getTimeVar;

/**
 * An abstract class which defines the list of utility methods for performing cleanup activities.
 */
public abstract class TaskHandler {

  private static final Logger LOG = LoggerFactory.getLogger(TaskHandler.class.getName());
  protected final TxnStore txnHandler;
  protected final HiveConf conf;
  protected final boolean metricsEnabled;
  protected final MetadataCache metadataCache;
  protected final FSRemover fsRemover;
  protected final long defaultRetention;

  TaskHandler(HiveConf conf, TxnStore txnHandler, MetadataCache metadataCache,
                         boolean metricsEnabled, FSRemover fsRemover) {
    this.conf = conf;
    this.txnHandler = txnHandler;
    this.metadataCache = metadataCache;
    this.metricsEnabled = metricsEnabled;
    this.fsRemover = fsRemover;
    this.defaultRetention = getTimeVar(conf, HIVE_COMPACTOR_CLEANER_RETRY_RETENTION_TIME, TimeUnit.MILLISECONDS);
  }

  public abstract List<Runnable> getTasks() throws MetaException;

  protected Table resolveTable(String dbName, String tableName) throws MetaException {
    return CompactorUtil.resolveTable(conf, dbName, tableName);
  }

  protected Partition resolvePartition(String dbName, String tableName, String partName) throws MetaException {
    return CompactorUtil.resolvePartition(conf, null, dbName, tableName, partName, CompactorUtil.METADATA_FETCH_MODE.LOCAL);
  }

  protected ValidReaderWriteIdList getValidCleanerWriteIdList(CompactionInfo info, ValidTxnList validTxnList)
          throws NoSuchTxnException, MetaException {
    List<String> tblNames = Collections.singletonList(AcidUtils.getFullTableName(info.dbname, info.tableName));
    GetValidWriteIdsRequest request = new GetValidWriteIdsRequest(tblNames);
    request.setValidTxnList(validTxnList.writeToString());
    GetValidWriteIdsResponse rsp = txnHandler.getValidWriteIds(request);
    // we could have no write IDs for a table if it was never written to but
    // since we are in the Cleaner phase of compactions, there must have
    // been some delta/base dirs
    assert rsp != null && rsp.getTblValidWriteIdsSize() == 1;

    return new ValidCleanerWriteIdList(
        TxnCommonUtils.createValidReaderWriteIdList(rsp.getTblValidWriteIds().get(0)));
  }

  protected boolean cleanAndVerifyObsoleteDirectories(CompactionInfo info, String location,
                                                      ValidReaderWriteIdList validWriteIdList, Table table) throws MetaException, IOException {
    Path path = new Path(location);
    FileSystem fs = path.getFileSystem(conf);

    // Collect all the files/dirs
    Map<Path, AcidUtils.HdfsDirSnapshot> dirSnapshots = AcidUtils.getHdfsDirSnapshotsForCleaner(fs, path);
    AcidDirectory dir = AcidUtils.getAcidState(fs, path, conf, validWriteIdList, Ref.from(false), false,
            dirSnapshots);
    boolean isDynPartAbort = CompactorUtil.isDynPartAbort(table, info.partName);

    List<Path> obsoleteDirs = CompactorUtil.getObsoleteDirs(dir, isDynPartAbort);
    if (isDynPartAbort || dir.hasUncompactedAborts()) {
      info.setWriteIds(dir.hasUncompactedAborts(), dir.getAbortedWriteIds());
    }

    List<Path> deleted = fsRemover.clean(new CleanupRequest.CleanupRequestBuilder().setLocation(location)
            .setDbName(info.dbname).setFullPartitionName(info.getFullPartitionName())
            .setRunAs(info.runAs).setObsoleteDirs(obsoleteDirs).setPurge(true)
            .build());

    if (!deleted.isEmpty()) {
      AcidMetricService.updateMetricsFromCleaner(info.dbname, info.tableName, info.partName, dir.getObsolete(), conf,
              txnHandler);
    }

    // Make sure there are no leftovers below the compacted watermark
    boolean success = false;
    conf.set(ValidTxnList.VALID_TXNS_KEY, new ValidReadTxnList().toString());
    dir = AcidUtils.getAcidState(fs, path, conf, new ValidCleanerWriteIdList(info.getFullTableName(), info.highestWriteId),
            Ref.from(false), false, dirSnapshots);

    List<Path> remained = subtract(CompactorUtil.getObsoleteDirs(dir, isDynPartAbort), deleted);
    if (!remained.isEmpty()) {
      LOG.warn("Remained {} obsolete directories from {}. {}",
              remained.size(), location, CompactorUtil.getDebugInfo(remained));
    } else {
      LOG.debug("All cleared below the watermark: {} from {}", info.highestWriteId, location);
      success = true;
    }

    return success;
  }

  protected void handleCleanerAttemptFailure(CompactionInfo info, String errorMessage) throws MetaException {
    int cleanAttempts = 0;
    info.errorMessage = errorMessage;
    if (info.isAbortedTxnCleanup()) {
      info.retryRetention = info.retryRetention > 0 ? info.retryRetention * 2 : defaultRetention;
      info.errorMessage = errorMessage;
      txnHandler.setCleanerRetryRetentionTimeOnError(info);
    } else {
      if (info.retryRetention > 0) {
        cleanAttempts = (int) (Math.log(info.retryRetention / defaultRetention) / Math.log(2)) + 1;
      }
      if (cleanAttempts >= getIntVar(conf, HIVE_COMPACTOR_CLEANER_MAX_RETRY_ATTEMPTS)) {
        //Mark it as failed if the max attempt threshold is reached.
        txnHandler.markFailed(info);
      } else {
        //Calculate retry retention time and update record.
        info.retryRetention = (long) Math.pow(2, cleanAttempts) * defaultRetention;
        txnHandler.setCleanerRetryRetentionTimeOnError(info);
      }
    }
  }
}
