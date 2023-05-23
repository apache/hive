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
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
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
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.commons.collections.ListUtils.subtract;

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

  TaskHandler(HiveConf conf, TxnStore txnHandler, MetadataCache metadataCache,
                         boolean metricsEnabled, FSRemover fsRemover) {
    this.conf = conf;
    this.txnHandler = txnHandler;
    this.metadataCache = metadataCache;
    this.metricsEnabled = metricsEnabled;
    this.fsRemover = fsRemover;
  }

  public abstract List<Runnable> getTasks() throws MetaException;

  protected Table resolveTable(String dbName, String tableName) throws MetaException {
    return CompactorUtil.resolveTable(conf, dbName, tableName);
  }

  protected Partition resolvePartition(String dbName, String tableName, String partName) throws MetaException {
    if (partName != null) {
      List<Partition> parts;
      try {
        parts = CompactorUtil.getPartitionsByNames(conf, dbName, tableName, partName);
        if (parts == null || parts.isEmpty()) {
          // The partition got dropped before we went looking for it.
          return null;
        }
      } catch (Exception e) {
        LOG.error("Unable to find partition: {}.{}.{}", dbName, tableName, partName, e);
        throw e;
      }
      if (parts.size() != 1) {
        LOG.error("{}.{}.{} does not refer to a single partition. {}", dbName, tableName, partName,
                Arrays.toString(parts.toArray()));
        throw new MetaException(String.join("Too many partitions for : ", dbName, tableName, partName));
      }
      return parts.get(0);
    } else {
      return null;
    }
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

    return TxnCommonUtils.createValidReaderWriteIdList(rsp.getTblValidWriteIds().get(0));
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
    dir = AcidUtils.getAcidState(fs, path, conf, new ValidReaderWriteIdList(
                    info.getFullTableName(), new long[0], new BitSet(), info.highestWriteId, Long.MAX_VALUE),
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
}
