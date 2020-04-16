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

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.common.util.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Class responsible to run query based major compaction on insert only tables.
 */
final class MmMajorQueryCompactor extends QueryCompactor {

  private static final Logger LOG = LoggerFactory.getLogger(MmMajorQueryCompactor.class.getName());

  @Override void runCompaction(HiveConf hiveConf, Table table, Partition partition, StorageDescriptor storageDescriptor,
      ValidWriteIdList writeIds, CompactionInfo compactionInfo) throws IOException {
    LOG.debug("Going to delete directories for aborted transactions for MM table " + table.getDbName() + "." + table
        .getTableName());
    AcidUtils.Directory dir = AcidUtils
        .getAcidState(null, new Path(storageDescriptor.getLocation()), hiveConf, writeIds, Ref.from(false), false,
            table.getParameters(), false);
    QueryCompactor.Util.removeFilesForMmTable(hiveConf, dir);

    String tmpLocation = Util.generateTmpPath(storageDescriptor);
    Path baseLocation = new Path(tmpLocation, "_base");

    // Set up the session for driver.
    HiveConf driverConf = new HiveConf(hiveConf);
    driverConf.set(HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT.varname, "column");

    // Note: we could skip creating the table and just add table type stuff directly to the
    //       "insert overwrite directory" command if there were no bucketing or list bucketing.
    String tmpPrefix = table.getDbName() + ".tmp_compactor_" + table.getTableName() + "_";
    String tmpTableName = tmpPrefix + System.currentTimeMillis();
    List<String> createTableQueries =
        getCreateQueries(tmpTableName, table, partition == null ? table.getSd() : partition.getSd(),
            baseLocation.toString());
    List<String> compactionQueries = getCompactionQueries(table, partition, tmpTableName);
    List<String> dropQueries = getDropQueries(tmpTableName);
    runCompactionQueries(driverConf, tmpTableName, storageDescriptor, writeIds, compactionInfo,
        createTableQueries, compactionQueries, dropQueries);
  }

  /**
   * Note: similar logic to the main committer; however, no ORC versions and stuff like that.
   * @param dest The final directory; basically a SD directory. Not the actual base/delta.
   * @param compactorTxnId txn that the compactor started
   */
  @Override
  protected void commitCompaction(String dest, String tmpTableName, HiveConf conf,
      ValidWriteIdList actualWriteIds, long compactorTxnId) throws IOException, HiveException {
    org.apache.hadoop.hive.ql.metadata.Table tempTable = Hive.get().getTable(tmpTableName);
    String from = tempTable.getSd().getLocation();
    Path fromPath = new Path(from), toPath = new Path(dest);
    FileSystem fs = fromPath.getFileSystem(conf);
    // Assume the high watermark can be used as maximum transaction ID.
    //todo: is that true?  can it be aborted? does it matter for compaction? probably OK since
    //getAcidState() doesn't check if X is valid in base_X_vY for compacted base dirs.
    long maxTxn = actualWriteIds.getHighWatermark();
    AcidOutputFormat.Options options =
        new AcidOutputFormat.Options(conf).writingBase(true).isCompressed(false).maximumWriteId(maxTxn).bucket(0)
            .statementId(-1).visibilityTxnId(compactorTxnId);
    Path newBaseDir = AcidUtils.createFilename(toPath, options).getParent();
    if (!fs.exists(fromPath)) {
      LOG.info(from + " not found.  Assuming 0 splits. Creating " + newBaseDir);
      fs.mkdirs(newBaseDir);
      return;
    }
    LOG.info("Moving contents of " + from + " to " + dest);
    fs.rename(fromPath, newBaseDir);
    fs.delete(fromPath, true);
  }

  private List<String> getCreateQueries(String tmpTableName, Table table,
      StorageDescriptor storageDescriptor, String baseLocation) {
    return Lists.newArrayList(
        new CompactionQueryBuilder(
            CompactionQueryBuilder.CompactionType.MAJOR_INSERT_ONLY,
            CompactionQueryBuilder.Operation.CREATE,
            tmpTableName)
            .setSourceTab(table)
            .setStorageDescriptor(storageDescriptor)
            .setLocation(baseLocation)
            .build()
    );
  }

  private List<String> getCompactionQueries(Table t, Partition p, String tmpName) {
    return Lists.newArrayList(
        new CompactionQueryBuilder(
            CompactionQueryBuilder.CompactionType.MAJOR_INSERT_ONLY,
            CompactionQueryBuilder.Operation.INSERT,
            tmpName)
            .setSourceTab(t)
            .setSourcePartition(p)
            .build()
    );
  }

  private List<String> getDropQueries(String tmpTableName) {
    return Lists.newArrayList(
        new CompactionQueryBuilder(
            CompactionQueryBuilder.CompactionType.MAJOR_INSERT_ONLY,
            CompactionQueryBuilder.Operation.DROP,
            tmpTableName).build());
  }
}
