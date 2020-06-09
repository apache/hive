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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidUtils;
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

    // Set up the session for driver.
    HiveConf driverConf = new HiveConf(hiveConf);
    driverConf.set(HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT.varname, "column");

    // Note: we could skip creating the table and just add table type stuff directly to the
    //       "insert overwrite directory" command if there were no bucketing or list bucketing.
    String tmpPrefix = table.getDbName() + ".tmp_compactor_" + table.getTableName() + "_";
    String tmpTableName = tmpPrefix + System.currentTimeMillis();
    Path resultBaseDir = QueryCompactor.Util.getCompactionResultDir(
        storageDescriptor, writeIds, driverConf, true, true, false);

    List<String> createTableQueries = getCreateQueries(tmpTableName, table, storageDescriptor,
        resultBaseDir.toString());
    List<String> compactionQueries = getCompactionQueries(table, partition, tmpTableName);
    List<String> dropQueries = getDropQueries(tmpTableName);
    runCompactionQueries(driverConf, tmpTableName, storageDescriptor, writeIds, compactionInfo,
        Lists.newArrayList(resultBaseDir), createTableQueries, compactionQueries, dropQueries);
  }

  /**
   * Note: similar logic to the main committer; however, no ORC versions and stuff like that.
   * @param dest The final directory; basically a SD directory. Not the actual base/delta.
   * @param compactorTxnId txn that the compactor started
   */
  @Override
  protected void commitCompaction(String dest, String tmpTableName, HiveConf conf,
      ValidWriteIdList actualWriteIds, long compactorTxnId) throws IOException, HiveException {
    Util.cleanupEmptyDir(conf, tmpTableName);
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
