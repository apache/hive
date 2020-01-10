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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import java.io.IOException;
import java.util.List;

/**
 * Class responsible of running query based major compaction.
 */
final class MajorQueryCompactor extends QueryCompactor {

  @Override void runCompaction(HiveConf hiveConf, Table table, Partition partition, StorageDescriptor storageDescriptor,
      ValidWriteIdList writeIds, CompactionInfo compactionInfo) throws IOException {
    AcidUtils
        .setAcidOperationalProperties(hiveConf, true, AcidUtils.getAcidOperationalProperties(table.getParameters()));

    HiveConf conf = new HiveConf(hiveConf);
    // Set up the session for driver.
    conf.set(HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT.varname, "column");
    /*
     * For now, we will group splits on tez so that we end up with all bucket files,
     * with same bucket number in one map task.
     */
    conf.set(HiveConf.ConfVars.SPLIT_GROUPING_MODE.varname, "compactor");

    String tmpPrefix = table.getDbName() + "_tmp_compactor_" + table.getTableName() + "_";
    String tmpTableName = tmpPrefix + System.currentTimeMillis();
    List<String> createQueries = getCreateQueries(tmpTableName, table);
    List<String> compactionQueries = getCompactionQueries(table, partition, tmpTableName);
    List<String> dropQueries = getDropQueries(tmpTableName);
    runCompactionQueries(conf, tmpTableName, storageDescriptor, writeIds, compactionInfo, createQueries,
        compactionQueries, dropQueries);
  }

  /**
   * Move and rename bucket files from the temp table (tmpTableName), to the new base path under the source table/ptn.
   * Since the temp table is a non-transactional table, it has file names in the "original" format.
   * Also, due to split grouping in
   * {@link org.apache.hadoop.hive.ql.exec.tez.SplitGrouper#getCompactorSplitGroups(InputSplit[],
   * Configuration, boolean)}, we will end up with one file per bucket.
   */
  @Override protected void commitCompaction(String dest, String tmpTableName, HiveConf conf,
      ValidWriteIdList actualWriteIds, long compactorTxnId) throws IOException, HiveException {
    org.apache.hadoop.hive.ql.metadata.Table tempTable = Hive.get().getTable(tmpTableName);
    Util.moveContents(new Path(tempTable.getSd().getLocation()), new Path(dest), true, false, conf, actualWriteIds,
        compactorTxnId);
  }

  /**
   * Note on ordering of rows in the temp table:
   * We need each final bucket file sorted by original write id (ascending), bucket (ascending) and row id (ascending).
   * (current write id will be the same as original write id).
   * We will be achieving the ordering via a custom split grouper for compactor.
   * See {@link org.apache.hadoop.hive.conf.HiveConf.ConfVars#SPLIT_GROUPING_MODE} for the config description.
   * See {@link org.apache.hadoop.hive.ql.exec.tez.SplitGrouper#getCompactorSplitGroups(InputSplit[], Configuration)}
   *  for details on the mechanism.
   */
  private List<String> getCreateQueries(String fullName, Table t) {
    StringBuilder query = new StringBuilder("create temporary table ").append(fullName).append(" (");
    // Acid virtual columns
    query.append(
        "`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, `currentTransaction` bigint, "
            + "`row` struct<");
    List<FieldSchema> cols = t.getSd().getCols();
    boolean isFirst = true;
    // Actual columns
    for (FieldSchema col : cols) {
      if (!isFirst) {
        query.append(", ");
      }
      isFirst = false;
      query.append("`").append(col.getName()).append("` ").append(":").append(col.getType());
    }
    query.append(">)");
    query.append(" stored as orc");
    query.append(" tblproperties ('transactional'='false')");
    return Lists.newArrayList(query.toString());
  }

  private List<String> getCompactionQueries(Table t, Partition p, String tmpName) {
    String fullName = t.getDbName() + "." + t.getTableName();
    StringBuilder query = new StringBuilder("insert into table " + tmpName + " ");
    StringBuilder filter = new StringBuilder();
    if (p != null) {
      filter.append(" where ");
      List<String> vals = p.getValues();
      List<FieldSchema> keys = t.getPartitionKeys();
      assert keys.size() == vals.size();
      for (int i = 0; i < keys.size(); ++i) {
        filter.append(i == 0 ? "`" : " and `").append(keys.get(i).getName()).append("`='").append(vals.get(i))
            .append("'");
      }
    }
    query.append(" select validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId), ROW__ID.writeId, "
        + "ROW__ID.bucketId, ROW__ID.rowId, ROW__ID.writeId, NAMED_STRUCT(");
    List<FieldSchema> cols = t.getSd().getCols();
    for (int i = 0; i < cols.size(); ++i) {
      query.append(i == 0 ? "'" : ", '").append(cols.get(i).getName()).append("', ").append(cols.get(i).getName());
    }
    query.append(") from ").append(fullName).append(filter);
    return Lists.newArrayList(query.toString());
  }

  private List<String> getDropQueries(String tmpTableName) {
    return Lists.newArrayList("drop table if exists " + tmpTableName);
  }
}
