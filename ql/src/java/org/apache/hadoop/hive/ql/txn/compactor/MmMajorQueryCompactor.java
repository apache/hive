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
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.ql.ddl.table.create.show.ShowCreateTableOperation;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.DirectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    removeFilesForMmTable(hiveConf, dir);

    // Then, actually do the compaction.
    if (!compactionInfo.isMajorCompaction()) {
      // Not supported for MM tables right now.
      LOG.info("Not compacting " + storageDescriptor.getLocation() + "; not a major compaction");
      return;
    }

    if (!Util.isEnoughToCompact(compactionInfo.isMajorCompaction(), dir, storageDescriptor)) {
      return;
    }

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

  // Remove the directories for aborted transactions only
  private void removeFilesForMmTable(HiveConf conf, AcidUtils.Directory dir) throws IOException {
    // For MM table, we only want to delete delta dirs for aborted txns.
    List<Path> filesToDelete = dir.getAbortedDirectories();
    if (filesToDelete.size() < 1) {
      return;
    }
    LOG.info("About to remove " + filesToDelete.size() + " aborted directories from " + dir);
    FileSystem fs = filesToDelete.get(0).getFileSystem(conf);
    for (Path dead : filesToDelete) {
      LOG.debug("Going to delete path " + dead.toString());
      fs.delete(dead, true);
    }
  }

  private List<String> getCreateQueries(String fullName, Table t, StorageDescriptor sd, String location) {
    StringBuilder query = new StringBuilder("create temporary table ").append(fullName).append("(");
    List<FieldSchema> cols = t.getSd().getCols();
    boolean isFirst = true;
    for (FieldSchema col : cols) {
      if (!isFirst) {
        query.append(", ");
      }
      isFirst = false;
      query.append("`").append(col.getName()).append("` ").append(col.getType());
    }
    query.append(") ");

    // Bucketing.
    List<String> buckCols = t.getSd().getBucketCols();
    if (buckCols.size() > 0) {
      query.append("CLUSTERED BY (").append(StringUtils.join(",", buckCols)).append(") ");
      List<Order> sortCols = t.getSd().getSortCols();
      if (sortCols.size() > 0) {
        query.append("SORTED BY (");
        isFirst = true;
        for (Order sortCol : sortCols) {
          if (!isFirst) {
            query.append(", ");
          }
          isFirst = false;
          query.append(sortCol.getCol()).append(" ").append(DirectionUtils.codeToText(sortCol.getOrder()));
        }
        query.append(") ");
      }
      query.append("INTO ").append(t.getSd().getNumBuckets()).append(" BUCKETS");
    }

    // Stored as directories. We don't care about the skew otherwise.
    if (t.getSd().isStoredAsSubDirectories()) {
      SkewedInfo skewedInfo = t.getSd().getSkewedInfo();
      if (skewedInfo != null && !skewedInfo.getSkewedColNames().isEmpty()) {
        query.append(" SKEWED BY (").append(StringUtils.join(", ", skewedInfo.getSkewedColNames())).append(") ON ");
        isFirst = true;
        for (List<String> colValues : skewedInfo.getSkewedColValues()) {
          if (!isFirst) {
            query.append(", ");
          }
          isFirst = false;
          query.append("('").append(StringUtils.join("','", colValues)).append("')");
        }
        query.append(") STORED AS DIRECTORIES");
      }
    }

    SerDeInfo serdeInfo = sd.getSerdeInfo();
    Map<String, String> serdeParams = serdeInfo.getParameters();
    query.append(" ROW FORMAT SERDE '").append(HiveStringUtils.escapeHiveCommand(serdeInfo.getSerializationLib()))
        .append("'");
    String sh = t.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE);
    assert sh == null; // Not supposed to be a compactable table.
    if (!serdeParams.isEmpty()) {
      ShowCreateTableOperation.appendSerdeParams(query, serdeParams);
    }
    query.append("STORED AS INPUTFORMAT '").append(HiveStringUtils.escapeHiveCommand(sd.getInputFormat()))
        .append("' OUTPUTFORMAT '").append(HiveStringUtils.escapeHiveCommand(sd.getOutputFormat()))
        .append("' LOCATION '").append(HiveStringUtils.escapeHiveCommand(location)).append("' TBLPROPERTIES (");
    // Exclude all standard table properties.
    Set<String> excludes = getHiveMetastoreConstants();
    excludes.addAll(StatsSetupConst.TABLE_PARAMS_STATS_KEYS);
    isFirst = true;
    for (Map.Entry<String, String> e : t.getParameters().entrySet()) {
      if (e.getValue() == null) {
        continue;
      }
      if (excludes.contains(e.getKey())) {
        continue;
      }
      if (!isFirst) {
        query.append(", ");
      }
      isFirst = false;
      query.append("'").append(e.getKey()).append("'='").append(HiveStringUtils.escapeHiveCommand(e.getValue()))
          .append("'");
    }
    if (!isFirst) {
      query.append(", ");
    }
    query.append("'transactional'='false')");
    return Lists.newArrayList(query.toString());

  }

  private List<String> getCompactionQueries(Table t, Partition p, String tmpName) {
    String fullName = t.getDbName() + "." + t.getTableName();
    // ideally we should make a special form of insert overwrite so that we:
    // 1) Could use fast merge path for ORC and RC.
    // 2) Didn't have to create a table.

    StringBuilder query = new StringBuilder("insert overwrite table " + tmpName + " ");
    StringBuilder filter = new StringBuilder();
    if (p != null) {
      filter = new StringBuilder(" where ");
      List<String> vals = p.getValues();
      List<FieldSchema> keys = t.getPartitionKeys();
      assert keys.size() == vals.size();
      for (int i = 0; i < keys.size(); ++i) {
        filter.append(i == 0 ? "`" : " and `").append(keys.get(i).getName()).append("`='").append(vals.get(i))
            .append("'");
      }
      query.append(" select ");
      // Use table descriptor for columns.
      List<FieldSchema> cols = t.getSd().getCols();
      for (int i = 0; i < cols.size(); ++i) {
        query.append(i == 0 ? "`" : ", `").append(cols.get(i).getName()).append("`");
      }
    } else {
      query.append("select *");
    }
    query.append(" from ").append(fullName).append(filter);
    return Lists.newArrayList(query.toString());
  }

  private List<String> getDropQueries(String tmpTableName) {
    return Lists.newArrayList("drop table if exists " + tmpTableName);
  }

  private static Set<String> getHiveMetastoreConstants() {
    Set<String> result = new HashSet<>();
    for (Field f : hive_metastoreConstants.class.getDeclaredFields()) {
      if (!Modifier.isStatic(f.getModifiers())) {
        continue;
      }
      if (!Modifier.isFinal(f.getModifiers())) {
        continue;
      }
      if (!String.class.equals(f.getType())) {
        continue;
      }
      f.setAccessible(true);
      try {
        result.add((String) f.get(null));
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    return result;
  }
}
