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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.ddl.table.create.show.ShowCreateTableOperation;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.util.DirectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class MmQueryCompactorUtils {

  private static final Logger LOG = LoggerFactory.getLogger(MmQueryCompactorUtils.class.getName());
  static final String DROP_IF_EXISTS = "drop table if exists ";

  private MmQueryCompactorUtils() {}

  /**
   * Creates a command to create a new table based on an example table (sourceTab).
   *
   * @param fullName of new table
   * @param sourceTab the table we are modeling the new table on
   * @param sd StorageDescriptor of the table or partition we are modeling the new table on
   * @param location of the new table
   * @param isPartitioned should the new table be partitioned
   * @param isExternal should the new table be external
   * @return query string creating the new table
   */
  static String getCreateQuery(String fullName, Table sourceTab, StorageDescriptor sd,
      String location, boolean isPartitioned, boolean isExternal) {
    StringBuilder query = new StringBuilder("create temporary ");
    if (isExternal) {
      query.append("external ");
    }
    query.append("table ").append(fullName).append("(");
    List<FieldSchema> cols = sourceTab.getSd().getCols();
    boolean isFirst = true;
    for (FieldSchema col : cols) {
      if (!isFirst) {
        query.append(", ");
      }
      isFirst = false;
      query.append("`").append(col.getName()).append("` ").append(col.getType());
    }
    query.append(") ");

    // Partitioning. Used for minor compaction.
    if (isPartitioned) {
      query.append(" PARTITIONED BY (`file_name` STRING) ");
    }

    // Bucketing.
    List<String> buckCols = sourceTab.getSd().getBucketCols();
    if (buckCols.size() > 0) {
      query.append("CLUSTERED BY (").append(StringUtils.join(",", buckCols)).append(") ");
      List<Order> sortCols = sourceTab.getSd().getSortCols();
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
      query.append("INTO ").append(sourceTab.getSd().getNumBuckets()).append(" BUCKETS");
    }

    // Stored as directories. We don't care about the skew otherwise.
    if (sourceTab.getSd().isStoredAsSubDirectories()) {
      SkewedInfo skewedInfo = sourceTab.getSd().getSkewedInfo();
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
    String sh = sourceTab.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE);
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
    for (Map.Entry<String, String> e : sourceTab.getParameters().entrySet()) {
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
    return query.toString();

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

  /**
   * Remove the delta directories of aborted transactions.
   */
  static void removeFilesForMmTable(HiveConf conf, AcidUtils.Directory dir) throws IOException {
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
}
