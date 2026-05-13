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

package org.apache.hadoop.hive.metastore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MTableColumnStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

/**
 * Statistics management task responsible for periodic auto-deletion of table and partition column
 * statistics based on a configured retention interval.
 *
 * <p>When {@code metastore.column.statistics.auto.deletion} is enabled, this task scans
 * {@code TAB_COL_STATS} and {@code PART_COL_STATS} for rows whose {@code lastAnalyzed} timestamp
 * is older than {@code metastore.column.statistics.retention.period}, and deletes them.
 * Individual tables may opt out by setting the table property
 * {@value #STATISTICS_AUTO_DELETION_EXCLUDE_TBLPROPERTY} to {@code "true"}.
 */
public class StatisticsManagementTask extends ObjectStore implements MetastoreTaskThread {

  private static final Logger LOG = LoggerFactory.getLogger(StatisticsManagementTask.class);

  /**
   * Table property key that, when set to {@code "true"} on a table, excludes it from automatic
   * statistics deletion regardless of the global retention setting.
   */
  public static final String STATISTICS_AUTO_DELETION_EXCLUDE_TBLPROPERTY =
      "statistics.auto.deletion.exclude";

  /** Separator used when building composite map keys; chosen to be safe in HMS identifiers. */
  private static final String KEY_SEP = "\0";

  private static final Lock LOCK = new ReentrantLock();

  @Override
  public long runFrequency(TimeUnit unit) {
    // when frequency=0, the auto deletion task is not being run
    if (MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.COLUMN_STATISTICS_RETENTION_PERIOD,
        TimeUnit.MILLISECONDS) <= 0) {
      return 0;
    }
    return MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.COLUMN_STATISTICS_MANAGEMENT_TASK_FREQUENCY, unit);
  }

  @Override
  public void setConf(Configuration configuration) {
    this.conf = new Configuration(configuration);
    super.setConf(this.conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void run() {
    LOG.debug("Auto statistics deletion started. Cleaning up table/partition column statistics"
        + " over the retention period.");
    long retentionMillis = MetastoreConf.getTimeVar(
        conf, MetastoreConf.ConfVars.COLUMN_STATISTICS_RETENTION_PERIOD, TimeUnit.MILLISECONDS);
    if (retentionMillis <= 0) {
      LOG.info("Statistics auto deletion is set to off currently.");
      return;
    }
    if (!LOCK.tryLock()) {
      return;
    }
    try {
      long now = System.currentTimeMillis();
      long lastAnalyzedThreshold = (now - retentionMillis) / 1000;
      List<Object[]> expiredTblRows;
      List<Object[]> expiredPartRows;
      boolean committed = false;
      openTransaction();
      try {
        PersistenceManager pm = getPersistenceManager();
        expiredTblRows = collectExpiredTableColStats(pm, lastAnalyzedThreshold);
        expiredPartRows = collectExpiredPartitionColStats(pm, lastAnalyzedThreshold);
        committed = commitTransaction();
      } finally {
        if (!committed) {
          rollbackTransaction();
        }
      }
      deleteExpiredTableColStats(expiredTblRows);
      deleteExpiredPartitionColStats(expiredPartRows);
    } catch (Exception e) {
      LOG.error("Error during statistics auto deletion", e);
    } finally {
      LOCK.unlock();
    }
  }

  /**
   * Queries {@code TAB_COL_STATS} for rows whose {@code lastAnalyzed} is older than the given
   * threshold. Results are copied into an {@link ArrayList} so they remain accessible after
   * the enclosing JDO transaction is committed.
   *
   * @param pm                    the JDO persistence manager to use for the query
   * @param lastAnalyzedThreshold epoch seconds; rows with lastAnalyzed below this value are expired
   * @return list of projected rows: [catName, dbName, tblName, colName]
   * @throws Exception if the JDO query fails
   */
  private List<Object[]> collectExpiredTableColStats(PersistenceManager pm,
                                                     long lastAnalyzedThreshold) throws Exception {
    try (Query tblQuery = pm.newQuery(MTableColumnStatistics.class)) {
      tblQuery.setFilter("lastAnalyzed < threshold && engine == \"hive\"");
      tblQuery.declareParameters("long threshold");
      tblQuery.setRange(0, 1000);
      tblQuery.setResult(
          "table.database.catalogName, "
              + "table.database.name, "
              + "table.tableName, "
              + "colName, "
              + "table.parameters.get(\"" + STATISTICS_AUTO_DELETION_EXCLUDE_TBLPROPERTY + "\")");
      @SuppressWarnings("unchecked")
      List<Object[]> rows = (List<Object[]>) tblQuery.execute(lastAnalyzedThreshold);
      return new ArrayList<>(rows);
    }
  }

  /**
   * Groups expired table-level column stats by {@code (catName, dbName, tblName)}, then deletes
   * all expired columns for each table in a single {@link RawStore} call and a dedicated
   * transaction. This avoids duplicate listener events for the same table.
   * Tables whose exclude property is set to {@code "true"} are skipped entirely.
   *
   * @param expiredTblRows projected rows from {@link #collectExpiredTableColStats}
   * @throws Exception if a delete operation fails
   */
  private void deleteExpiredTableColStats(List<Object[]> expiredTblRows) throws Exception {
    // key: catName + SEP + dbName + SEP + tblName, value: list of expired colNames
    Map<String, List<String>> tblToColsMap = new LinkedHashMap<>();
    // keep a parallel map to reconstruct the key parts when issuing deletes
    Map<String, String[]> keyToCoords = new LinkedHashMap<>();

    for (Object[] row : expiredTblRows) {
      String catName   = (String) row[0];
      String dbName    = (String) row[1];
      String tblName   = (String) row[2];
      String colName   = (String) row[3];
      String excludeVal = (String) row[4];
      if (Boolean.parseBoolean(excludeVal)) {
        LOG.info("Skipping auto deletion of table stats for {}.{} due to exclude property.",
            dbName, tblName);
        continue;
      }
      String key = catName + KEY_SEP + dbName + KEY_SEP + tblName;
      tblToColsMap.computeIfAbsent(key, k -> new ArrayList<>()).add(colName);
      keyToCoords.putIfAbsent(key, new String[]{catName, dbName, tblName});
    }

    // one transaction per table, delete all expired columns in a single call
    for (Map.Entry<String, List<String>> entry : tblToColsMap.entrySet()) {
      String[] coords = keyToCoords.get(entry.getKey());
      boolean committed = false;
      openTransaction();
      try {
        deleteTableColumnStatistics(coords[0], coords[1], coords[2], entry.getValue(), "hive");
        committed = commitTransaction();
      } finally {
        if (!committed) {
          rollbackTransaction();
        }
      }
    }
  }

  /**
   * Queries {@code PART_COL_STATS} for rows whose {@code lastAnalyzed} is older than the given
   * threshold. Results are copied into an {@link ArrayList} so they remain accessible after
   * the enclosing JDO transaction is committed.
   *
   * @param pm                    the JDO persistence manager to use for the query
   * @param lastAnalyzedThreshold epoch seconds; rows with lastAnalyzed below this value are expired
   * @return list of projected rows: [catName, dbName, tblName, partName, colName]
   * @throws Exception if the JDO query fails
   */
  private List<Object[]> collectExpiredPartitionColStats(PersistenceManager pm,
                                                         long lastAnalyzedThreshold) throws Exception {
    try (Query partQuery = pm.newQuery(MPartitionColumnStatistics.class)) {
      partQuery.setFilter("lastAnalyzed < threshold && engine == \"hive\"");
      partQuery.declareParameters("long threshold");
      partQuery.setRange(0, 1000);
      partQuery.setResult(
          "partition.table.database.catalogName, "
              + "partition.table.database.name, "
              + "partition.table.tableName, "
              + "partition.partitionName, "
              + "colName, "
              + "partition.table.parameters.get(\"" + STATISTICS_AUTO_DELETION_EXCLUDE_TBLPROPERTY + "\")");
      @SuppressWarnings("unchecked")
      List<Object[]> rows = (List<Object[]>) partQuery.execute(lastAnalyzedThreshold);
      return new ArrayList<>(rows);
    }
  }

  /**
   * Groups expired partition-level column stats by {@code (catName, dbName, tblName, partName)},
   * then deletes all expired columns for each partition in a single {@link RawStore} call and a
   * dedicated transaction. This avoids duplicate listener events for the same partition.
   * Tables whose exclude property is set to {@code "true"} are skipped entirely.
   *
   * @param expiredPartRows projected rows from {@link #collectExpiredPartitionColStats}
   * @throws Exception if a delete operation fails
   */
  private void deleteExpiredPartitionColStats(List<Object[]> expiredPartRows) throws Exception {
    // key: catName + SEP + dbName + SEP + tblName + SEP + partName, value: list of expired colNames
    Map<String, List<String>> partToColsMap = new LinkedHashMap<>();
    Map<String, String[]> keyToCoords = new LinkedHashMap<>();
    for (Object[] row : expiredPartRows) {
      String catName   = (String) row[0];
      String dbName    = (String) row[1];
      String tblName   = (String) row[2];
      String partName  = (String) row[3];
      String colName   = (String) row[4];
      String excludeVal = (String) row[5];
      if (Boolean.parseBoolean(excludeVal)) {
        LOG.info("Skipping auto deletion of partition stats for {}.{} due to exclude property.",
            dbName, tblName);
        continue;
      }
      String key = catName + KEY_SEP + dbName + KEY_SEP + tblName + KEY_SEP + partName;
      partToColsMap.computeIfAbsent(key, k -> new ArrayList<>()).add(colName);
      keyToCoords.putIfAbsent(key, new String[]{catName, dbName, tblName, partName});
    }

    // one transaction per partition, delete all expired columns in a single call
    for (Map.Entry<String, List<String>> entry : partToColsMap.entrySet()) {
      String[] coords = keyToCoords.get(entry.getKey());
      boolean committed = false;
      openTransaction();
      try {
        deletePartitionColumnStatistics(coords[0], coords[1], coords[2],
            Collections.singletonList(coords[3]), entry.getValue(), "hive");
        committed = commitTransaction();
      } finally {
        if (!committed) {
          rollbackTransaction();
        }
      }
    }
  }
}
