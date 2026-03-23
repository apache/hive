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

package org.apache.hadoop.hive.metastore.directsql;

import javax.jdo.PersistenceManager;
import javax.jdo.datastore.JDOConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.Batchable;
import org.apache.hadoop.hive.metastore.QueryWrapper;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.directsql.MetaStoreDirectSql.getIdListForIn;
import static org.apache.hadoop.hive.metastore.directsql.MetastoreDirectSqlUtils.closeDbConn;
import static org.apache.hadoop.hive.metastore.directsql.MetastoreDirectSqlUtils.executeWithArray;
import static org.apache.hadoop.hive.metastore.directsql.MetastoreDirectSqlUtils.extractSqlClob;
import static org.apache.hadoop.hive.metastore.directsql.MetastoreDirectSqlUtils.makeParams;

public class DirectSqlDeleteStats {
  private static final Logger LOG = LoggerFactory.getLogger(DirectSqlDeleteStats.class);
  private final MetaStoreDirectSql directSql;
  private final PersistenceManager pm;
  private final int batchSize;

  public DirectSqlDeleteStats(MetaStoreDirectSql directSql, PersistenceManager pm) {
    this.directSql = directSql;
    this.pm = pm;
    this.batchSize = directSql.getDirectSqlBatchSize();
  }

  public boolean deletePartitionColumnStats(String catName, String dbName, String tblName,
      List<String> partNames, List<String> colNames, String engine) throws MetaException {
    List<Long> partIds = Batchable.runBatched(batchSize, partNames, new Batchable<String, Long>() {
      @Override
      public List<Long> run(List<String> input) throws Exception {
        String sqlFilter = "\"PARTITIONS\".\"PART_NAME\" in  (" + makeParams(input.size()) + ")";
        List<Long> partitionIds = directSql.getPartitionIdsViaSqlFilter(catName, dbName, tblName, sqlFilter,
            input, Collections.emptyList(), -1);
        if (!partitionIds.isEmpty()) {
          String deleteSql = "delete from \"PART_COL_STATS\" where \"PART_ID\" in ( " + getIdListForIn(partitionIds) + ")";
          List<Object> params = new ArrayList<>(colNames == null ? 1 : colNames.size() + 1);

          if (colNames != null && !colNames.isEmpty()) {
            deleteSql += " and \"COLUMN_NAME\" in (" + makeParams(colNames.size()) + ")";
            params.addAll(colNames);
          }

          if (engine != null) {
            deleteSql += " and \"ENGINE\" = ?";
            params.add(engine);
          }
          try (QueryWrapper queryParams = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", deleteSql))) {
            executeWithArray(queryParams.getInnerQuery(), params.toArray(), deleteSql);
          }
        }
        return partitionIds;
      }
    });
    try {
      return updateColumnStatsAccurateForPartitions(partIds, colNames);
    } catch (SQLException e) {
      LOG.error("Fail to update the partitions' column accurate status", e);
      throw new MetaException("Updating partitions's column accurate throws: " + e.getMessage());
    }
  }

  /**
   a helper function which will get the current COLUMN_STATS_ACCURATE parameter on table level
   and update the COLUMN_STATS_ACCURATE parameter with the new value on table level using directSql
   */
  private long updateColumnStatsAccurateForTable(Table table, List<String> droppedCols) throws MetaException {
    Map<String, String> params = table.getParameters();
    // get the current COLUMN_STATS_ACCURATE
    String currentValue;
    if (params == null || (currentValue = params.get(StatsSetupConst.COLUMN_STATS_ACCURATE)) == null) {
      return 0;
    }
    // if the dropping columns is empty, that means we delete all the columns
    if (droppedCols == null || droppedCols.isEmpty()) {
      StatsSetupConst.clearColumnStatsState(params);
    } else {
      StatsSetupConst.removeColumnStatsState(params, droppedCols);
    }

    String updatedValue = params.get(StatsSetupConst.COLUMN_STATS_ACCURATE);
    // if the COL_STATS_ACCURATE has changed, then update it using directSql
    if (currentValue.equals(updatedValue)) {
      return 0;
    }
    return directSql.updateTableParam(table, StatsSetupConst.COLUMN_STATS_ACCURATE, currentValue, updatedValue);
  }

  private boolean updateColumnStatsAccurateForPartitions(List<Long> partIds, List<String> colNames)
      throws MetaException, SQLException {
    // Get the list of params that need to be updated
    List<Pair<Long, String>> updates = getPartColAccuToUpdate(partIds, colNames);
    if (updates.isEmpty()) {
      // Nothing to update: treat as successful completion
      return true;
    }
    JDOConnection jdoConn = null;
    try {
      jdoConn = pm.getDataStoreConnection();
      Connection dbConn = (Connection) jdoConn.getNativeConnection();
      String update = "UPDATE \"PARTITION_PARAMS\" SET " + " \"PARAM_VALUE\" = ?" +
          " WHERE \"PART_ID\" = ? AND \"PARAM_KEY\" = ?";
      try (PreparedStatement pst = dbConn.prepareStatement(update)) {
        List<Long> updated = new ArrayList<>();
        for (Pair<Long, String> accurate : updates) {
          pst.setString(1, accurate.getRight());
          pst.setLong(2, accurate.getLeft());
          pst.setString(3, StatsSetupConst.COLUMN_STATS_ACCURATE);
          pst.addBatch();
          updated.add(accurate.getLeft());
          if (updated.size() == batchSize) {
            LOG.debug("Execute updates on part: {}", updated);
            verifyUpdates(pst.executeBatch(), updated);
            updated = new ArrayList<>();
          }
        }
        if (!updated.isEmpty()) {
          verifyUpdates(pst.executeBatch(), updated);
        }
      }
      return true;
    } finally {
      closeDbConn(jdoConn);
    }
  }

  private void verifyUpdates(int[] numUpdates, List<Long> partIds) throws MetaException {
    for (int i = 0; i < numUpdates.length; i++) {
      if (numUpdates[i] != 1) {
        throw new MetaException("Invalid state of PARTITION_PARAMS ("
            + StatsSetupConst.COLUMN_STATS_ACCURATE + ") for PART_ID " + partIds.get(i));
      }
    }
  }

  private List<Pair<Long, String>> getPartColAccuToUpdate(List<Long> partIds, List<String> colNames) throws MetaException {
    return Batchable.runBatched(batchSize, partIds, new Batchable<>() {
      @Override
      public List<Pair<Long, String>> run(List<Long> input) throws Exception {
        // 3. Get current COLUMN_STATS_ACCURATE values
        String queryText = "SELECT \"PART_ID\", \"PARAM_VALUE\" FROM \"PARTITION_PARAMS\"" +
            " WHERE \"PARAM_KEY\" = ? AND \"PART_ID\" IN (" + makeParams(input.size()) + ")";
        Object[] params = new Object[1 + input.size()];
        params[0] = StatsSetupConst.COLUMN_STATS_ACCURATE;
        for (int i = 0; i < input.size(); i++) {
          params[i + 1] = input.get(i);
        }

        List<Pair<Long, String>> result = new ArrayList<>();
        try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
          @SuppressWarnings("unchecked") List<Object> sqlResult = executeWithArray(query.getInnerQuery(), params,
              queryText);
          for (Object row : sqlResult) {
            Object[] fields = (Object[]) row;
            Long partId = MetastoreDirectSqlUtils.extractSqlLong(fields[0]);
            if (fields[1] == null) {
              continue;
            }
            Map<String, String> accurateMap = new HashMap<>();
            String accurateBefore = extractSqlClob(fields[1]);
            accurateMap.put(StatsSetupConst.COLUMN_STATS_ACCURATE, accurateBefore);
            if (colNames == null || colNames.isEmpty()) {
              StatsSetupConst.clearColumnStatsState(accurateMap);
            } else {
              StatsSetupConst.removeColumnStatsState(accurateMap, colNames);
            }
            String accurateAfter = accurateMap.get(StatsSetupConst.COLUMN_STATS_ACCURATE);
            if (accurateAfter.equals(accurateBefore)) {
              continue;
            }
            result.add(Pair.of(partId, accurateAfter));
          }
        }
        return result;
      }
    });
  }

  public boolean deleteTableColumnStatistics(Table table, List<String> colNames, String engine) throws MetaException {
    String deleteSql = "delete from \"TAB_COL_STATS\" where \"TBL_ID\" = ?";
    List<Object> params = new ArrayList<>();
    params.add(table.getId());

    if (colNames != null && !colNames.isEmpty()) {
      deleteSql += " and \"COLUMN_NAME\" in (" + makeParams(colNames.size()) + ")";
      params.addAll(colNames);
    }
    if (engine != null) {
      deleteSql += " and \"ENGINE\" = ?";
      params.add(engine);
    }
    try (QueryWrapper queryParams = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", deleteSql))) {
      executeWithArray(queryParams.getInnerQuery(), params.toArray(), deleteSql);
    }
    long numUpdated = updateColumnStatsAccurateForTable(table, colNames);
    if (numUpdated == 0 && LOG.isDebugEnabled()) {
      LOG.debug("No COLUMN_STATS_ACCURATE rows updated for table {}", table.getTableName());
    }
    // Return true as long as the delete (and any required metadata updates) completed without exception.
    return true;
  }
}
