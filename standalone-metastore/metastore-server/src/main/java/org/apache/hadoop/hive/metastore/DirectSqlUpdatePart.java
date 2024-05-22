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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.events.UpdatePartitionColumnStatEvent;
import org.apache.hadoop.hive.metastore.events.UpdatePartitionColumnStatEventBatch;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.model.MColumnDescriptor;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MStringList;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.datanucleus.ExecutionContext;
import org.datanucleus.api.jdo.JDOPersistenceManager;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;

import javax.jdo.PersistenceManager;
import javax.jdo.datastore.JDOConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.common.StatsSetupConst.COLUMN_STATS_ACCURATE;
import static org.apache.hadoop.hive.metastore.HMSHandler.getPartValsFromName;
import static org.apache.hadoop.hive.metastore.MetastoreDirectSqlUtils.executeWithArray;
import static org.apache.hadoop.hive.metastore.MetastoreDirectSqlUtils.extractSqlClob;
import static org.apache.hadoop.hive.metastore.MetastoreDirectSqlUtils.extractSqlInt;
import static org.apache.hadoop.hive.metastore.MetastoreDirectSqlUtils.extractSqlLong;

/**
 * This class contains the optimizations for MetaStore that rely on direct SQL access to
 * the underlying database. It should use ANSI SQL and be compatible with common databases
 * such as MySQL (note that MySQL doesn't use full ANSI mode by default), Postgres, etc.
 *
 * This class separates out the update part from MetaStoreDirectSql class.
 */
class DirectSqlUpdatePart {
  private static final Logger LOG = LoggerFactory.getLogger(DirectSqlUpdatePart.class.getName());

  private final PersistenceManager pm;
  private final Configuration conf;
  private final DatabaseProduct dbType;
  private final int maxBatchSize;
  private final SQLGenerator sqlGenerator;

  public DirectSqlUpdatePart(PersistenceManager pm, Configuration conf,
                             DatabaseProduct dbType, int batchSize) {
    this.pm = pm;
    this.conf = conf;
    this.dbType = dbType;
    this.maxBatchSize = batchSize;
    sqlGenerator = new SQLGenerator(dbType, conf);
  }

  void rollbackDBConn(Connection dbConn) {
    try {
      if (dbConn != null && !dbConn.isClosed()) dbConn.rollback();
    } catch (SQLException e) {
      LOG.warn("Failed to rollback db connection ", e);
    }
  }

  void closeDbConn(JDOConnection jdoConn) {
    try {
      if (jdoConn != null) {
        jdoConn.close();
      }
    } catch (Exception e) {
      LOG.warn("Failed to close db connection", e);
    }
  }

  static String quoteString(String input) {
    return "'" + input + "'";
  }

  private void populateInsertUpdateMap(Map<PartitionInfo, ColumnStatistics> statsPartInfoMap,
                                       Map<PartColNameInfo, MPartitionColumnStatistics> updateMap,
                                       Map<PartColNameInfo, MPartitionColumnStatistics>insertMap,
                                       Connection dbConn, Table tbl) throws SQLException, MetaException, NoSuchObjectException {
    StringBuilder prefix = new StringBuilder();
    StringBuilder suffix = new StringBuilder();
    List<String> queries = new ArrayList<>();
    Set<PartColNameInfo> selectedParts = new HashSet<>();

    List<Long> partIdList = statsPartInfoMap.keySet().stream().map(
            e -> e.partitionId).collect(Collectors.toList()
    );

    prefix.append("select \"PART_ID\", \"COLUMN_NAME\", \"ENGINE\" from \"PART_COL_STATS\" WHERE ");
    TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix,
            partIdList, "\"PART_ID\"", true, false);

    try (Statement statement = dbConn.createStatement()) {
      for (String query : queries) {
        LOG.debug("Execute query: " + query);
        try (ResultSet rs = statement.executeQuery(query)) {
          while (rs.next()) {
            selectedParts.add(new PartColNameInfo(rs.getLong(1), rs.getString(2), rs.getString(3)));
          }
        }
      }
    }

    for (Map.Entry entry : statsPartInfoMap.entrySet()) {
      PartitionInfo partitionInfo = (PartitionInfo) entry.getKey();
      ColumnStatistics colStats = (ColumnStatistics) entry.getValue();
      long partId = partitionInfo.partitionId;
      ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();
      if (!statsDesc.isSetCatName()) {
        statsDesc.setCatName(tbl.getCatName());
      }
      for (ColumnStatisticsObj statisticsObj : colStats.getStatsObj()) {
        PartColNameInfo temp = new PartColNameInfo(partId, statisticsObj.getColName(),
            colStats.getEngine());
        if (selectedParts.contains(temp)) {
          updateMap.put(temp, StatObjectConverter.
                  convertToMPartitionColumnStatistics(null, statsDesc, statisticsObj, colStats.getEngine()));
        } else {
          insertMap.put(temp, StatObjectConverter.
                  convertToMPartitionColumnStatistics(null, statsDesc, statisticsObj, colStats.getEngine()));
        }
      }
    }
  }

  private void updatePartColStatTable(Map<PartColNameInfo, MPartitionColumnStatistics> updateMap,
                                          Connection dbConn) throws SQLException, MetaException, NoSuchObjectException {
    Map<String, List<Map.Entry<PartColNameInfo, MPartitionColumnStatistics>>> updates = new HashMap<>();
    for (Map.Entry<PartColNameInfo, MPartitionColumnStatistics> entry : updateMap.entrySet()) {
      MPartitionColumnStatistics mPartitionColumnStatistics = entry.getValue();
      StringBuilder update = new StringBuilder("UPDATE \"PART_COL_STATS\" SET ")
          .append(StatObjectConverter.getUpdatedColumnSql(mPartitionColumnStatistics))
          .append(" WHERE \"PART_ID\" = ? AND \"COLUMN_NAME\" = ? AND \"ENGINE\" = ?");
      updates.computeIfAbsent(update.toString(), k -> new ArrayList<>()).add(entry);
    }

    for (Map.Entry<String, List<Map.Entry<PartColNameInfo, MPartitionColumnStatistics>>> entry : updates.entrySet()) {
      List<Long> partIds = new ArrayList<>();
      try (PreparedStatement pst = dbConn.prepareStatement(entry.getKey())) {
        List<Map.Entry<PartColNameInfo, MPartitionColumnStatistics>> entries = entry.getValue();
        for (Map.Entry<PartColNameInfo, MPartitionColumnStatistics> partStats : entries) {
          PartColNameInfo partColNameInfo = partStats.getKey();
          MPartitionColumnStatistics mPartitionColumnStatistics = partStats.getValue();
          int colIdx = StatObjectConverter.initUpdatedColumnStatement(mPartitionColumnStatistics, pst);
          pst.setLong(colIdx++, partColNameInfo.partitionId);
          pst.setString(colIdx++, mPartitionColumnStatistics.getColName());
          pst.setString(colIdx++, mPartitionColumnStatistics.getEngine());
          partIds.add(partColNameInfo.partitionId);
          pst.addBatch();
          if (partIds.size() == maxBatchSize) {
            LOG.debug("Execute updates on part: {}", partIds);
            verifyUpdates(pst.executeBatch(), partIds);
            partIds = new ArrayList<>();
          }
        }
        if (!partIds.isEmpty()) {
          LOG.debug("Execute updates on part: {}", partIds);
          verifyUpdates(pst.executeBatch(), partIds);
        }
      }
    }
  }

  private void verifyUpdates(int[] numUpdates, List<Long> partIds) throws MetaException {
    for (int i = 0; i < numUpdates.length; i++) {
      if (numUpdates[i] != 1) {
        throw new MetaException("Invalid state of PART_COL_STATS for PART_ID " + partIds.get(i));
      }
    }
  }

  private void insertIntoPartColStatTable(Map<PartColNameInfo, MPartitionColumnStatistics> insertMap,
                                          long maxCsId,
                                          Connection dbConn) throws SQLException, MetaException, NoSuchObjectException {
    int numRows = 0;
    String insert = "INSERT INTO \"PART_COL_STATS\" (\"CS_ID\", \"CAT_NAME\", \"DB_NAME\","
            + "\"TABLE_NAME\", \"PARTITION_NAME\", \"COLUMN_NAME\", \"COLUMN_TYPE\", \"PART_ID\","
            + " \"LONG_LOW_VALUE\", \"LONG_HIGH_VALUE\", \"DOUBLE_HIGH_VALUE\", \"DOUBLE_LOW_VALUE\","
            + " \"BIG_DECIMAL_LOW_VALUE\", \"BIG_DECIMAL_HIGH_VALUE\", \"NUM_NULLS\", \"NUM_DISTINCTS\", \"BIT_VECTOR\" ,"
            + " \"HISTOGRAM\", \"AVG_COL_LEN\", \"MAX_COL_LEN\", \"NUM_TRUES\", \"NUM_FALSES\", \"LAST_ANALYZED\", \"ENGINE\") values "
            + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    try (PreparedStatement preparedStatement = dbConn.prepareStatement(insert)) {
      for (Map.Entry entry : insertMap.entrySet()) {
        PartColNameInfo partColNameInfo = (PartColNameInfo) entry.getKey();
        Long partId = partColNameInfo.partitionId;
        MPartitionColumnStatistics mPartitionColumnStatistics = (MPartitionColumnStatistics) entry.getValue();

        preparedStatement.setLong(1, maxCsId);
        preparedStatement.setString(2, mPartitionColumnStatistics.getCatName());
        preparedStatement.setString(3, mPartitionColumnStatistics.getDbName());
        preparedStatement.setString(4, mPartitionColumnStatistics.getTableName());
        preparedStatement.setString(5, mPartitionColumnStatistics.getPartitionName());
        preparedStatement.setString(6, mPartitionColumnStatistics.getColName());
        preparedStatement.setString(7, mPartitionColumnStatistics.getColType());
        preparedStatement.setLong(8, partId);
        preparedStatement.setObject(9, mPartitionColumnStatistics.getLongLowValue());
        preparedStatement.setObject(10, mPartitionColumnStatistics.getLongHighValue());
        preparedStatement.setObject(11, mPartitionColumnStatistics.getDoubleHighValue());
        preparedStatement.setObject(12, mPartitionColumnStatistics.getDoubleLowValue());
        preparedStatement.setString(13, mPartitionColumnStatistics.getDecimalLowValue());
        preparedStatement.setString(14, mPartitionColumnStatistics.getDecimalHighValue());
        preparedStatement.setObject(15, mPartitionColumnStatistics.getNumNulls());
        preparedStatement.setObject(16, mPartitionColumnStatistics.getNumDVs());
        preparedStatement.setObject(17, mPartitionColumnStatistics.getBitVector());
        preparedStatement.setBytes(18, mPartitionColumnStatistics.getHistogram());
        preparedStatement.setObject(19, mPartitionColumnStatistics.getAvgColLen());
        preparedStatement.setObject(20, mPartitionColumnStatistics.getMaxColLen());
        preparedStatement.setObject(21, mPartitionColumnStatistics.getNumTrues());
        preparedStatement.setObject(22, mPartitionColumnStatistics.getNumFalses());
        preparedStatement.setLong(23, mPartitionColumnStatistics.getLastAnalyzed());
        preparedStatement.setString(24, mPartitionColumnStatistics.getEngine());

        maxCsId++;
        numRows++;
        preparedStatement.addBatch();
        if (numRows == maxBatchSize) {
          preparedStatement.executeBatch();
          numRows = 0;
        }
      }

      if (numRows != 0) {
        preparedStatement.executeBatch();
      }
    }
  }

  private Map<Long, String> getParamValues(Connection dbConn, List<Long> partIdList) throws SQLException {
    List<String> queries = new ArrayList<>();
    StringBuilder prefix = new StringBuilder();
    StringBuilder suffix = new StringBuilder();

    prefix.append("select \"PART_ID\", \"PARAM_VALUE\" "
            + " from \"PARTITION_PARAMS\" where "
            + " \"PARAM_KEY\" = 'COLUMN_STATS_ACCURATE' "
            + " and ");
    TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix,
            partIdList, "\"PART_ID\"", true, false);

    Map<Long, String> partIdToParaMap = new HashMap<>();
    try (Statement statement = dbConn.createStatement()) {
      for (String query : queries) {
        LOG.debug("Execute query: " + query);
        try (ResultSet rs = statement.executeQuery(query)) {
          while (rs.next()) {
            partIdToParaMap.put(rs.getLong(1), rs.getString(2));
          }
        }
      }
    }

    return partIdToParaMap;
  }

  private void updateWriteIdForPartitions(Connection dbConn, long writeId, List<Long> partIdList) throws SQLException {
    StringBuilder prefix = new StringBuilder();
    List<String> queries = new ArrayList<>();
    StringBuilder suffix = new StringBuilder();

    prefix.append("UPDATE \"PARTITIONS\" set \"WRITE_ID\" = " + writeId + " where ");
    TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix,
            partIdList, "\"PART_ID\"", false, false);

    try (Statement statement = dbConn.createStatement()) {
      for (String query : queries) {
        LOG.debug("Execute update: " + query);
        statement.executeUpdate(query);
      }
    }
  }

  private Map<String, Map<String, String>> updatePartitionParamTable(Connection dbConn,
                                                                     Map<PartitionInfo, ColumnStatistics> partitionInfoMap,
                                                                     String validWriteIds,
                                                                     long writeId,
                                                                     boolean isAcidTable)
          throws SQLException, MetaException {
    Map<String, Map<String, String>> result = new HashMap<>();
    boolean areTxnStatsSupported = MetastoreConf.getBoolVar(conf, ConfVars.HIVE_TXN_STATS_ENABLED);
    String insert = "INSERT INTO \"PARTITION_PARAMS\" (\"PART_ID\", \"PARAM_KEY\", \"PARAM_VALUE\") "
            + "VALUES( ? , 'COLUMN_STATS_ACCURATE'  , ? )";
    String delete = "DELETE from \"PARTITION_PARAMS\" "
            + " where \"PART_ID\" = ? "
            + " and \"PARAM_KEY\" = 'COLUMN_STATS_ACCURATE'";
    String update = "UPDATE \"PARTITION_PARAMS\" set \"PARAM_VALUE\" = ? "
            + " where \"PART_ID\" = ? "
            + " and \"PARAM_KEY\" = 'COLUMN_STATS_ACCURATE'";
    int numInsert = 0;
    int numDelete = 0;
    int numUpdate = 0;

    List<Long> partIdList = partitionInfoMap.keySet().stream().map(
            e -> e.partitionId).collect(Collectors.toList()
    );

    // get the old parameters from PARTITION_PARAMS table.
    Map<Long, String> partIdToParaMap = getParamValues(dbConn, partIdList);

    try (PreparedStatement statementInsert = dbConn.prepareStatement(insert);
         PreparedStatement statementDelete = dbConn.prepareStatement(delete);
         PreparedStatement statementUpdate = dbConn.prepareStatement(update)) {
      for (Map.Entry entry : partitionInfoMap.entrySet()) {
        PartitionInfo partitionInfo = (PartitionInfo) entry.getKey();
        ColumnStatistics colStats = (ColumnStatistics) entry.getValue();
        List<String> colNames = colStats.getStatsObj().stream().map(e -> e.getColName()).collect(Collectors.toList());
        long partWriteId = partitionInfo.writeId;
        long partId = partitionInfo.partitionId;
        Map<String, String> newParameter;

        if (!partIdToParaMap.containsKey(partId)) {
          newParameter = new HashMap<>();
          newParameter.put(COLUMN_STATS_ACCURATE, "TRUE");
          StatsSetupConst.setColumnStatsState(newParameter, colNames);
          statementInsert.setLong(1, partId);
          statementInsert.setString(2, newParameter.get(COLUMN_STATS_ACCURATE));
          numInsert++;
          statementInsert.addBatch();
          if (numInsert == maxBatchSize) {
            LOG.debug(" Executing insert " + insert);
            statementInsert.executeBatch();
            numInsert = 0;
          }
        } else {
          String oldStats = partIdToParaMap.get(partId);

          Map<String, String> oldParameter = new HashMap<>();
          oldParameter.put(COLUMN_STATS_ACCURATE, oldStats);

          newParameter = new HashMap<>();
          newParameter.put(COLUMN_STATS_ACCURATE, oldStats);
          StatsSetupConst.setColumnStatsState(newParameter, colNames);

          if (isAcidTable) {
            String errorMsg = ObjectStore.verifyStatsChangeCtx(
                    colStats.getStatsDesc().getDbName() + "." + colStats.getStatsDesc().getTableName(),
                    oldParameter, newParameter, writeId, validWriteIds, true);
            if (errorMsg != null) {
              throw new MetaException(errorMsg);
            }
          }

          if (isAcidTable &&
                  (!areTxnStatsSupported || !ObjectStore.isCurrentStatsValidForTheQuery(oldParameter, partWriteId,
                          validWriteIds, true))) {
            statementDelete.setLong(1, partId);
            statementDelete.addBatch();
            numDelete++;
            if (numDelete == maxBatchSize) {
              statementDelete.executeBatch();
              numDelete = 0;
              LOG.debug("Removed COLUMN_STATS_ACCURATE from the parameters of the partition "
                      + colStats.getStatsDesc().getDbName() + "." + colStats.getStatsDesc().getTableName() + "."
                      + colStats.getStatsDesc().getPartName());
            }
          } else {
            statementUpdate.setString(1, newParameter.get(COLUMN_STATS_ACCURATE));
            statementUpdate.setLong(2, partId);
            statementUpdate.addBatch();
            numUpdate++;
            if (numUpdate == maxBatchSize) {
              LOG.debug(" Executing update " + statementUpdate);
              statementUpdate.executeBatch();
              numUpdate = 0;
            }
          }
        }
        result.put(partitionInfo.partitionName, newParameter);
      }

      if (numInsert != 0) {
        statementInsert.executeBatch();
      }

      if (numUpdate != 0) {
        statementUpdate.executeBatch();
      }

      if (numDelete != 0) {
        statementDelete.executeBatch();
      }

      if (isAcidTable) {
        updateWriteIdForPartitions(dbConn, writeId, partIdList);
      }
      return result;
    }
  }


  private Map<PartitionInfo, ColumnStatistics> getPartitionInfo(Connection dbConn, long tblId,
                                                                 Map<String, ColumnStatistics> partColStatsMap)
          throws SQLException, MetaException {
    List<String> queries = new ArrayList<>();
    StringBuilder prefix = new StringBuilder();
    StringBuilder suffix = new StringBuilder();
    Map<PartitionInfo, ColumnStatistics> partitionInfoMap = new HashMap<>();

    List<String> partKeys = partColStatsMap.keySet().stream().map(
            e -> quoteString(e)).collect(Collectors.toList()
    );

    prefix.append("select \"PART_ID\", \"WRITE_ID\", \"PART_NAME\"  from \"PARTITIONS\" where ");
    suffix.append(" and  \"TBL_ID\" = " + tblId);
    TxnUtils.buildQueryWithINClauseStrings(conf, queries, prefix, suffix,
            partKeys, "\"PART_NAME\"", true, false);

    try (Statement statement = dbConn.createStatement()) {
      for (String query : queries) {
        // Select for update makes sure that the partitions are not modified while the stats are getting updated.
        query = sqlGenerator.addForUpdateClause(query);
        LOG.debug("Execute query: " + query);
        try (ResultSet rs = statement.executeQuery(query)) {
          while (rs.next()) {
            PartitionInfo partitionInfo = new PartitionInfo(rs.getLong(1),
                rs.getLong(2), rs.getString(3));
            partitionInfoMap.put(partitionInfo, partColStatsMap.get(rs.getString(3)));
          }
        }
      }
    }
    return partitionInfoMap;
  }

  private void setAnsiQuotes(Connection dbConn) throws SQLException {
    if (sqlGenerator.getDbProduct().isMYSQL()) {
      try (Statement stmt = dbConn.createStatement()) {
        stmt.execute("SET @@session.sql_mode=ANSI_QUOTES");
      }
    }
  }

  /**
   * Update the statistics for the given partitions. Add the notification logs also.
   * @return map of partition key to column stats if successful, null otherwise.
   */
  public Map<String, Map<String, String>> updatePartitionColumnStatistics(Map<String, ColumnStatistics> partColStatsMap,
                                                      Table tbl, long csId,
                                                      String validWriteIds, long writeId,
                                                      List<TransactionalMetaStoreEventListener> transactionalListeners)
          throws MetaException {
    JDOConnection jdoConn = null;
    Connection dbConn = null;
    boolean committed = false;
    try {
      dbType.lockInternal();
      jdoConn = pm.getDataStoreConnection();
      dbConn = (Connection) (jdoConn.getNativeConnection());

      setAnsiQuotes(dbConn);

      Map<PartitionInfo, ColumnStatistics> partitionInfoMap = getPartitionInfo(dbConn, tbl.getId(), partColStatsMap);

      Map<String, Map<String, String>> result =
              updatePartitionParamTable(dbConn, partitionInfoMap, validWriteIds, writeId, TxnUtils.isAcidTable(tbl));

      Map<PartColNameInfo, MPartitionColumnStatistics> insertMap = new HashMap<>();
      Map<PartColNameInfo, MPartitionColumnStatistics> updateMap = new HashMap<>();
      populateInsertUpdateMap(partitionInfoMap, updateMap, insertMap, dbConn, tbl);

      LOG.info("Number of stats to insert  " + insertMap.size() + " update " + updateMap.size());

      if (insertMap.size() != 0) {
        insertIntoPartColStatTable(insertMap, csId, dbConn);
      }

      if (updateMap.size() != 0) {
        updatePartColStatTable(updateMap, dbConn);
      }

      if (transactionalListeners != null) {
        UpdatePartitionColumnStatEventBatch eventBatch = new UpdatePartitionColumnStatEventBatch(null);
        for (Map.Entry entry : result.entrySet()) {
          Map<String, String> parameters = (Map<String, String>) entry.getValue();
          ColumnStatistics colStats = partColStatsMap.get(entry.getKey());
          List<String> partVals = getPartValsFromName(tbl, colStats.getStatsDesc().getPartName());
          UpdatePartitionColumnStatEvent event = new UpdatePartitionColumnStatEvent(colStats, partVals, parameters,
                  tbl, writeId, null);
          eventBatch.addPartColStatEvent(event);
        }
        MetaStoreListenerNotifier.notifyEventWithDirectSql(transactionalListeners,
                EventMessage.EventType.UPDATE_PARTITION_COLUMN_STAT_BATCH, eventBatch, dbConn, sqlGenerator);
      }
      dbConn.commit();
      committed = true;
      return result;
    } catch (Exception e) {
      LOG.error("Unable to update Column stats for  " + tbl.getTableName(), e);
      throw new MetaException("Unable to update Column stats for  " + tbl.getTableName()
              + " due to: "  + e.getMessage());
    } finally {
      if (!committed) {
        rollbackDBConn(dbConn);
      }
      closeDbConn(jdoConn);
      dbType.unlockInternal();
    }
  }

  /**
   * Gets the next CS id from sequence MPartitionColumnStatistics and increment the CS id by numStats.
   * @return The CD id before update.
   */
  public long getNextCSIdForMPartitionColumnStatistics(long numStats) throws MetaException {
    long maxCsId = 0;
    boolean committed = false;
    Connection dbConn = null;
    JDOConnection jdoConn = null;

    try {
      dbType.lockInternal();
      jdoConn = pm.getDataStoreConnection();
      dbConn = (Connection) (jdoConn.getNativeConnection());

      setAnsiQuotes(dbConn);

      // This loop will be iterated at max twice. If there is no records, it will first insert and then do a select.
      // We are not using any upsert operations as select for update and then update is required to make sure that
      // the caller gets a reserved range for CSId not used by any other thread.
      boolean insertDone = false;
      while (maxCsId == 0) {
        String query = sqlGenerator.addForUpdateClause("SELECT \"NEXT_VAL\" FROM \"SEQUENCE_TABLE\" "
                + "WHERE \"SEQUENCE_NAME\"= "
                + quoteString("org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics"));
        LOG.debug("Execute query: " + query);
        try (Statement statement = dbConn.createStatement();
             ResultSet rs = statement.executeQuery(query)) {
          if (rs.next()) {
            maxCsId = rs.getLong(1);
          } else if (insertDone) {
            throw new MetaException("Invalid state of SEQUENCE_TABLE for MPartitionColumnStatistics");
          } else {
            insertDone = true;
            query = "INSERT INTO \"SEQUENCE_TABLE\" (\"SEQUENCE_NAME\", \"NEXT_VAL\")  VALUES ( "
                    + quoteString("org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics") + "," + 1
                    + ")";
            try {
              statement.executeUpdate(query);
            } catch (SQLException e) {
              // If the record is already inserted by some other thread continue to select.
              if (dbType.isDuplicateKeyError(e)) {
                continue;
              }
              LOG.error("Unable to insert into SEQUENCE_TABLE for MPartitionColumnStatistics.", e);
              throw e;
            }
          }
        }
      }

      long nextMaxCsId = maxCsId + numStats + 1;
      String query = "UPDATE \"SEQUENCE_TABLE\" SET \"NEXT_VAL\" = "
              + nextMaxCsId
              + " WHERE \"SEQUENCE_NAME\" = "
              + quoteString("org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics");

      try (Statement statement = dbConn.createStatement()) {
        statement.executeUpdate(query);
      }
      dbConn.commit();
      committed = true;
      return maxCsId;
    } catch (Exception e) {
      LOG.error("Unable to getNextCSIdForMPartitionColumnStatistics", e);
      throw new MetaException("Unable to getNextCSIdForMPartitionColumnStatistics  "
              + " due to: " + e.getMessage());
    } finally {
      if (!committed) {
        rollbackDBConn(dbConn);
      }
      closeDbConn(jdoConn);
      dbType.unlockInternal();
    }
  }

  public void alterPartitions(Map<List<String>, Long> partValuesToId, Map<Long, Long> partIdToSdId,
                              List<Partition> newParts) throws MetaException {
    List<Long> partIds = new ArrayList<>(newParts.size());
    Map<Long, Optional<Map<String, String>>> partParamsOpt = new HashMap<>();
    Map<Long, StorageDescriptor> idToSd = new HashMap<>();
    for (Partition newPart : newParts) {
      Long partId = partValuesToId.get(newPart.getValues());
      Long sdId = partIdToSdId.get(partId);
      partIds.add(partId);
      partParamsOpt.put(partId, Optional.ofNullable(newPart.getParameters()));
      idToSd.put(sdId, newPart.getSd());
    }

    // alter partitions does not change partition values,
    // so only PARTITIONS and PARTITION_PARAMS need to update.
    updatePartitionsInBatch(partValuesToId, newParts);
    updateParamTableInBatch("\"PARTITION_PARAMS\"", "\"PART_ID\"", partIds, partParamsOpt);
    updateStorageDescriptorInBatch(idToSd);
  }

  private interface ThrowableConsumer<T> {
    void accept(T t) throws SQLException, MetaException;
  }

  private <T> List<Long> filterIdsByNonNullValue(List<Long> ids, Map<Long, T> map) {
    return ids.stream().filter(id -> map.get(id) != null).collect(Collectors.toList());
  }

  private void updateWithStatement(ThrowableConsumer<PreparedStatement> consumer, String query)
      throws MetaException {
    JDOConnection jdoConn = pm.getDataStoreConnection();
    boolean doTrace = LOG.isDebugEnabled();
    long start = doTrace ? System.nanoTime() : 0;
    try (PreparedStatement statement =
             ((Connection) jdoConn.getNativeConnection()).prepareStatement(query)) {
      consumer.accept(statement);
      MetastoreDirectSqlUtils.timingTrace(doTrace, query, start, doTrace ? System.nanoTime() : 0);
    } catch (SQLException e) {
      LOG.error("Failed to execute update query: " + query, e);
      throw new MetaException("Unable to execute update due to: " + e.getMessage());
    } finally {
      closeDbConn(jdoConn);
    }
  }

  private void updatePartitionsInBatch(Map<List<String>, Long> partValuesToId,
                                       List<Partition> newParts) throws MetaException {
    List<String> columns = Arrays.asList("\"CREATE_TIME\"", "\"LAST_ACCESS_TIME\"", "\"WRITE_ID\"");
    List<String> conditionKeys = Arrays.asList("\"PART_ID\"");
    String stmt = TxnUtils.createUpdatePreparedStmt("\"PARTITIONS\"", columns, conditionKeys);
    int maxRows = dbType.getMaxRows(maxBatchSize, 4);
    updateWithStatement(statement -> Batchable.runBatched(maxRows, newParts, new Batchable<Partition, Void>() {
      @Override
      public List<Void> run(List<Partition> input) throws SQLException {
        for (Partition p : input) {
          statement.setLong(1, p.getCreateTime());
          statement.setLong(2, p.getLastAccessTime());
          statement.setLong(3, p.getWriteId());
          statement.setLong(4, partValuesToId.get(p.getValues()));
          statement.addBatch();
        }
        statement.executeBatch();
        return null;
      }
    }), stmt);
  }

  /* Get stringListId from both SKEWED_VALUES and SKEWED_COL_VALUE_LOC_MAP tables. */
  private List<Long> getStringListId(List<Long> sdIds) throws MetaException {
    return Batchable.runBatched(maxBatchSize, sdIds, new Batchable<Long, Long>() {
      @Override
      public List<Long> run(List<Long> input) throws Exception {
        List<Long> result = new ArrayList<>();
        String idLists = MetaStoreDirectSql.getIdListForIn(input);
        String queryFromSkewedValues = "select \"STRING_LIST_ID_EID\" " +
            "from \"SKEWED_VALUES\" where \"SD_ID_OID\" in (" + idLists + ")";
        try (QueryWrapper query =
                 new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryFromSkewedValues))) {
          List<Long> sqlResult = executeWithArray(query.getInnerQuery(), null, queryFromSkewedValues);
          result.addAll(sqlResult);
        }
        String queryFromValueLoc = "select \"STRING_LIST_ID_KID\" " +
            "from \"SKEWED_COL_VALUE_LOC_MAP\" where \"SD_ID\" in (" + idLists + ")";
        try (QueryWrapper query =
                 new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryFromValueLoc))) {
          List<Long> sqlResult = executeWithArray(query.getInnerQuery(), null, queryFromValueLoc);
          result.addAll(sqlResult);
        }
        return result;
      }
    });
  }

  private void updateParamTableInBatch(String paramTable, String idColumn, List<Long> ids,
                                       Map<Long, Optional<Map<String, String>>> newParamsOpt) throws MetaException {
    Map<Long, Map<String, String>> oldParams = getParams(paramTable, idColumn, ids);

    List<Pair<Long, String>> paramsToDelete = new ArrayList<>();
    List<Pair<Long, Pair<String, String>>> paramsToUpdate = new ArrayList<>();
    List<Pair<Long, Pair<String, String>>> paramsToAdd = new ArrayList<>();

    for (Long id : ids) {
      Map<String, String> oldParam = oldParams.getOrDefault(id, new HashMap<>());
      Map<String, String> newParam = newParamsOpt.get(id).orElseGet(HashMap::new);
      for (Map.Entry<String, String> entry : oldParam.entrySet()) {
        String key = entry.getKey();
        String oldValue = entry.getValue();
        if (!newParam.containsKey(key)) {
          paramsToDelete.add(Pair.of(id, key));
        } else if (!oldValue.equals(newParam.get(key))) {
          paramsToUpdate.add(Pair.of(id, Pair.of(key, newParam.get(key))));
        }
      }
      List<Pair<Long, Pair<String, String>>> newlyParams = newParam.entrySet().stream()
          .filter(entry -> !oldParam.containsKey(entry.getKey()))
          .map(entry -> Pair.of(id, Pair.of(entry.getKey(), entry.getValue())))
          .collect(Collectors.toList());
      paramsToAdd.addAll(newlyParams);
    }

    deleteParams(paramTable, idColumn, paramsToDelete);
    updateParams(paramTable, idColumn, paramsToUpdate);
    insertParams(paramTable, idColumn, paramsToAdd);
  }

  private Map<Long, Map<String, String>> getParams(String paramTable, String idName,
                                                   List<Long> ids) throws MetaException {
    Map<Long, Map<String, String>> idToParams = new HashMap<>();
    Batchable.runBatched(maxBatchSize, ids, new Batchable<Long, Object>() {
      @Override
      public List<Object> run(List<Long> input) throws MetaException {
        String idLists = MetaStoreDirectSql.getIdListForIn(input);
        String queryText = "select " + idName + ", \"PARAM_KEY\", \"PARAM_VALUE\" from " +
            paramTable + " where " + idName +  " in (" + idLists + ")";
        try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
          List<Object[]> sqlResult = executeWithArray(query.getInnerQuery(), null, queryText);
          for (Object[] row : sqlResult) {
            Long id = extractSqlLong(row[0]);
            String paramKey = extractSqlClob(row[1]);
            String paramVal = extractSqlClob(row[2]);
            idToParams.computeIfAbsent(id, key -> new HashMap<>()).put(paramKey, paramVal);
          }
        }
        return null;
      }
    });
    return idToParams;
  }

  private void deleteParams(String paramTable, String idColumn,
                            List<Pair<Long, String>> deleteIdKeys) throws MetaException {
    String deleteStmt = "delete from " + paramTable + " where " + idColumn +  "=? and PARAM_KEY=?";
    int maxRows = dbType.getMaxRows(maxBatchSize, 2);
    updateWithStatement(statement -> Batchable.runBatched(maxRows, deleteIdKeys,
        new Batchable<Pair<Long, String>, Void>() {
          @Override
          public List<Void> run(List<Pair<Long, String>> input) throws SQLException {
            for (Pair<Long, String> pair : input) {
              statement.setLong(1, pair.getLeft());
              statement.setString(2, pair.getRight());
              statement.addBatch();
            }
            statement.executeBatch();
            return null;
          }
        }
    ), deleteStmt);
  }

  private void updateParams(String paramTable, String idColumn,
                            List<Pair<Long, Pair<String, String>>> updateIdAndParams) throws MetaException {
    List<String> columns = Arrays.asList("\"PARAM_VALUE\"");
    List<String> conditionKeys = Arrays.asList(idColumn, "\"PARAM_KEY\"");
    String stmt = TxnUtils.createUpdatePreparedStmt(paramTable, columns, conditionKeys);
    int maxRows = dbType.getMaxRows(maxBatchSize, 3);
    updateWithStatement(statement -> Batchable.runBatched(maxRows, updateIdAndParams,
        new Batchable<Pair<Long, Pair<String, String>>, Object>() {
          @Override
          public List<Object> run(List<Pair<Long, Pair<String, String>>> input) throws SQLException {
            for (Pair<Long, Pair<String, String>> pair : input) {
              statement.setString(1, pair.getRight().getRight());
              statement.setLong(2, pair.getLeft());
              statement.setString(3, pair.getRight().getLeft());
              statement.addBatch();
            }
            statement.executeBatch();
            return null;
          }
        }
    ), stmt);
  }

  private void insertParams(String paramTable, String idColumn,
                            List<Pair<Long, Pair<String, String>>> addIdAndParams) throws MetaException {
    List<String> columns = Arrays.asList(idColumn, "\"PARAM_KEY\"", "\"PARAM_VALUE\"");
    String query = TxnUtils.createInsertPreparedStmt(paramTable, columns);
    int maxRows = dbType.getMaxRows(maxBatchSize, 3);
    updateWithStatement(statement -> Batchable.runBatched(maxRows, addIdAndParams,
        new Batchable<Pair<Long, Pair<String, String>>, Void>() {
          @Override
          public List<Void> run(List<Pair<Long, Pair<String, String>>> input) throws SQLException {
            for (Pair<Long, Pair<String, String>> pair : input) {
              statement.setLong(1, pair.getLeft());
              statement.setString(2, pair.getRight().getLeft());
              statement.setString(3, pair.getRight().getRight());
              statement.addBatch();
            }
            statement.executeBatch();
            return null;
          }
        }
    ), query);
  }

  private void updateStorageDescriptorInBatch(Map<Long, StorageDescriptor> idToSd)
      throws MetaException {
    Map<Long, Long> sdIdToCdId = new HashMap<>();
    Map<Long, Long> sdIdToSerdeId = new HashMap<>();
    List<Long> cdIds = new ArrayList<>();
    List<Long> validSdIds = filterIdsByNonNullValue(new ArrayList<>(idToSd.keySet()), idToSd);
    Batchable.runBatched(maxBatchSize, validSdIds, new Batchable<Long, Void>() {
      @Override
      public List<Void> run(List<Long> input) throws Exception {
        String idLists = MetaStoreDirectSql.getIdListForIn(input);
        String queryText = "select \"SD_ID\", \"CD_ID\", \"SERDE_ID\" from \"SDS\" " +
            "where \"SD_ID\" in (" + idLists + ")";
        try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
          List<Object[]> sqlResult = executeWithArray(query.getInnerQuery(), null, queryText);
          for (Object[] row : sqlResult) {
            Long sdId = extractSqlLong(row[0]);
            Long cdId = extractSqlLong(row[1]);
            Long serdeId = extractSqlLong(row[2]);
            sdIdToCdId.put(sdId, cdId);
            sdIdToSerdeId.put(sdId, serdeId);
            cdIds.add(cdId);
          }
        }
        return null;
      }
    });

    Map<Long, Optional<Map<String, String>>> sdParamsOpt = new HashMap<>();
    Map<Long, List<String>> idToBucketCols = new HashMap<>();
    Map<Long, List<Order>> idToSortCols = new HashMap<>();
    Map<Long, SkewedInfo> idToSkewedInfo = new HashMap<>();
    Map<Long, List<FieldSchema>> sdIdToNewColumns = new HashMap<>();
    List<Long> serdeIds = new ArrayList<>();
    Map<Long, SerDeInfo> serdeIdToSerde = new HashMap<>();
    Map<Long, Optional<Map<String, String>>> serdeParamsOpt = new HashMap<>();
    for (Long sdId : validSdIds) {
      StorageDescriptor sd = idToSd.get(sdId);
      sdParamsOpt.put(sdId, Optional.ofNullable(sd.getParameters()));
      idToBucketCols.put(sdId, sd.getBucketCols());
      idToSortCols.put(sdId, sd.getSortCols());
      idToSkewedInfo.put(sdId, sd.getSkewedInfo());
      sdIdToNewColumns.put(sdId, sd.getCols());

      Long serdeId = sdIdToSerdeId.get(sdId);
      serdeIds.add(serdeId);
      serdeIdToSerde.put(serdeId, sd.getSerdeInfo());
      serdeParamsOpt.put(serdeId, Optional.ofNullable(sd.getSerdeInfo().getParameters()));
    }

    updateParamTableInBatch("\"SD_PARAMS\"", "\"SD_ID\"", validSdIds, sdParamsOpt);
    updateBucketColsInBatch(idToBucketCols, validSdIds);
    updateSortColsInBatch(idToSortCols, validSdIds);
    updateSkewedInfoInBatch(idToSkewedInfo, validSdIds);
    Map<Long, Long> sdIdToNewCdId = updateCDInBatch(cdIds, validSdIds, sdIdToCdId, sdIdToNewColumns);
    updateSerdeInBatch(serdeIds, serdeIdToSerde);
    updateParamTableInBatch("\"SERDE_PARAMS\"", "\"SERDE_ID\"", serdeIds, serdeParamsOpt);

    List<Long> cdIdsMayDelete = sdIdToCdId.entrySet().stream()
        .filter(entry -> sdIdToNewCdId.containsKey(entry.getKey()))
        .map(entry -> entry.getValue())
        .collect(Collectors.toList());

    // Update SDS table after CDS to get the freshest CD_ID values.
    sdIdToCdId.replaceAll((sdId, cdId) ->
        sdIdToNewCdId.containsKey(sdId) ? sdIdToNewCdId.get(sdId) : cdId);
    updateSDInBatch(validSdIds, idToSd, sdIdToCdId);

    List<Long> usedIds = Batchable.runBatched(maxBatchSize, cdIdsMayDelete,
        new Batchable<Long, Long>() {
          @Override
          public List<Long> run(List<Long> input) throws Exception {
            String idLists = MetaStoreDirectSql.getIdListForIn(input);
            String queryText = "select \"CD_ID\" from \"SDS\" where \"CD_ID\" in ( " + idLists + ")";
            try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
              List<Long> sqlResult = executeWithArray(query.getInnerQuery(), null, queryText);
              return new ArrayList<>(sqlResult);
            }
          }
    });
    List<Long> unusedCdIds = cdIdsMayDelete.stream().filter(id -> !usedIds.contains(id)).collect(Collectors.toList());

    deleteCDInBatch(unusedCdIds);
  }

  private void updateSDInBatch(List<Long> ids, Map<Long, StorageDescriptor> idToSd,
                               Map<Long, Long> idToCdId) throws MetaException {
    List<String> columns = Arrays.asList("\"CD_ID\"", "\"INPUT_FORMAT\"", "\"IS_COMPRESSED\"",
        "\"IS_STOREDASSUBDIRECTORIES\"", "\"LOCATION\"", "\"NUM_BUCKETS\"", "\"OUTPUT_FORMAT\"");
    List<String> conditionKeys = Arrays.asList("\"SD_ID\"");
    String stmt = TxnUtils.createUpdatePreparedStmt("\"SDS\"", columns, conditionKeys);
    int maxRows = dbType.getMaxRows(maxBatchSize, 8);
    updateWithStatement(statement -> Batchable.runBatched(maxRows, ids,
        new Batchable<Long, Void>() {
          @Override
          public List<Void> run(List<Long> input) throws SQLException {
            for (Long sdId : input) {
              StorageDescriptor sd = idToSd.get(sdId);
              statement.setLong(1, idToCdId.get(sdId));
              statement.setString(2, sd.getInputFormat());
              statement.setObject(3, dbType.getBoolean(sd.isCompressed()));
              statement.setObject(4, dbType.getBoolean(sd.isStoredAsSubDirectories()));
              statement.setString(5, sd.getLocation());
              statement.setInt(6, sd.getNumBuckets());
              statement.setString(7, sd.getOutputFormat());
              statement.setLong(8, sdId);
              statement.addBatch();
            }
            statement.executeBatch();
            return null;
          }
        }
    ), stmt);
  }

  private void updateBucketColsInBatch(Map<Long, List<String>> sdIdToBucketCols,
                                       List<Long> sdIds) throws MetaException {
    Batchable.runBatched(maxBatchSize, sdIds, new Batchable<Long, Void>() {
      @Override
      public List<Void> run(List<Long> input) throws MetaException {
        String idLists = MetaStoreDirectSql.getIdListForIn(input);
        String queryText = "delete from \"BUCKETING_COLS\" where \"SD_ID\" in (" + idLists + ")";
        updateWithStatement(PreparedStatement::executeUpdate, queryText);
        return null;
      }
    });
    List<String> columns = Arrays.asList("\"SD_ID\"", "\"INTEGER_IDX\"", "\"BUCKET_COL_NAME\"");
    String stmt = TxnUtils.createInsertPreparedStmt("\"BUCKETING_COLS\"", columns);
    List<Long> idWithBucketCols = filterIdsByNonNullValue(sdIds, sdIdToBucketCols);
    int maxRows = dbType.getMaxRows(maxBatchSize, 3);
    updateWithStatement(statement -> Batchable.runBatched(maxRows, idWithBucketCols, new Batchable<Long, Object>() {
      @Override
      public List<Object> run(List<Long> input) throws SQLException {
        for (Long id : input) {
          List<String> bucketCols = sdIdToBucketCols.get(id);
          for (int i = 0; i < bucketCols.size(); i++) {
            statement.setLong(1, id);
            statement.setInt(2, i);
            statement.setString(3, bucketCols.get(i));
            statement.addBatch();
          }
        }
        statement.executeBatch();
        return null;
      }
    }), stmt);
  }

  private void updateSortColsInBatch(Map<Long, List<Order>> sdIdToSortCols,
                                     List<Long> sdIds) throws MetaException {
    Batchable.runBatched(maxBatchSize, sdIds, new Batchable<Long, Void>() {
      @Override
      public List<Void> run(List<Long> input) throws MetaException {
        String idLists = MetaStoreDirectSql.getIdListForIn(input);
        String queryText = "delete from \"SORT_COLS\" where \"SD_ID\" in (" + idLists + ")";
        updateWithStatement(PreparedStatement::executeUpdate, queryText);
        return null;
      }
    });

    List<String> columns = Arrays.asList("\"SD_ID\"", "\"INTEGER_IDX\"", "\"COLUMN_NAME\"", "\"ORDER\"");
    String stmt = TxnUtils.createInsertPreparedStmt("\"SORT_COLS\"", columns);
    List<Long> idWithSortCols = filterIdsByNonNullValue(sdIds, sdIdToSortCols);
    int maxRows = dbType.getMaxRows(maxBatchSize, 4);
    updateWithStatement(statement -> Batchable.runBatched(maxRows, idWithSortCols, new Batchable<Long, Object>() {
      @Override
      public List<Object> run(List<Long> input) throws SQLException {
        for (Long id : input) {
          List<Order> bucketCols = sdIdToSortCols.get(id);
          for (int i = 0; i < bucketCols.size(); i++) {
            statement.setLong(1, id);
            statement.setInt(2, i);
            statement.setString(3, bucketCols.get(i).getCol());
            statement.setInt(4, bucketCols.get(i).getOrder());
            statement.addBatch();
          }
        }
        statement.executeBatch();
        return null;
      }
    }), stmt);
  }

  private void updateSkewedInfoInBatch(Map<Long, SkewedInfo> sdIdToSkewedInfo,
                                       List<Long> sdIds) throws MetaException {
    // Delete all mapping old stringLists and skewedValues,
    // skewedValues first for the foreign key constraint.
    List<Long> stringListId = getStringListId(sdIds);
    if (!stringListId.isEmpty()) {
      Batchable.runBatched(maxBatchSize, sdIds, new Batchable<Long, Void>() {
        @Override
        public List<Void> run(List<Long> input) throws Exception {
          String idLists = MetaStoreDirectSql.getIdListForIn(input);
          String deleteSkewValuesQuery =
              "delete from \"SKEWED_VALUES\" where \"SD_ID_OID\" in (" + idLists + ")";
          updateWithStatement(PreparedStatement::executeUpdate, deleteSkewValuesQuery);
          String deleteSkewColValueLocMapQuery =
              "delete from \"SKEWED_COL_VALUE_LOC_MAP\" where \"SD_ID\" in (" + idLists + ")";
          updateWithStatement(PreparedStatement::executeUpdate, deleteSkewColValueLocMapQuery);
          String deleteSkewColNamesQuery =
              "delete from \"SKEWED_COL_NAMES\" where \"SD_ID\" in (" + idLists + ")";
          updateWithStatement(PreparedStatement::executeUpdate, deleteSkewColNamesQuery);
          return null;
        }
      });
      Batchable.runBatched(maxBatchSize, stringListId, new Batchable<Long, Void>() {
        @Override
        public List<Void> run(List<Long> input) throws MetaException {
          String idLists = MetaStoreDirectSql.getIdListForIn(input);
          String deleteStringListValuesQuery =
              "delete from \"SKEWED_STRING_LIST_VALUES\" where \"STRING_LIST_ID\" in (" + idLists + ")";
          updateWithStatement(PreparedStatement::executeUpdate, deleteStringListValuesQuery);
          String deleteStringListQuery =
              "delete from \"SKEWED_STRING_LIST\" where \"STRING_LIST_ID\" in (" + idLists + ")";
          updateWithStatement(PreparedStatement::executeUpdate, deleteStringListQuery);
          return null;
        }
      });
    }

    // Generate new stringListId for each SdId
    Map<Long, List<String>> idToSkewedColNames = new HashMap<>();         // used for SKEWED_COL_NAMES
    List<Long> newStringListId = new ArrayList<>();                       // used for SKEWED_STRING_LIST
    Map<Long, List<String>> stringListIdToValues = new HashMap<>();       // used for SKEWED_STRING_LIST_VALUES
    Map<Long, List<Long>> sdIdToNewStringListId = new HashMap<>();        // used for SKEWED_VALUES
    Map<Long, List<Pair<Long, String>>> sdIdToValueLoc = new HashMap<>(); // used for SKEWED_COL_VALUE_LOC_MAP

    List<Long> idWithSkewedInfo = filterIdsByNonNullValue(sdIds, sdIdToSkewedInfo);
    for (Long sdId : idWithSkewedInfo) {
      SkewedInfo skewedInfo = sdIdToSkewedInfo.get(sdId);
      idToSkewedColNames.put(sdId, skewedInfo.getSkewedColNames());
      List<List<String>> skewedColValues = skewedInfo.getSkewedColValues();
      if (skewedColValues != null) {
        for (List<String> colValues : skewedColValues) {
          Long nextStringListId = getDataStoreId(MStringList.class);
          newStringListId.add(nextStringListId);
          sdIdToNewStringListId.computeIfAbsent(sdId, k -> new ArrayList<>()).add(nextStringListId);
          stringListIdToValues.put(nextStringListId, colValues);
        }
      }
      Map<List<String>, String> skewedColValueLocationMaps = skewedInfo.getSkewedColValueLocationMaps();
      if (skewedColValueLocationMaps != null) {
        for (Map.Entry<List<String>, String> entry : skewedColValueLocationMaps.entrySet()) {
          List<String> colValues = entry.getKey();
          String location = entry.getValue();
          Long nextStringListId = getDataStoreId(MStringList.class);
          newStringListId.add(nextStringListId);
          stringListIdToValues.put(nextStringListId, colValues);
          sdIdToValueLoc.computeIfAbsent(sdId, k -> new ArrayList<>()).add(Pair.of(nextStringListId, location));
        }
      }
    }

    insertSkewedColNamesInBatch(idToSkewedColNames, sdIds);
    insertStringListInBatch(newStringListId);
    insertStringListValuesInBatch(stringListIdToValues, newStringListId);
    insertSkewedValuesInBatch(sdIdToNewStringListId, sdIds);
    insertSkewColValueLocInBatch(sdIdToValueLoc, sdIds);
  }

  private Long getDataStoreId(Class<?> modelClass) throws MetaException {
    ExecutionContext ec = ((JDOPersistenceManager) pm).getExecutionContext();
    AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(modelClass, ec.getClassLoaderResolver());
    if (cmd.getIdentityType() == IdentityType.DATASTORE) {
      return (Long) ec.getStoreManager().getValueGenerationStrategyValue(ec, cmd, -1);
    } else {
      throw new MetaException("Identity type is not datastore.");
    }
  }

  private void insertSkewedColNamesInBatch(Map<Long, List<String>> sdIdToSkewedColNames,
                                           List<Long> sdIds) throws MetaException {
    List<String> columns = Arrays.asList("\"SD_ID\"", "\"INTEGER_IDX\"", "\"SKEWED_COL_NAME\"");
    String stmt = TxnUtils.createInsertPreparedStmt("\"SKEWED_COL_NAMES\"", columns);
    List<Long> idWithSkewedCols = filterIdsByNonNullValue(sdIds, sdIdToSkewedColNames);
    int maxRows = dbType.getMaxRows(maxBatchSize, 3);
    updateWithStatement(statement -> Batchable.runBatched(maxRows, idWithSkewedCols, new Batchable<Long, Object>() {
      @Override
      public List<Object> run(List<Long> input) throws SQLException {
        for (Long id : input) {
          List<String> skewedColNames = sdIdToSkewedColNames.get(id);
          for (int i = 0; i < skewedColNames.size(); i++) {
            statement.setLong(1, id);
            statement.setInt(2, i);
            statement.setString(3, skewedColNames.get(i));
            statement.addBatch();
          }
        }
        statement.executeBatch();
        return null;
      }
    }), stmt);
  }

  private void insertStringListInBatch(List<Long> stringListIds) throws MetaException {
    List<String> columns = Arrays.asList("\"STRING_LIST_ID\"");
    String insertQuery = TxnUtils.createInsertPreparedStmt("\"SKEWED_STRING_LIST\"", columns);
    int maxRows = dbType.getMaxRows(maxBatchSize, 1);
    updateWithStatement(statement -> Batchable.runBatched(maxRows, stringListIds,
        new Batchable<Long, Void>() {
          @Override
          public List<Void> run(List<Long> input) throws SQLException {
            for (Long id : input) {
              statement.setLong(1, id);
              statement.addBatch();
            }
            statement.executeBatch();
            return null;
          }
        }
    ), insertQuery);
  }

  private void insertStringListValuesInBatch(Map<Long, List<String>> stringListIdToValues,
                                             List<Long> stringListIds) throws MetaException {
    List<String> columns = Arrays.asList("\"STRING_LIST_ID\"", "\"INTEGER_IDX\"", "\"STRING_LIST_VALUE\"");
    String insertQuery = TxnUtils.createInsertPreparedStmt("\"SKEWED_STRING_LIST_VALUES\"", columns);
    List<Long> idWithStringList = filterIdsByNonNullValue(stringListIds, stringListIdToValues);
    int maxRows = dbType.getMaxRows(maxBatchSize, 3);
    updateWithStatement(statement -> Batchable.runBatched(maxRows, idWithStringList,
        new Batchable<Long, Void>() {
          @Override
          public List<Void> run(List<Long> input) throws SQLException {
            for (Long stringListId : input) {
              List<String> values = stringListIdToValues.get(stringListId);
              for (int i = 0; i < values.size(); i++) {
                statement.setLong(1, stringListId);
                statement.setInt(2, i);
                statement.setString(3, values.get(i));
                statement.addBatch();
              }
            }
            statement.executeBatch();
            return null;
          }
        }
    ), insertQuery);
  }

  private void insertSkewedValuesInBatch(Map<Long, List<Long>> sdIdToStringListId,
                                        List<Long> sdIds) throws MetaException {
    List<String> columns = Arrays.asList("\"SD_ID_OID\"", "\"INTEGER_IDX\"", "\"STRING_LIST_ID_EID\"");
    String insertQuery = TxnUtils.createInsertPreparedStmt("\"SKEWED_VALUES\"", columns);
    List<Long> idWithSkewedValues = filterIdsByNonNullValue(sdIds, sdIdToStringListId);
    int maxRows = dbType.getMaxRows(maxBatchSize, 3);
    updateWithStatement(statement -> Batchable.runBatched(maxRows, idWithSkewedValues,
        new Batchable<Long, Void>() {
          @Override
          public List<Void> run(List<Long> input) throws Exception {
            for (Long sdId : input) {
              List<Long> stringListIds = sdIdToStringListId.get(sdId);
              for (int i = 0; i < stringListIds.size(); i++) {
                statement.setLong(1, sdId);
                statement.setInt(2, i);
                statement.setLong(3, stringListIds.get(i));
                statement.addBatch();
              }
            }
            statement.executeBatch();
            return null;
          }
        }
    ), insertQuery);
  }

  private void insertSkewColValueLocInBatch(Map<Long, List<Pair<Long, String>>> sdIdToColValueLoc,
                                            List<Long> sdIds) throws MetaException {
    List<String> columns = Arrays.asList("\"SD_ID\"", "\"STRING_LIST_ID_KID\"", "\"LOCATION\"");
    String insertQuery = TxnUtils.createInsertPreparedStmt("\"SKEWED_COL_VALUE_LOC_MAP\"", columns);
    List<Long> idWithColValueLoc = filterIdsByNonNullValue(sdIds, sdIdToColValueLoc);
    int maxRows = dbType.getMaxRows(maxBatchSize, 3);
    updateWithStatement(statement -> Batchable.runBatched(maxRows, idWithColValueLoc,
        new Batchable<Long, Void>() {
          @Override
          public List<Void> run(List<Long> input) throws Exception {
            for (Long sdId : input) {
              List<Pair<Long, String>> stringListIdAndLoc = sdIdToColValueLoc.get(sdId);
              for (Pair<Long, String> pair : stringListIdAndLoc) {
                statement.setLong(1, sdId);
                statement.setLong(2, pair.getLeft());
                statement.setString(3, pair.getRight());
                statement.addBatch();
              }
            }
            statement.executeBatch();
            return null;
          }
        }
    ), insertQuery);
  }

  private Map<Long, Long> updateCDInBatch(List<Long> cdIds, List<Long> sdIds, Map<Long, Long> sdIdToCdId,
                                          Map<Long, List<FieldSchema>> sdIdToNewColumns) throws MetaException {
    Map<Long, List<Pair<Integer, FieldSchema>>> cdIdToColIdxPair = new HashMap<>();
    Batchable.runBatched(maxBatchSize, cdIds, new Batchable<Long, Void>() {
      @Override
      public List<Void> run(List<Long> input) throws Exception {
        String idLists = MetaStoreDirectSql.getIdListForIn(input);
        String queryText = "select \"CD_ID\", \"COMMENT\", \"COLUMN_NAME\", \"TYPE_NAME\", " +
            "\"INTEGER_IDX\" from \"COLUMNS_V2\" where \"CD_ID\" in (" + idLists + ")";
        try (QueryWrapper query = new QueryWrapper(pm.newQuery("javax.jdo.query.SQL", queryText))) {
          List<Object[]> sqlResult = executeWithArray(query.getInnerQuery(), null, queryText);
          for (Object[] row : sqlResult) {
            Long id = extractSqlLong(row[0]);
            String comment = extractSqlClob(row[1]);
            String name = extractSqlClob(row[2]);
            String type = extractSqlClob(row[3]);
            int index = extractSqlInt(row[4]);
            FieldSchema field = new FieldSchema(name, type, comment);
            cdIdToColIdxPair.computeIfAbsent(id, k -> new ArrayList<>()).add(Pair.of(index, field));
          }
        }
        return null;
      }
    });
    List<Long> newCdIds = new ArrayList<>();
    Map<Long, List<FieldSchema>> newCdIdToCols = new HashMap<>();
    Map<Long, Long> oldCdIdToNewCdId = new HashMap<>();
    Map<Long, Long> sdIdToNewCdId = new HashMap<>();
    // oldCdId -> [(oldIdx, newIdx)], used to update KEY_CONSTRAINTS
    Map<Long, List<Pair<Integer, Integer>>> oldCdIdToColIdxPairs = new HashMap<>();
    for (Long sdId : sdIds) {
      Long cdId = sdIdToCdId.get(sdId);
      List<Pair<Integer, FieldSchema>> cols = cdIdToColIdxPair.get(cdId);
      // Placeholder to avoid IndexOutOfBoundsException.
      List<FieldSchema> oldCols = new ArrayList<>(Collections.nCopies(cols.size(), null));
      cols.forEach(pair -> oldCols.set(pair.getLeft(), pair.getRight()));

      List<FieldSchema> newCols = sdIdToNewColumns.get(sdId);
      // Use the new column descriptor only if the old column descriptor differs from the new one.
      if (oldCols == null || !oldCols.equals(newCols)) {
        if (oldCols != null && newCols != null) {
          Long newCdId = getDataStoreId(MColumnDescriptor.class);
          newCdIds.add(newCdId);
          newCdIdToCols.put(newCdId, newCols);
          oldCdIdToNewCdId.put(cdId, newCdId);
          sdIdToNewCdId.put(sdId, newCdId);
          for (int i = 0; i < oldCols.size(); i++) {
            FieldSchema oldCol = oldCols.get(i);
            int newIdx = newCols.indexOf(oldCol);
            if (newIdx != -1) {
              oldCdIdToColIdxPairs.computeIfAbsent(cdId, k -> new ArrayList<>()).add(Pair.of(i, newIdx));
            }
          }
        }
      }
    }

    insertCDInBatch(newCdIds, newCdIdToCols);
    // TODO: followed the jdo implement now, but it should be an error in such case:
    //       partitions use the default table cd, when changing partition cd with
    //       constraint key mapping, the constraints will be update unexpected.
    updateKeyConstraintsInBatch(oldCdIdToNewCdId, oldCdIdToColIdxPairs);

    return sdIdToNewCdId;
  }

  private void insertCDInBatch(List<Long> ids, Map<Long, List<FieldSchema>> idToCols)
      throws MetaException {
    String insertCds = TxnUtils.createInsertPreparedStmt("\"CDS\"", Arrays.asList("\"CD_ID\""));
    int maxRows = dbType.getMaxRows(maxBatchSize, 1);
    updateWithStatement(statement -> Batchable.runBatched(maxRows, ids,
        new Batchable<Long, Void>() {
          @Override
          public List<Void> run(List<Long> input) throws SQLException {
            for (Long id : input) {
              statement.setLong(1, id);
              statement.addBatch();
            }
            statement.executeBatch();
            return null;
          }
    }), insertCds);

    List<String> columns = Arrays.asList("\"CD_ID\"",
        "\"COMMENT\"", "\"COLUMN_NAME\"", "\"TYPE_NAME\"", "\"INTEGER_IDX\"");
    String insertColumns = TxnUtils.createInsertPreparedStmt("\"COLUMNS_V2\"", columns);
    int maxRowsForCDs = dbType.getMaxRows(maxBatchSize, 5);
    updateWithStatement(statement -> Batchable.runBatched(maxRowsForCDs, ids,
        new Batchable<Long, Void>() {
          @Override
          public List<Void> run(List<Long> input) throws Exception {
            for (Long id : input) {
              List<FieldSchema> cols = idToCols.get(id);
              for (int i = 0; i < cols.size(); i++) {
                FieldSchema col = cols.get(i);
                statement.setLong(1, id);
                statement.setString(2, col.getComment());
                statement.setString(3, col.getName());
                statement.setString(4, col.getType());
                statement.setInt(5, i);
                statement.addBatch();
              }
            }
            statement.executeBatch();
            return null;
          }
    }), insertColumns);
  }

  private void updateKeyConstraintsInBatch(Map<Long, Long> oldCdIdToNewCdId,
                                           Map<Long, List<Pair<Integer, Integer>>> oldCdIdToColIdxPairs) throws MetaException {
    List<Long> oldCdIds = new ArrayList<>(oldCdIdToNewCdId.keySet());
    String tableName = "\"KEY_CONSTRAINTS\"";
    List<String> parentColumns = Arrays.asList("\"PARENT_CD_ID\"", "\"PARENT_INTEGER_IDX\"");
    List<String> childColumns = Arrays.asList("\"CHILD_CD_ID\"", "\"CHILD_INTEGER_IDX\"");

    String updateParent = TxnUtils.createUpdatePreparedStmt(tableName, parentColumns, parentColumns);
    String updateChild = TxnUtils.createUpdatePreparedStmt(tableName, childColumns, childColumns);
    for (String updateStmt : new String[]{updateParent, updateChild}) {
      int maxRows = dbType.getMaxRows(maxBatchSize, 4);
      updateWithStatement(statement -> Batchable.runBatched(maxRows, oldCdIds,
          new Batchable<Long, Void>() {
            @Override
            public List<Void> run(List<Long> input) throws SQLException {
              for (Long oldId : input) {
                // Followed the jdo implement to update only mapping columns for KEY_CONSTRAINTS.
                if (!oldCdIdToColIdxPairs.containsKey(oldId)) {
                  continue;
                }
                Long newId = oldCdIdToNewCdId.get(oldId);
                for (Pair<Integer, Integer> idx : oldCdIdToColIdxPairs.get(oldId)) {
                  statement.setLong(1, newId);
                  statement.setInt(2, idx.getRight());
                  statement.setLong(3, oldId);
                  statement.setInt(4, idx.getLeft());
                  statement.addBatch();
                }
              }
              statement.executeBatch();
              return null;
            }
      }), updateStmt);
    }
  }

  private void deleteCDInBatch(List<Long> cdIds) throws MetaException {
    Batchable.runBatched(maxBatchSize, cdIds, new Batchable<Long, Void>() {
      @Override
      public List<Void> run(List<Long> input) throws Exception {
        String idLists = MetaStoreDirectSql.getIdListForIn(input);
        // First remove any constraints that may be associated with these CDs
        String deleteConstraintsByCd = "delete from \"KEY_CONSTRAINTS\" where \"CHILD_CD_ID\" in ("
            + idLists + ") or \"PARENT_CD_ID\" in (" + idLists + ")";
        updateWithStatement(PreparedStatement::executeUpdate, deleteConstraintsByCd);

        // Then delete COLUMNS_V2 before CDS for foreign constraints.
        String deleteColumns = "delete from \"COLUMNS_V2\" where \"CD_ID\" in (" + idLists + ")";
        updateWithStatement(PreparedStatement::executeUpdate, deleteColumns);

        // Finally delete CDS
        String deleteCDs = "delete from \"CDS\" where \"CD_ID\" in (" + idLists + ")";
        updateWithStatement(PreparedStatement::executeUpdate, deleteCDs);
        return null;
      }
    });
  }

  private void updateSerdeInBatch(List<Long> ids, Map<Long, SerDeInfo> idToSerde)
      throws MetaException {
    // Followed the jdo implement to update only NAME and SLIB of SERDES.
    List<String> columns = Arrays.asList("\"NAME\"", "\"SLIB\"");
    List<String> condKeys = Arrays.asList("\"SERDE_ID\"");
    String updateStmt = TxnUtils.createUpdatePreparedStmt("\"SERDES\"", columns, condKeys);
    List<Long> idWithSerde = filterIdsByNonNullValue(ids, idToSerde);
    int maxRows = dbType.getMaxRows(maxBatchSize, 3);
    updateWithStatement(statement -> Batchable.runBatched(maxRows, idWithSerde,
        new Batchable<Long, Void>() {
          @Override
          public List<Void> run(List<Long> input) throws SQLException {
            for (Long id : input) {
              SerDeInfo serde = idToSerde.get(id);
              statement.setString(1, serde.getName());
              statement.setString(2, serde.getSerializationLib());
              statement.setLong(3, id);
              statement.addBatch();
            }
            statement.executeBatch();
            return null;
          }
    }), updateStmt);
  }

  private static final class PartitionInfo {
    long partitionId;
    long writeId;
    String partitionName;
    public PartitionInfo(long partitionId, long writeId, String partitionName) {
      this.partitionId = partitionId;
      this.writeId = writeId;
      this.partitionName = partitionName;
    }

    @Override
    public int hashCode() {
      return (int)partitionId;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null) {
        return false;
      }
      if (!(o instanceof PartitionInfo)) {
        return false;
      }
      PartitionInfo other = (PartitionInfo)o;
      if (this.partitionId != other.partitionId) {
        return false;
      }
      return true;
    }
  }

  private static final class PartColNameInfo {
    long partitionId;
    String colName;
    String engine;
    public PartColNameInfo(long partitionId, String colName, String engine) {
      this.partitionId = partitionId;
      this.colName = colName;
      this.engine = engine;
    }

    @Override
    public int hashCode() {
      return (int)partitionId;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null) {
        return false;
      }
      if (!(o instanceof PartColNameInfo)) {
        return false;
      }
      PartColNameInfo other = (PartColNameInfo)o;
      if (this.partitionId != other.partitionId) {
        return false;
      }
      if (!this.colName.equalsIgnoreCase(other.colName)) {
        return false;
      }
      return Objects.equals(this.engine, other.engine);
    }
  }
}
