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

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.events.UpdatePartitionColumnStatEvent;
import org.apache.hadoop.hive.metastore.events.UpdatePartitionColumnStatEventBatch;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.common.StatsSetupConst.COLUMN_STATS_ACCURATE;
import static org.apache.hadoop.hive.metastore.HMSHandler.getPartValsFromName;

/**
 * This class contains the optimizations for MetaStore that rely on direct SQL access to
 * the underlying database. It should use ANSI SQL and be compatible with common databases
 * such as MySQL (note that MySQL doesn't use full ANSI mode by default), Postgres, etc.
 *
 * This class separates out the statistics update part from MetaStoreDirectSql class.
 */
class DirectSqlUpdateStat {
  private static final Logger LOG = LoggerFactory.getLogger(DirectSqlUpdateStat.class.getName());
  PersistenceManager pm;
  Configuration conf;
  DatabaseProduct dbType;
  int maxBatchSize;
  SQLGenerator sqlGenerator;
  private static final ReentrantLock derbyLock = new ReentrantLock(true);
  
  public DirectSqlUpdateStat(PersistenceManager pm, Configuration conf,
                                          DatabaseProduct dbType, int batchSize) {
    this.pm = pm;
    this.conf = conf;
    this.dbType = dbType;
    this.maxBatchSize = batchSize;
    sqlGenerator = new SQLGenerator(dbType, conf);
  }

  /**
   * {@link #lockInternal()} and {@link #unlockInternal()} are used to serialize those operations that require
   * Select ... For Update to sequence operations properly.  In practice that means when running
   * with Derby database.  See more notes at class level.
   */
  private void lockInternal() {
    if(dbType.isDERBY()) {
      derbyLock.lock();
    }
  }

  private void unlockInternal() {
    if(dbType.isDERBY()) {
      derbyLock.unlock();
    }
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

  void closeStmt(Statement stmt) {
    try {
      if (stmt != null && !stmt.isClosed()) stmt.close();
    } catch (SQLException e) {
      LOG.warn("Failed to close statement ", e);
    }
  }

  void close(ResultSet rs) {
    try {
      if (rs != null && !rs.isClosed()) {
        rs.close();
      }
    }
    catch(SQLException ex) {
      LOG.warn("Failed to close statement ", ex);
    }
  }

  static String quoteString(String input) {
    return "'" + input + "'";
  }

  void close(ResultSet rs, Statement stmt, JDOConnection dbConn) {
    close(rs);
    closeStmt(stmt);
    closeDbConn(dbConn);
  }

  private void populateInsertUpdateMap(Map<PartitionInfo, ColumnStatistics> statsPartInfoMap,
                                       Map<PartColNameInfo, MPartitionColumnStatistics> updateMap,
                                       Map<PartColNameInfo, MPartitionColumnStatistics>insertMap,
                                       Connection dbConn) throws SQLException, MetaException, NoSuchObjectException {
    StringBuilder prefix = new StringBuilder();
    StringBuilder suffix = new StringBuilder();
    Statement statement = null;
    ResultSet rs = null;
    List<String> queries = new ArrayList<>();
    Set<PartColNameInfo> selectedParts = new HashSet<>();

    List<Long> partIdList = statsPartInfoMap.keySet().stream().map(
            e -> e.partitionId).collect(Collectors.toList()
    );

    prefix.append("select \"PART_ID\", \"COLUMN_NAME\" from \"PART_COL_STATS\" WHERE ");
    TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix,
            partIdList, "\"PART_ID\"", true, false);

    for (String query : queries) {
      try {
        statement = dbConn.createStatement();
        LOG.debug("Going to execute query " + query);
        rs = statement.executeQuery(query);
        while (rs.next()) {
          selectedParts.add(new PartColNameInfo(rs.getLong(1), rs.getString(2)));
        }
      } finally {
        close(rs, statement, null);
      }
    }

    for (Map.Entry entry : statsPartInfoMap.entrySet()) {
      PartitionInfo partitionInfo = (PartitionInfo) entry.getKey();
      ColumnStatistics colStats = (ColumnStatistics) entry.getValue();
      long partId = partitionInfo.partitionId;
      ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();
      for (ColumnStatisticsObj statisticsObj : colStats.getStatsObj()) {
        PartColNameInfo temp = new PartColNameInfo(partId, statisticsObj.getColName());
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
    PreparedStatement pst = null;
    for (Map.Entry entry : updateMap.entrySet()) {
      PartColNameInfo partColNameInfo = (PartColNameInfo) entry.getKey();
      Long partId = partColNameInfo.partitionId;
      MPartitionColumnStatistics mPartitionColumnStatistics = (MPartitionColumnStatistics) entry.getValue();
      String update = "UPDATE \"PART_COL_STATS\" SET ";
      update += StatObjectConverter.getUpdatedColumnSql(mPartitionColumnStatistics);
      update += " WHERE \"PART_ID\" = " + partId + " AND "
              + " \"COLUMN_NAME\" = " +  quoteString(mPartitionColumnStatistics.getColName());
      try {
        pst = dbConn.prepareStatement(update);
        StatObjectConverter.initUpdatedColumnStatement(mPartitionColumnStatistics, pst);
        LOG.info("Going to execute update " + update);
        int numUpdate = pst.executeUpdate();
        if (numUpdate != 1) {
          throw new MetaException("Invalid state of  PART_COL_STATS for PART_ID " + partId);
        }
      } finally {
        closeStmt(pst);
      }
    }
  }

  private void insertIntoPartColStatTable(Map<PartColNameInfo, MPartitionColumnStatistics> insertMap,
                                          long maxCsId,
                                          Connection dbConn) throws SQLException, MetaException, NoSuchObjectException {
    PreparedStatement preparedStatement = null;
    int numRows = 0;
    String insert = "INSERT INTO \"PART_COL_STATS\" (\"CS_ID\", \"CAT_NAME\", \"DB_NAME\","
            + "\"TABLE_NAME\", \"PARTITION_NAME\", \"COLUMN_NAME\", \"COLUMN_TYPE\", \"PART_ID\","
            + " \"LONG_LOW_VALUE\", \"LONG_HIGH_VALUE\", \"DOUBLE_HIGH_VALUE\", \"DOUBLE_LOW_VALUE\","
            + " \"BIG_DECIMAL_LOW_VALUE\", \"BIG_DECIMAL_HIGH_VALUE\", \"NUM_NULLS\", \"NUM_DISTINCTS\", \"BIT_VECTOR\" ,"
            + " \"AVG_COL_LEN\", \"MAX_COL_LEN\", \"NUM_TRUES\", \"NUM_FALSES\", \"LAST_ANALYZED\", \"ENGINE\") values "
            + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    try {
      preparedStatement = dbConn.prepareStatement(insert);
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
        preparedStatement.setObject(18, mPartitionColumnStatistics.getAvgColLen());
        preparedStatement.setObject(19, mPartitionColumnStatistics.getMaxColLen());
        preparedStatement.setObject(20, mPartitionColumnStatistics.getNumTrues());
        preparedStatement.setObject(21, mPartitionColumnStatistics.getNumFalses());
        preparedStatement.setLong(22, mPartitionColumnStatistics.getLastAnalyzed());
        preparedStatement.setString(23, mPartitionColumnStatistics.getEngine());

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
    } finally {
      closeStmt(preparedStatement);
    }
  }

  private Map<Long, String> getParamValues(Connection dbConn, List<Long> partIdList) throws SQLException {
    List<String> queries = new ArrayList<>();
    StringBuilder prefix = new StringBuilder();
    StringBuilder suffix = new StringBuilder();
    Statement statement = null;
    ResultSet rs = null;

    prefix.append("select \"PART_ID\", \"PARAM_VALUE\" "
            + " from \"PARTITION_PARAMS\" where "
            + " \"PARAM_KEY\" = 'COLUMN_STATS_ACCURATE' "
            + " and ");
    TxnUtils.buildQueryWithINClause(conf, queries, prefix, suffix,
            partIdList, "\"PART_ID\"", true, false);

    Map<Long, String> partIdToParaMap = new HashMap<>();
    for (String query : queries) {
      try {
        statement = dbConn.createStatement();
        LOG.debug("Going to execute query " + query);
        rs = statement.executeQuery(query);
        while (rs.next()) {
          partIdToParaMap.put(rs.getLong(1), rs.getString(2));
        }
      } finally {
        close(rs, statement, null);
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

    Statement statement = null;
    for (String query : queries) {
      try {
        statement = dbConn.createStatement();
        LOG.debug("Going to execute update " + query);
        statement.executeUpdate(query);
      } finally {
        closeStmt(statement);
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
    PreparedStatement statementInsert = null;
    PreparedStatement statementDelete = null;
    PreparedStatement statementUpdate = null;
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

    try {
      statementInsert = dbConn.prepareStatement(insert);
      statementDelete = dbConn.prepareStatement(delete);
      statementUpdate = dbConn.prepareStatement(update);
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
    } finally {
      closeStmt(statementInsert);
      closeStmt(statementUpdate);
      closeStmt(statementDelete);
    }
  }

  private static class PartitionInfo {
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

  private static class PartColNameInfo {
    long partitionId;
    String colName;
    public PartColNameInfo(long partitionId, String colName) {
      this.partitionId = partitionId;
      this.colName = colName;
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
      if (this.colName.equalsIgnoreCase(other.colName)) {
        return true;
      }
      return false;
    }
  }

  private Map<PartitionInfo, ColumnStatistics> getPartitionInfo(Connection dbConn, long tblId,
                                                                 Map<String, ColumnStatistics> partColStatsMap)
          throws SQLException, MetaException {
    List<String> queries = new ArrayList<>();
    StringBuilder prefix = new StringBuilder();
    StringBuilder suffix = new StringBuilder();
    Statement statement = null;
    ResultSet rs = null;
    Map<PartitionInfo, ColumnStatistics> partitionInfoMap = new HashMap<>();

    List<String> partKeys = partColStatsMap.keySet().stream().map(
            e -> quoteString(e)).collect(Collectors.toList()
    );

    prefix.append("select \"PART_ID\", \"WRITE_ID\", \"PART_NAME\"  from \"PARTITIONS\" where ");
    suffix.append(" and  \"TBL_ID\" = " + tblId);
    TxnUtils.buildQueryWithINClauseStrings(conf, queries, prefix, suffix,
            partKeys, "\"PART_NAME\"", true, false);

    for (String query : queries) {
      // Select for update makes sure that the partitions are not modified while the stats are getting updated.
      query = sqlGenerator.addForUpdateClause(query);
      try {
        statement = dbConn.createStatement();
        LOG.debug("Going to execute query <" + query + ">");
        rs = statement.executeQuery(query);
        while (rs.next()) {
          PartitionInfo partitionInfo = new PartitionInfo(rs.getLong(1),
                  rs.getLong(2), rs.getString(3));
          partitionInfoMap.put(partitionInfo, partColStatsMap.get(rs.getString(3)));
        }
      } finally {
        close(rs, statement, null);
      }
    }
    return partitionInfoMap;
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
      lockInternal();
      jdoConn = pm.getDataStoreConnection();
      dbConn = (Connection) (jdoConn.getNativeConnection());

      Map<PartitionInfo, ColumnStatistics> partitionInfoMap = getPartitionInfo(dbConn, tbl.getId(), partColStatsMap);

      Map<String, Map<String, String>> result =
              updatePartitionParamTable(dbConn, partitionInfoMap, validWriteIds, writeId, TxnUtils.isAcidTable(tbl));

      Map<PartColNameInfo, MPartitionColumnStatistics> insertMap = new HashMap<>();
      Map<PartColNameInfo, MPartitionColumnStatistics> updateMap = new HashMap<>();
      populateInsertUpdateMap(partitionInfoMap, updateMap, insertMap, dbConn);

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
      unlockInternal();
    }
  }

  /**
   * Gets the next CS id from sequence MPartitionColumnStatistics and increment the CS id by numStats.
   * @return The CD id before update.
   */
  public long getNextCSIdForMPartitionColumnStatistics(long numStats) throws MetaException {
    Statement statement = null;
    ResultSet rs = null;
    long maxCsId = 0;
    boolean committed = false;
    Connection dbConn = null;
    JDOConnection jdoConn = null;

    try {
      lockInternal();
      jdoConn = pm.getDataStoreConnection();
      dbConn = (Connection) (jdoConn.getNativeConnection());

      // This loop will be iterated at max twice. If there is no records, it will first insert and then do a select.
      // We are not using any upsert operations as select for update and then update is required to make sure that
      // the caller gets a reserved range for CSId not used by any other thread.
      boolean insertDone = false;
      while (maxCsId == 0) {
        String query = "SELECT \"NEXT_VAL\" FROM \"SEQUENCE_TABLE\" WHERE \"SEQUENCE_NAME\"= "
                + quoteString("org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics")
                + " FOR UPDATE";
        LOG.debug("Going to execute query " + query);
        statement = dbConn.createStatement();
        rs = statement.executeQuery(query);
        if (rs.next()) {
          maxCsId = rs.getLong(1);
        } else if (insertDone) {
          throw new MetaException("Invalid state of SEQUENCE_TABLE for MPartitionColumnStatistics");
        } else {
          insertDone = true;
          closeStmt(statement);
          statement = dbConn.createStatement();
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
          } finally {
            closeStmt(statement);
          }
        }
      }

      long nextMaxCsId = maxCsId + numStats + 1;
      closeStmt(statement);
      statement = dbConn.createStatement();
      String query = "UPDATE \"SEQUENCE_TABLE\" SET \"NEXT_VAL\" = "
              + nextMaxCsId
              + " WHERE \"SEQUENCE_NAME\" = "
              + quoteString("org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics");
      statement.executeUpdate(query);
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
      close(rs, statement, jdoConn);
      unlockInternal();
    }
  }
}
