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
package org.apache.hadoop.hive.metastore.txn.jdbc.queries;

import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionState;
import org.apache.hadoop.hive.metastore.txn.MetaWrapperException;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class ShowCompactHandler implements QueryHandler<ShowCompactResponse> {
  
  private static final String DEFAULT_POOL_NAME = "default";

  //language=SQL
  private static final String SHOW_COMPACTION_QUERY =
      " XX.* FROM ( SELECT " +
          "  \"CQ_DATABASE\" AS \"CC_DATABASE\", \"CQ_TABLE\" AS \"CC_TABLE\", \"CQ_PARTITION\" AS \"CC_PARTITION\", " +
          "  \"CQ_STATE\" AS \"CC_STATE\", \"CQ_TYPE\" AS \"CC_TYPE\", \"CQ_WORKER_ID\" AS \"CC_WORKER_ID\", " +
          "  \"CQ_START\" AS \"CC_START\", -1 \"CC_END\", \"CQ_RUN_AS\" AS \"CC_RUN_AS\", " +
          "  \"CQ_HADOOP_JOB_ID\" AS \"CC_HADOOP_JOB_ID\", \"CQ_ID\" AS \"CC_ID\", \"CQ_ERROR_MESSAGE\" AS \"CC_ERROR_MESSAGE\", " +
          "  \"CQ_ENQUEUE_TIME\" AS \"CC_ENQUEUE_TIME\", \"CQ_WORKER_VERSION\" AS \"CC_WORKER_VERSION\", " +
          "  \"CQ_INITIATOR_ID\" AS \"CC_INITIATOR_ID\", \"CQ_INITIATOR_VERSION\" AS \"CC_INITIATOR_VERSION\", " +
          "  \"CQ_CLEANER_START\" AS \"CC_CLEANER_START\", \"CQ_POOL_NAME\" AS \"CC_POOL_NAME\", \"CQ_TXN_ID\" AS \"CC_TXN_ID\", " +
          "  \"CQ_NEXT_TXN_ID\" AS \"CC_NEXT_TXN_ID\", \"CQ_COMMIT_TIME\" AS \"CC_COMMIT_TIME\", " +
          "  \"CQ_HIGHEST_WRITE_ID\" AS \"CC_HIGHEST_WRITE_ID\" " +
          "FROM " +
          "  \"COMPACTION_QUEUE\" " +
          "UNION ALL " +
          "SELECT " +
          "  \"CC_DATABASE\" , \"CC_TABLE\", \"CC_PARTITION\", \"CC_STATE\", \"CC_TYPE\", \"CC_WORKER_ID\", " +
          "  \"CC_START\", \"CC_END\", \"CC_RUN_AS\", \"CC_HADOOP_JOB_ID\", \"CC_ID\", \"CC_ERROR_MESSAGE\", " +
          "  \"CC_ENQUEUE_TIME\", \"CC_WORKER_VERSION\", \"CC_INITIATOR_ID\", \"CC_INITIATOR_VERSION\", " +
          "   -1 , \"CC_POOL_NAME\", \"CC_TXN_ID\", \"CC_NEXT_TXN_ID\", \"CC_COMMIT_TIME\", " +
          "  \"CC_HIGHEST_WRITE_ID\"" +
          "FROM " +
          "  \"COMPLETED_COMPACTIONS\" ) XX " +
          "WHERE " +
          "  (\"CC_ID\" = :id OR :id IS NULL) AND " +
          "  (\"CC_DATABASE\" = :dbName OR :dbName IS NULL) AND " +
          "  (\"CC_TABLE\" = :tableName OR :tableName IS NULL) AND " +
          "  (\"CC_PARTITION\" = :partition OR :partition IS NULL) AND " +
          "  (\"CC_STATE\" = :state OR :state IS NULL) AND " +
          "  (\"CC_TYPE\" = :type OR :type IS NULL) AND " +
          "  (\"CC_POOL_NAME\" = :poolName OR :poolName IS NULL)";

  //language=SQL
  private static final String SHOW_COMPACTION_ORDERBY_CLAUSE =
      " ORDER BY CASE " +
          "   WHEN \"CC_END\" > \"CC_START\" and \"CC_END\" > \"CC_COMMIT_TIME\" " +
          "     THEN \"CC_END\" " +
          "   WHEN \"CC_START\" > \"CC_COMMIT_TIME\" " +
          "     THEN \"CC_START\" " +
          "   ELSE \"CC_COMMIT_TIME\" " +
          " END desc ," +
          " \"CC_ENQUEUE_TIME\" asc";


  private final ShowCompactRequest request;
  private final SQLGenerator sqlGenerator;


  public ShowCompactHandler(ShowCompactRequest request, SQLGenerator sqlGenerator) {
    this.request = request;
    this.sqlGenerator = sqlGenerator;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    String noSelectQuery = SHOW_COMPACTION_QUERY + getShowCompactSortingOrderClause(request);
    int rowLimit = (int) request.getLimit();    
    if (rowLimit > 0) {
      return sqlGenerator.addLimitClause(rowLimit, noSelectQuery);
    } else {
    }
    return "SELECT " + noSelectQuery;
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    Long id = request.getId() > 0 ? request.getId() : null;
    try {
      return new MapSqlParameterSource()
          .addValue("id", id, Types.BIGINT)
          .addValue("dbName", request.getDbName(), Types.VARCHAR)
          .addValue("tableName", request.getTbName(), Types.VARCHAR)
          .addValue("partition", request.getPartName(), Types.VARCHAR)
          .addValue("state", request.getState(), Types.CHAR)
          .addValue("type", request.getType() == null ? null : Character.toString(TxnUtils.thriftCompactionType2DbType(request.getType())), Types.CHAR)
          .addValue("poolName", request.getPoolName(), Types.VARCHAR);
    } catch (MetaException e) {
      throw new MetaWrapperException(e);
    }
  }

  @Override
  public ShowCompactResponse extractData(ResultSet rs) throws SQLException, DataAccessException {
    ShowCompactResponse response = new ShowCompactResponse(new ArrayList<>());
    while (rs.next()) {
      ShowCompactResponseElement e = new ShowCompactResponseElement();
      e.setDbname(rs.getString(1));
      e.setTablename(rs.getString(2));
      e.setPartitionname(rs.getString(3));
      e.setState(CompactionState.fromSqlConst(rs.getString(4)).toString());
      try {
        e.setType(TxnUtils.dbCompactionType2ThriftType(rs.getString(5).charAt(0)));
      } catch (SQLException ex) {
        //do nothing to handle RU/D if we add another status
      }
      e.setWorkerid(rs.getString(6));
      long start = rs.getLong(7);
      if (!rs.wasNull()) {
        e.setStart(start);
      }
      long endTime = rs.getLong(8);
      if (endTime != -1) {
        e.setEndTime(endTime);
      }
      e.setRunAs(rs.getString(9));
      e.setHadoopJobId(rs.getString(10));
      e.setId(rs.getLong(11));
      e.setErrorMessage(rs.getString(12));
      long enqueueTime = rs.getLong(13);
      if (!rs.wasNull()) {
        e.setEnqueueTime(enqueueTime);
      }
      e.setWorkerVersion(rs.getString(14));
      e.setInitiatorId(rs.getString(15));
      e.setInitiatorVersion(rs.getString(16));
      long cleanerStart = rs.getLong(17);
      if (!rs.wasNull() && (cleanerStart != -1)) {
        e.setCleanerStart(cleanerStart);
      }
      String poolName = rs.getString(18);
      if (isBlank(poolName)) {
        e.setPoolName(DEFAULT_POOL_NAME);
      } else {
        e.setPoolName(poolName);
      }
      e.setTxnId(rs.getLong(19));
      e.setNextTxnId(rs.getLong(20));
      e.setCommitTime(rs.getLong(21));
      e.setHightestTxnId(rs.getLong(22));
      response.addToCompacts(e);      
    }
    return response;
  }

  private String getShowCompactSortingOrderClause(ShowCompactRequest request) {
    String sortingOrder = request.getOrder();
    return isNotBlank(sortingOrder) ? "  ORDER BY  " + sortingOrder : SHOW_COMPACTION_ORDERBY_CLAUSE;
  }
  
}
