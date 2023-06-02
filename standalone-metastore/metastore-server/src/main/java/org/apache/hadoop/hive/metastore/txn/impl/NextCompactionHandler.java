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
package org.apache.hadoop.hive.metastore.txn.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.FindNextCompactRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.retryhandling.QueryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import static org.apache.hadoop.hive.metastore.txn.TxnStore.INITIATED_STATE;
import static org.apache.hadoop.hive.metastore.txn.TxnStore.WORKING_STATE;
import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;

public class NextCompactionHandler implements QueryHandler<CompactionInfo> {

  private static final Logger LOG = LoggerFactory.getLogger(NextCompactionHandler.class);

  private final FindNextCompactRequest request;
  private final NamedParameterJdbcTemplate jdbcTemplate;
  private final Timestamp currentDbTime;
  private final long poolTimeout;

  private static final String updateStatement =
      "UPDATE \"COMPACTION_QUEUE\" " +
          "SET " +
          " \"CQ_WORKER_ID\" = :workerId, " +
          " \"CQ_WORKER_VERSION\" = :workerVersion, " +
          " \"CQ_START\" = :now, " +
          " \"CQ_STATE\" = :newState " +
          "WHERE \"CQ_ID\" = :id AND \"CQ_STATE\"= :oldState";

  public NextCompactionHandler(FindNextCompactRequest request, NamedParameterJdbcTemplate jdbcTemplate, Timestamp currentDbTime, long poolTimeout) {
    this.request = request;
    this.jdbcTemplate = jdbcTemplate;
    this.currentDbTime = currentDbTime;
    this.poolTimeout = poolTimeout;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT \"CQ_ID\", \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\", " +
        "\"CQ_TYPE\", \"CQ_POOL_NAME\", \"CQ_NUMBER_OF_BUCKETS\", \"CQ_ORDER_BY\", " +
        "\"CQ_TBLPROPERTIES\" FROM \"COMPACTION_QUEUE\" WHERE \"CQ_STATE\" = :state AND ");
    boolean hasPoolName = StringUtils.isNotBlank(request.getPoolName());
    if(hasPoolName) {
      sb.append("\"CQ_POOL_NAME\"= :poolName");
    } else {
      sb.append("\"CQ_POOL_NAME\" IS NULL OR  \"CQ_ENQUEUE_TIME\" < (")
          .append(getEpochFn(databaseProduct)).append(" - ").append(poolTimeout).append(")");
    }
    sb.append(" ORDER BY \"CQ_ID\" ASC");
    return sb.toString();
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    MapSqlParameterSource params = new MapSqlParameterSource()
        .addValue("state", Character.toString(INITIATED_STATE), Types.CHAR);
    if(StringUtils.isNotBlank(request.getPoolName())) {
      params.addValue("poolName", request.getPoolName());
    }
    return params;
  }

  @Override
  public CompactionInfo extractData(ResultSet rs) throws SQLException, DataAccessException {
    CompactionInfo info;
    while (rs.next()){
      info = new CompactionInfo();
      info.id = rs.getLong(1);
      info.dbname = rs.getString(2);
      info.tableName = rs.getString(3);
      info.partName = rs.getString(4);
      info.type = TxnUtils.dbCompactionType2ThriftType(rs.getString(5).charAt(0));
      info.poolName = rs.getString(6);
      info.numberOfBuckets = rs.getInt(7);
      info.orderByClause = rs.getString(8);
      info.properties = rs.getString(9);
      info.workerId = request.getWorkerId();

      String workerVersion = request.getWorkerVersion();

      // Now, update this record as being worked on by this worker.
      int updCount = jdbcTemplate.update(updateStatement,
          new MapSqlParameterSource()
              .addValue("id", info.id)
              .addValue("workerId", info.workerId)
              .addValue("workerVersion", workerVersion)
              .addValue("now", currentDbTime.getTime())
              .addValue("newState", Character.toString(WORKING_STATE), Types.CHAR)
              .addValue("oldState", Character.toString(INITIATED_STATE), Types.CHAR));
      if (updCount == 1) {
        return info;
      } else if (updCount == 0) {
        LOG.debug("Worker {} (version: {}) picked up {}", request.getWorkerId(), workerVersion, info);
      } else {
        LOG.error("Unable to set to cq_state={} for compaction record: {}. updCnt={}. workerId={}. workerVersion={}",
            WORKING_STATE, info, updCount, request.getWorkerId(), workerVersion);
        throw new SQLException("Update failed for compaction: " + info);
      }
    }
    LOG.debug("No compactions found ready to compact");
    return null;
  }

}
