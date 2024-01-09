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
package org.apache.hadoop.hive.metastore.txn.jdbc.functions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FindNextCompactRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import static org.apache.hadoop.hive.metastore.txn.TxnStore.INITIATED_STATE;
import static org.apache.hadoop.hive.metastore.txn.TxnStore.WORKING_STATE;
import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;

public class NextCompactionFunction implements TransactionalFunction<CompactionInfo> {

  private static final Logger LOG = LoggerFactory.getLogger(NextCompactionFunction.class);

  private static final String updateStatement =
      "UPDATE \"COMPACTION_QUEUE\" " +
          "SET " +
          " \"CQ_WORKER_ID\" = :workerId, " +
          " \"CQ_WORKER_VERSION\" = :workerVersion, " +
          " \"CQ_START\" = :now, " +
          " \"CQ_STATE\" = :newState " +
          "WHERE \"CQ_ID\" = :id AND \"CQ_STATE\"= :oldState";

  private final FindNextCompactRequest request;
  private final Timestamp currentDbTime; 
  private final long poolTimeout;

  public NextCompactionFunction(FindNextCompactRequest request, Timestamp currentDbTime, long poolTimeout) {
    this.request = request;
    this.currentDbTime = currentDbTime;
    this.poolTimeout = poolTimeout;
  }

  @Override
  public CompactionInfo execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT \"CQ_ID\", \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\", " +
        "\"CQ_TYPE\", \"CQ_WORKER_ID\", \"CQ_WORKER_VERSION\", \"CQ_POOL_NAME\", \"CQ_NUMBER_OF_BUCKETS\", \"CQ_ORDER_BY\", " +
        "\"CQ_TBLPROPERTIES\" FROM \"COMPACTION_QUEUE\" WHERE \"CQ_STATE\" = :state AND ");
    boolean hasPoolName = StringUtils.isNotBlank(request.getPoolName());
    if(hasPoolName) {
      sb.append("\"CQ_POOL_NAME\"= :poolName");
    } else {
      sb.append("\"CQ_POOL_NAME\" IS NULL OR  \"CQ_ENQUEUE_TIME\" < (")
          .append(getEpochFn(jdbcResource.getDatabaseProduct())).append(" - ").append(poolTimeout).append(")");
    }
    sb.append(" ORDER BY \"CQ_ID\" ASC");

    MapSqlParameterSource params = new MapSqlParameterSource()
        .addValue("state", Character.toString(INITIATED_STATE), Types.CHAR);
    if(StringUtils.isNotBlank(request.getPoolName())) {
      params.addValue("poolName", request.getPoolName());
    }

    NamedParameterJdbcTemplate jdbcTemplate = jdbcResource.getJdbcTemplate();
    return jdbcTemplate.query(sb.toString(), params, new ResultSetExtractor<CompactionInfo>() {
      @Override
      public CompactionInfo extractData(ResultSet rs) throws SQLException, DataAccessException {
        while (rs.next()) {
          CompactionInfo info = new CompactionInfo();
          info.id = rs.getLong("CQ_ID");
          info.dbname = rs.getString("CQ_DATABASE");
          info.tableName = rs.getString("CQ_TABLE");
          info.partName = rs.getString("CQ_PARTITION");
          info.type = TxnUtils.dbCompactionType2ThriftType(rs.getString("CQ_TYPE").charAt(0));
          info.workerId = rs.getString("CQ_WORKER_ID");
          info.workerVersion = rs.getString("CQ_WORKER_VERSION");
          info.poolName = rs.getString("CQ_POOL_NAME");
          info.numberOfBuckets = rs.getInt("CQ_NUMBER_OF_BUCKETS");
          info.orderByClause = rs.getString("CQ_ORDER_BY");
          info.properties = rs.getString("CQ_TBLPROPERTIES");

          // Now, update this record as being worked on by this worker.
          int updCount = jdbcTemplate.update(updateStatement,
              new MapSqlParameterSource()
                  .addValue("id", info.id)
                  .addValue("workerId", request.getWorkerId())
                  .addValue("workerVersion", request.getWorkerVersion())
                  .addValue("now", currentDbTime.getTime())
                  .addValue("newState", Character.toString(WORKING_STATE), Types.CHAR)
                  .addValue("oldState", Character.toString(INITIATED_STATE), Types.CHAR));
          if (updCount == 1) {
            return info;
          } else if (updCount == 0) {
            LOG.debug("Compaction item ({}) already picked up another Worker {} (version: {})", info, info.workerId, info.workerVersion);
          } else {
            LOG.error("Unable to set to cq_state={} for compaction record: {}. updCnt={}. workerId={}. workerVersion={}",
                WORKING_STATE, info, updCount, request.getWorkerId(), request.getWorkerVersion());
            throw new SQLException("Update failed for compaction: " + info);
          }
        }
        LOG.debug("No compactions found ready to compact");
        return null;
      }
    });
    
  }

}