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
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;

public class GetCompactionInfoHandler implements QueryHandler<CompactionInfo> {

  private final long id;
  private final boolean isTransactionId;

  // language=SQL
  public static final String SELECT_BY_ID = 
      "SELECT \"CQ_ID\", \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\", "
      + "\"CQ_STATE\", \"CQ_TYPE\", \"CQ_TBLPROPERTIES\", \"CQ_WORKER_ID\", \"CQ_START\", \"CQ_RUN_AS\", "
      + "\"CQ_HIGHEST_WRITE_ID\", \"CQ_META_INFO\", \"CQ_HADOOP_JOB_ID\", \"CQ_ERROR_MESSAGE\", "
      + "\"CQ_ENQUEUE_TIME\", \"CQ_WORKER_VERSION\", \"CQ_INITIATOR_ID\", \"CQ_INITIATOR_VERSION\", "
      + "\"CQ_RETRY_RETENTION\", \"CQ_NEXT_TXN_ID\", \"CQ_TXN_ID\", \"CQ_COMMIT_TIME\", \"CQ_POOL_NAME\", "
      + "\"CQ_NUMBER_OF_BUCKETS\", \"CQ_ORDER_BY\" FROM \"COMPACTION_QUEUE\" WHERE \"CQ_ID\" = :id";

  // language=SQL
  public static final String SELECT_BY_TXN_ID =
      "SELECT \"CQ_ID\", \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\", "
          + "\"CQ_STATE\", \"CQ_TYPE\", \"CQ_TBLPROPERTIES\", \"CQ_WORKER_ID\", \"CQ_START\", \"CQ_RUN_AS\", "
          + "\"CQ_HIGHEST_WRITE_ID\", \"CQ_META_INFO\", \"CQ_HADOOP_JOB_ID\", \"CQ_ERROR_MESSAGE\", "
          + "\"CQ_ENQUEUE_TIME\", \"CQ_WORKER_VERSION\", \"CQ_INITIATOR_ID\", \"CQ_INITIATOR_VERSION\", "
          + "\"CQ_RETRY_RETENTION\", \"CQ_NEXT_TXN_ID\", \"CQ_TXN_ID\", \"CQ_COMMIT_TIME\", \"CQ_POOL_NAME\", "
          + "\"CQ_NUMBER_OF_BUCKETS\", \"CQ_ORDER_BY\" FROM \"COMPACTION_QUEUE\" WHERE \"CQ_TXN_ID\" = :id";

  public GetCompactionInfoHandler(long id, boolean isTransactionId) {
    this.id = id;
    this.isTransactionId = isTransactionId;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return isTransactionId ? SELECT_BY_TXN_ID : SELECT_BY_ID;
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource().addValue("id", id);
  }  

  @Override
  public CompactionInfo extractData(ResultSet rs) throws SQLException, DataAccessException {
    if (rs.next()) {
      CompactionInfo fullCi = new CompactionInfo();
      fullCi.id = rs.getLong("CQ_ID");
      fullCi.dbname = rs.getString("CQ_DATABASE");
      fullCi.tableName = rs.getString("CQ_TABLE");
      fullCi.partName = rs.getString("CQ_PARTITION");
      fullCi.state = rs.getString("CQ_STATE").charAt(0);//cq_state
      fullCi.type = TxnUtils.dbCompactionType2ThriftType(rs.getString("CQ_TYPE").charAt(0));
      fullCi.properties = rs.getString("CQ_TBLPROPERTIES");
      fullCi.workerId = rs.getString("CQ_WORKER_ID");
      fullCi.start = rs.getLong("CQ_START");
      fullCi.runAs = rs.getString("CQ_RUN_AS");
      fullCi.highestWriteId = rs.getLong("CQ_HIGHEST_WRITE_ID");
      fullCi.metaInfo = rs.getBytes("CQ_META_INFO");
      fullCi.hadoopJobId = rs.getString("CQ_HADOOP_JOB_ID");
      fullCi.errorMessage = rs.getString("CQ_ERROR_MESSAGE");
      fullCi.enqueueTime = rs.getLong("CQ_ENQUEUE_TIME");
      fullCi.workerVersion = rs.getString("CQ_WORKER_VERSION");
      fullCi.initiatorId = rs.getString("CQ_INITIATOR_ID");
      fullCi.initiatorVersion = rs.getString("CQ_INITIATOR_VERSION");
      fullCi.retryRetention = rs.getLong("CQ_RETRY_RETENTION");
      fullCi.nextTxnId = rs.getLong("CQ_NEXT_TXN_ID");
      fullCi.txnId = rs.getLong("CQ_TXN_ID");
      fullCi.commitTime = rs.getLong("CQ_COMMIT_TIME");
      fullCi.poolName = rs.getString("CQ_POOL_NAME");
      fullCi.numberOfBuckets = rs.getInt("CQ_NUMBER_OF_BUCKETS");
      fullCi.orderByClause = rs.getString("CQ_ORDER_BY");
      return fullCi;
    }
    return null;
  }
}
