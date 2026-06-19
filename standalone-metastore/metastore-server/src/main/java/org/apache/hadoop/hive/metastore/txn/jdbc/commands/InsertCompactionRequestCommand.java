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
package org.apache.hadoop.hive.metastore.txn.jdbc.commands;

import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionState;
import org.apache.hadoop.hive.metastore.txn.MetaWrapperException;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.ParameterizedCommand;
import org.apache.hadoop.hive.metastore.utils.StringableMap;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.Types;
import java.util.function.Function;

import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;

public class InsertCompactionRequestCommand implements ParameterizedCommand {
  
  private final long id;
  private final CompactionState compactionState;
  private final CompactionRequest rqst;
  private Long highestWriteId = null;
  private Long txnId = null;

  public InsertCompactionRequestCommand(long id, CompactionState compactionState, CompactionRequest rqst) {
    this.id = id;
    this.compactionState = compactionState;
    this.rqst = rqst;
  }
  
  public InsertCompactionRequestCommand withTxnDetails(long highestWriteId, long txnId) {
    this.highestWriteId = highestWriteId;
    this.txnId = txnId;
    return this;
  }

  @Override
  public Function<Integer, Boolean> resultPolicy() {
    return ParameterizedCommand.EXACTLY_ONE_ROW;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return "INSERT INTO \"COMPACTION_QUEUE\" (\"CQ_ID\", \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\", \"CQ_STATE\", " +
        "\"CQ_TYPE\", \"CQ_POOL_NAME\", \"CQ_NUMBER_OF_BUCKETS\", \"CQ_ORDER_BY\", \"CQ_TBLPROPERTIES\", \"CQ_RUN_AS\", " +
        "\"CQ_INITIATOR_ID\", \"CQ_INITIATOR_VERSION\", \"CQ_HIGHEST_WRITE_ID\", \"CQ_TXN_ID\", \"CQ_ENQUEUE_TIME\") " +
        "VALUES(:id, :dbName, :tableName, :partition, :state, :type, :poolName, :buckets, :orderBy, :tblProperties, " +
        ":runAs, :initiatorId, :initiatorVersion, :highestWriteId, :txnId, " + getEpochFn(databaseProduct) + ")";
  }
  
  @Override
  public SqlParameterSource getQueryParameters() {
    try {
      return new MapSqlParameterSource()
          .addValue("id", id)
          .addValue("dbName", rqst.getDbname(), Types.VARCHAR)
          .addValue("tableName", rqst.getTablename(), Types.VARCHAR)
          .addValue("partition", rqst.getPartitionname(), Types.VARCHAR)
          .addValue("state", compactionState.getSqlConst(), Types.VARCHAR)
          .addValue("type", TxnUtils.thriftCompactionType2DbType(rqst.getType()), Types.VARCHAR)
          .addValue("poolName", rqst.getPoolName(), Types.VARCHAR)
          .addValue("buckets", rqst.isSetNumberOfBuckets() ? rqst.getNumberOfBuckets() : null, Types.INTEGER)
          .addValue("orderBy", rqst.getOrderByClause(), Types.VARCHAR)
          .addValue("tblProperties", rqst.getProperties() == null ? null : new StringableMap(rqst.getProperties()), Types.VARCHAR)
          .addValue("runAs", rqst.getRunas(), Types.VARCHAR)
          .addValue("initiatorId", rqst.getInitiatorId(), Types.VARCHAR)
          .addValue("initiatorVersion", rqst.getInitiatorVersion(), Types.VARCHAR)
          .addValue("highestWriteId", highestWriteId, Types.BIGINT)
          .addValue("txnId", txnId, Types.BIGINT);
    } catch (MetaException e) {
      throw new MetaWrapperException(e);
    }
  }
}
