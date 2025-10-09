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
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.MetaWrapperException;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.ParameterizedCommand;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.function.Function;

public class InsertCompactionInfoCommand implements ParameterizedCommand {

  private final CompactionInfo ci;

  private final long compactionEndTime;

  //language=SQL
  private static final String INSERT =
      "INSERT INTO \"COMPLETED_COMPACTIONS\" " +
          "   (\"CC_ID\", \"CC_DATABASE\", \"CC_TABLE\", \"CC_PARTITION\", \"CC_STATE\", \"CC_TYPE\", " +
          "   \"CC_TBLPROPERTIES\", \"CC_WORKER_ID\", \"CC_START\", \"CC_END\", \"CC_RUN_AS\", " +
          "   \"CC_HIGHEST_WRITE_ID\", \"CC_META_INFO\", \"CC_HADOOP_JOB_ID\", \"CC_ERROR_MESSAGE\", " +
          "   \"CC_ENQUEUE_TIME\", \"CC_WORKER_VERSION\", \"CC_INITIATOR_ID\", \"CC_INITIATOR_VERSION\"," +
          "   \"CC_NEXT_TXN_ID\", \"CC_TXN_ID\", \"CC_COMMIT_TIME\", \"CC_POOL_NAME\", \"CC_NUMBER_OF_BUCKETS\", " +
          "   \"CC_ORDER_BY\") " +
          "   VALUES(:id,:dbname,:tableName,:partName,:state,:type,:properties,:workerId,:start,:endTime," +
          "    :runAs,:highestWriteId,:metaInfo,:hadoopJobId,:errorMessage,:enqueueTime,:workerVersion,:initiatorId," +
          "    :initiatorVersion,:nextTxnId,:txnId,:commitTime,:poolName,:numberOfBuckets,:orderByClause)";

  @Override
  public Function<Integer, Boolean> resultPolicy() {
    return ParameterizedCommand.EXACTLY_ONE_ROW;
  }


  public InsertCompactionInfoCommand(CompactionInfo ci, long compactionEndTime) {
    this.ci = ci;
    this.compactionEndTime = compactionEndTime;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return INSERT;
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    try {
      return new MapSqlParameterSource()
          .addValue("id", ci.id)
          .addValue("dbname", ci.dbname)
          .addValue("tableName", ci.tableName)
          .addValue("partName", ci.partName)
          .addValue("state", Character.toString(ci.state))
          .addValue("type", Character.toString(TxnUtils.thriftCompactionType2DbType(ci.type)))
          .addValue("properties", ci.properties)
          .addValue("workerId", ci.workerId)
          .addValue("start", ci.start)
          .addValue("endTime", compactionEndTime)
          .addValue("runAs", ci.runAs)
          .addValue("highestWriteId", ci.highestWriteId)
          .addValue("metaInfo", ci.metaInfo)
          .addValue("hadoopJobId", ci.hadoopJobId)
          .addValue("errorMessage", ci.errorMessage)
          .addValue("enqueueTime", ci.enqueueTime)
          .addValue("workerVersion", ci.workerVersion)
          .addValue("initiatorId", ci.initiatorId)
          .addValue("initiatorVersion", ci.initiatorVersion)
          .addValue("nextTxnId", ci.nextTxnId)
          .addValue("txnId", ci.txnId)
          .addValue("commitTime", ci.commitTime)
          .addValue("poolName", ci.poolName)
          .addValue("numberOfBuckets", ci.numberOfBuckets)
          .addValue("orderByClause", ci.orderByClause);
    } catch (MetaException e) {
      throw new MetaWrapperException(e);
    }
  }  

}
