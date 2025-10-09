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

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.CompactionInfoStruct;
import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoRequest;
import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionState;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class GetLatestCommittedCompactionInfoHandler implements QueryHandler<GetLatestCommittedCompactionInfoResponse> {
  
  private final GetLatestCommittedCompactionInfoRequest rqst;

  public GetLatestCommittedCompactionInfoHandler(GetLatestCommittedCompactionInfoRequest rqst) {
    this.rqst = rqst;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return 
        "SELECT * FROM ( " +
          "SELECT \"CC_ID\", \"CC_DATABASE\", \"CC_TABLE\", \"CC_PARTITION\", \"CC_TYPE\" FROM \"COMPLETED_COMPACTIONS\" " +
          "   WHERE \"CC_STATE\" = :succeeded " + 
          "UNION ALL " +
          "SELECT \"CQ_ID\" AS \"CC_ID\", \"CQ_DATABASE\" AS \"CC_DATABASE\", \"CQ_TABLE\" AS \"CC_TABLE\", " +
          "\"CQ_PARTITION\" AS \"CC_PARTITION\", \"CQ_TYPE\" AS \"CC_TYPE\" FROM \"COMPACTION_QUEUE\" " +
          "   WHERE \"CQ_STATE\" = :readyForCleaning) AS compactions " +
        "WHERE " +
            "\"CC_DATABASE\" = :dbName AND \"CC_TABLE\" = :tableName AND " +
            "(\"CC_PARTITION\" IN (:partitionNames) OR :emptyPartitionNames = TRUE) AND " +
            "(\"CC_ID\" > :id OR :id IS NULL) " +
        "ORDER BY \"CC_ID\" DESC";
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource()
        .addValue("succeeded", CompactionState.SUCCEEDED.getSqlConst(), Types.CHAR)
        .addValue("readyForCleaning", CompactionState.READY_FOR_CLEANING.getSqlConst(), Types.CHAR)
        .addValue("dbName", rqst.getDbname())
        .addValue("tableName", rqst.getTablename())
        .addValue("emptyPartitionNames", CollectionUtils.isEmpty(rqst.getPartitionnames()), Types.BOOLEAN)
        .addValue("partitionNames", CollectionUtils.isEmpty(rqst.getPartitionnames()) ? null : rqst.getPartitionnames(), Types.VARCHAR)
        .addValue("id", rqst.isSetLastCompactionId() ? rqst.getLastCompactionId() : null, Types.BIGINT);
  }

  @Override
  public GetLatestCommittedCompactionInfoResponse extractData(ResultSet rs) throws SQLException, DataAccessException {
    GetLatestCommittedCompactionInfoResponse response = new GetLatestCommittedCompactionInfoResponse(new ArrayList<>());
    Set<String> partitionSet = new HashSet<>();
    while (rs.next()) {
      CompactionInfoStruct lci = new CompactionInfoStruct();
      lci.setId(rs.getLong(1));
      lci.setDbname(rs.getString(2));
      lci.setTablename(rs.getString(3));
      String partition = rs.getString(4);
      if (!rs.wasNull()) {
        lci.setPartitionname(partition);
      }
      lci.setType(TxnUtils.dbCompactionType2ThriftType(rs.getString(5).charAt(0)));
      // Only put the latest record of each partition into response
      if (!partitionSet.contains(partition)) {
        response.addToCompactions(lci);
        partitionSet.add(partition);
      }
    }
    return response;
  }

}
