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

import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.GetOpenTxnsListHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GetValidWriteIdsFunction implements TransactionalFunction<GetValidWriteIdsResponse> {

  private final GetValidWriteIdsRequest rqst;
  private final long openTxnTimeOutMillis;

  public GetValidWriteIdsFunction(GetValidWriteIdsRequest rqst, long openTxnTimeOutMillis) {
    this.rqst = rqst;
    this.openTxnTimeOutMillis = openTxnTimeOutMillis;
  }

  public GetValidWriteIdsResponse execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    ValidTxnList validTxnList;

    // We should prepare the valid write ids list based on validTxnList of current txn.
    // If no txn exists in the caller, then they would pass null for validTxnList and so it is
    // required to get the current state of txns to make validTxnList
    if (rqst.isSetValidTxnList()) {
      assert !rqst.isSetWriteId();
      validTxnList = new ValidReadTxnList(rqst.getValidTxnList());
    } else if (rqst.isSetWriteId()) {
      validTxnList = TxnCommonUtils.createValidReadTxnList(getOpenTxns(jdbcResource), 
          getTxnId(jdbcResource, rqst.getFullTableNames().get(0), rqst.getWriteId())); 
    } else {
      // Passing 0 for currentTxn means, this validTxnList is not wrt to any txn
      validTxnList = TxnCommonUtils.createValidReadTxnList(getOpenTxns(jdbcResource), 0);
    }

    // Get the valid write id list for all the tables read by the current txn
    List<TableValidWriteIds> tblValidWriteIdsList = new ArrayList<>();
    for (String fullTableName : rqst.getFullTableNames()) {
      tblValidWriteIdsList.add(new GetValidWriteIdsForTableFunction(validTxnList, fullTableName).execute(jdbcResource));
    }
    return new GetValidWriteIdsResponse(tblValidWriteIdsList);
  }

  private long getTxnId(MultiDataSourceJdbcResource jdbcResource, String fullTableName, Long writeId) throws MetaException {
    String[] names = TxnUtils.getDbTableName(fullTableName);
    assert (names.length == 2);
    Long txnId = jdbcResource.getJdbcTemplate().query(
        "SELECT \"T2W_TXNID\" FROM \"TXN_TO_WRITE_ID\" WHERE \"T2W_DATABASE\" = :db AND "
            + "\"T2W_TABLE\" = :table AND \"T2W_WRITEID\" = :writeId",
        new MapSqlParameterSource()
            .addValue("db", names[0])
            .addValue("table", names[1])
            .addValue("writeId", writeId),
        (ResultSet rs) -> {
          if(rs.next()) {
            long id = rs.getLong(1);
            return rs.wasNull() ? null : id;
          }
          return null;
        });
    if (txnId == null) {
      throw new MetaException("invalid write id " + writeId + " for table " + fullTableName);
    }
    return txnId;
  }
  
  private GetOpenTxnsResponse getOpenTxns(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    return jdbcResource.execute(new GetOpenTxnsListHandler(false, openTxnTimeOutMillis))
        .toOpenTxnsResponse(Collections.singletonList(TxnType.READ_ONLY));
  }

}
