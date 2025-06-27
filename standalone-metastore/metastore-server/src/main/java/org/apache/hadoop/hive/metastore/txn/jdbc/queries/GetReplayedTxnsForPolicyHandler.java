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
import org.apache.hadoop.hive.metastore.api.ReplayedTxnsForPolicyResult;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class GetReplayedTxnsForPolicyHandler implements QueryHandler<ReplayedTxnsForPolicyResult> {

    private final String replPolicy;

    public GetReplayedTxnsForPolicyHandler(String replPolicy) {
        this.replPolicy = replPolicy;
    }

    @Override
    public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
        return "SELECT  \"RTM_SRC_TXN_ID\", \"RTM_TARGET_TXN_ID\" " +
                "FROM \"REPL_TXN_MAP\" WHERE \"RTM_REPL_POLICY\" = :replPolicy";
    }

    @Override
    public SqlParameterSource getQueryParameters() {
        return new MapSqlParameterSource()
                .addValue("replPolicy", replPolicy);
    }

    @Override
    public ReplayedTxnsForPolicyResult extractData(ResultSet rs) throws SQLException, DataAccessException {
        Map<String, String> result = new HashMap<>();
        while (rs.next()) {
            long srcTxnId = rs.getLong("RTM_SRC_TXN_ID");
            long tgtTxnId = rs.getLong("RTM_TARGET_TXN_ID");

            result.put(String.valueOf(srcTxnId), String.valueOf(tgtTxnId));
        }
        return new ReplayedTxnsForPolicyResult(result);
    }
}
