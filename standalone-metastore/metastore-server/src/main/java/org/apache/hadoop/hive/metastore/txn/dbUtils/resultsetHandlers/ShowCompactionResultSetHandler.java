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
package org.apache.hadoop.hive.metastore.txn.dbUtils.resultsetHandlers;

import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.txn.CompactionState;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class ShowCompactionResultSetHandler implements ResultSetHandler {

    int limit;
    public ShowCompactionResultSetHandler(int limit){
        this.limit = limit ;
    }

    private static final String DEFAULT_POOL_NAME = "default";
    public Object handle(final ResultSet rs) throws SQLException {
        ShowCompactResponse response = new ShowCompactResponse(new ArrayList<>());
        while (rs.next() && limit > 0) {
            ShowCompactResponseElement e = new ShowCompactResponseElement();
            e.setDbname(rs.getString(1));
            e.setTablename(rs.getString(2));
            e.setPartitionname(rs.getString(3));
            e.setState(CompactionState.fromSqlConst(rs.getString(4)).toString());
            try {
                e.setType(TxnUtils.dbCompactionType2ThriftType(rs.getString(5).charAt(0)));
            } catch (MetaException ex) {
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
            limit--;
        }
        return response;
    }
}
