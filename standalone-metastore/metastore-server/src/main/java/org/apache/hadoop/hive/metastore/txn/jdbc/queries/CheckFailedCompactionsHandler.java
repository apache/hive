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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.metastore.txn.TxnStore.DID_NOT_INITIATE;
import static org.apache.hadoop.hive.metastore.txn.TxnStore.FAILED_STATE;

public class CheckFailedCompactionsHandler implements QueryHandler<Boolean> {
  
  private final Configuration conf;
  private final CompactionInfo ci;

  public CheckFailedCompactionsHandler(Configuration conf, CompactionInfo ci) {
    this.conf = conf;
    this.ci = ci;
  }

  //language=SQL
  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return "SELECT \"CC_STATE\", \"CC_ENQUEUE_TIME\" FROM \"COMPLETED_COMPACTIONS\" WHERE " +
        "\"CC_DATABASE\" = :dbName AND \"CC_TABLE\" = :tableName AND (:partName IS NULL OR \"CC_PARTITION\" = :partName) " +
        "AND \"CC_STATE\" != :state ORDER BY \"CC_ID\" DESC";    
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource()
        .addValue("state", Character.toString(DID_NOT_INITIATE), Types.CHAR)
        .addValue("dbName", ci.dbname)
        .addValue("tableName", ci.tableName)
        .addValue("partName", ci.partName, Types.VARCHAR);
  }

  @Override
  public Boolean extractData(ResultSet rs) throws SQLException, DataAccessException {
    int numFailed = 0;
    int numTotal = 0;
    long lastEnqueueTime = -1;
    int failedThreshold = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD);
    while(rs.next() && ++numTotal <= failedThreshold) {
      long enqueueTime = rs.getLong(2);
      if (enqueueTime > lastEnqueueTime) {
        lastEnqueueTime = enqueueTime;
      }
      if(rs.getString(1).charAt(0) == FAILED_STATE) {
        numFailed++;
      }
      else {
        numFailed--;
      }
    }
    // If the last attempt was too long ago, ignore the failed threshold and try compaction again
    long retryTime = MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_INITIATOR_FAILED_RETRY_TIME, TimeUnit.MILLISECONDS);
    boolean needsRetry = (retryTime > 0) && (lastEnqueueTime + retryTime < System.currentTimeMillis());
    return (numFailed == failedThreshold) && !needsRetry;
  }
}
