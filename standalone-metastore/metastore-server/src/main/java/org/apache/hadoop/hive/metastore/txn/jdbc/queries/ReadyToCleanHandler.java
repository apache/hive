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
import org.apache.hadoop.hive.metastore.txn.TxnHandler;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.metastore.txn.TxnStore.ABORT_TXN_CLEANUP_TYPE;
import static org.apache.hadoop.hive.metastore.txn.TxnStore.READY_FOR_CLEANING;
import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;

public class ReadyToCleanHandler implements QueryHandler<List<CompactionInfo>> {
  
  private final long minOpenTxnWaterMark;
  private final long retentionTime;
  private final int fetchSize;

  public ReadyToCleanHandler(Configuration conf, long minOpenTxnWaterMark, long retentionTime) {
    this.minOpenTxnWaterMark = minOpenTxnWaterMark;
    this.retentionTime = retentionTime;
    this.fetchSize = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_FETCH_SIZE);
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    /*
     * By filtering on minOpenTxnWaterMark, we will only clea nup after every transaction is committed, that could see
     * the uncompacted deltas. This way the cleaner can clean up everything that was made obsolete by this compaction.
     */
    String whereClause = " WHERE \"CQ_STATE\" = '" + READY_FOR_CLEANING + "'" + 
        " AND \"CQ_TYPE\" != '" + ABORT_TXN_CLEANUP_TYPE + "'" +
        " AND (\"CQ_COMMIT_TIME\" < (" + getEpochFn(databaseProduct) + " - \"CQ_RETRY_RETENTION\" - " + retentionTime + ") OR \"CQ_COMMIT_TIME\" IS NULL)";

    String queryStr =
        " \"CQ_ID\", \"cq1\".\"CQ_DATABASE\", \"cq1\".\"CQ_TABLE\", \"cq1\".\"CQ_PARTITION\"," +
            "  \"CQ_TYPE\", \"CQ_RUN_AS\", \"CQ_HIGHEST_WRITE_ID\", \"CQ_TBLPROPERTIES\", \"CQ_RETRY_RETENTION\", " +
            "  \"CQ_NEXT_TXN_ID\"";
    if (TxnHandler.ConfVars.useMinHistoryWriteId()) {
      queryStr += ", \"MIN_OPEN_WRITE_ID\"";
    }
    queryStr +=
        "  FROM \"COMPACTION_QUEUE\" \"cq1\" " +
            "INNER JOIN (" +
            "  SELECT MIN(\"CQ_HIGHEST_WRITE_ID\") \"MIN_WRITE_ID_HWM\", \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\"" +
            "  FROM \"COMPACTION_QUEUE\""
            + whereClause +
            "  GROUP BY \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\") \"cq2\" " +
            "ON \"cq1\".\"CQ_DATABASE\" = \"cq2\".\"CQ_DATABASE\""+
            "  AND \"cq1\".\"CQ_TABLE\" = \"cq2\".\"CQ_TABLE\""+
            "  AND (\"cq1\".\"CQ_PARTITION\" = \"cq2\".\"CQ_PARTITION\"" +
            "    OR \"cq1\".\"CQ_PARTITION\" IS NULL AND \"cq2\".\"CQ_PARTITION\" IS NULL)" +
            "  AND \"CQ_HIGHEST_WRITE_ID\" = \"MIN_WRITE_ID_HWM\" ";

    if (TxnHandler.ConfVars.useMinHistoryWriteId()) {
      queryStr +=
          "LEFT JOIN (" +
              "  SELECT MIN(\"MH_WRITEID\") \"MIN_OPEN_WRITE_ID\", \"MH_DATABASE\", \"MH_TABLE\"" +
              "  FROM \"MIN_HISTORY_WRITE_ID\"" +
              "  GROUP BY \"MH_DATABASE\", \"MH_TABLE\") \"hwm\" " +
              "ON \"cq1\".\"CQ_DATABASE\" = \"hwm\".\"MH_DATABASE\"" +
              "  AND \"cq1\".\"CQ_TABLE\" = \"hwm\".\"MH_TABLE\"";

      whereClause += " AND (\"CQ_HIGHEST_WRITE_ID\" < \"MIN_OPEN_WRITE_ID\" OR \"MIN_OPEN_WRITE_ID\" IS NULL)";

    } else if (minOpenTxnWaterMark > 0) {
      whereClause += " AND (\"CQ_NEXT_TXN_ID\" <= " + minOpenTxnWaterMark + " OR \"CQ_NEXT_TXN_ID\" IS NULL)";
    }
    queryStr += whereClause + " ORDER BY \"CQ_ID\"";

    queryStr = databaseProduct.addLimitClause(fetchSize, queryStr);
    return queryStr;
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return null;
  }

  @Override
  public List<CompactionInfo> extractData(ResultSet rs) throws SQLException, DataAccessException {
    List<CompactionInfo> infos = new ArrayList<>();
    while (rs.next()) {
      CompactionInfo info = new CompactionInfo();
      info.id = rs.getLong(1);
      info.dbname = rs.getString(2);
      info.tableName = rs.getString(3);
      info.partName = rs.getString(4);
      info.type = TxnUtils.dbCompactionType2ThriftType(rs.getString(5).charAt(0));
      info.runAs = rs.getString(6);
      info.highestWriteId = rs.getLong(7);
      info.properties = rs.getString(8);
      info.retryRetention = rs.getInt(9);
      info.nextTxnId = rs.getLong(10);
      if (TxnHandler.ConfVars.useMinHistoryWriteId()) {
        info.minOpenWriteId = rs.getLong(11);
      }
      infos.add(info);
    }
    return infos;
  }
  
}
