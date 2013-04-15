/**
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
package org.apache.hadoop.hive.ql.profiler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;

public class HiveProfilePublisher {
  private final Log LOG = LogFactory.getLog(this.getClass().getName());
  private boolean connected = false;
  private HiveProfilePublisherInfo info;


  public boolean closeConnection() {

    if (info == null || info.getConnection() == null) {
      return true;
    }
    try {
      if (info.getInsertStatement() != null) {
        info.closeInsertStatement();
      }
      info.getConnection().close();
      return true;
    } catch (Exception e) {
      LOG.error("Error during JDBC termination. ", e);
      return false;
    }
  }

  public boolean initialize(Configuration conf) {
    try {
      info = new HiveProfilePublisherInfo(conf);
      String createTable = getCreate();
      LOG.info(createTable);
      HiveProfilerUtils.createTableIfNonExistent(info, createTable);
      info.prepareInsert();
    } catch (Exception e) {
      LOG.error("Error during HiveProfilePublisher initialization", e);
      return false;
    }
    return true;
  }

  private String getCreate() {
    return "CREATE TABLE " + info.getTableName() +
    " ( " +
      HiveProfilerStats.Columns.QUERY_ID + " VARCHAR(512) NOT NULL, " +
      HiveProfilerStats.Columns.TASK_ID + " VARCHAR(512) NOT NULL, " +
      HiveProfilerStats.Columns.OPERATOR_ID + " INT, "  +
      HiveProfilerStats.Columns.OPERATOR_NAME + " VARCHAR(512) NOT NULL, " +
      HiveProfilerStats.Columns.PARENT_OPERATOR_ID + " INT," +
      HiveProfilerStats.Columns.PARENT_OPERATOR_NAME + " VARCHAR(512), " +
      HiveProfilerStats.Columns.LEVEL_ANNO_NAME + " VARCHAR(512), " +
      HiveProfilerStats.Columns.CALL_COUNT + " BIGINT, " +
      HiveProfilerStats.Columns.INCL_TIME + " BIGINT ) ";

  }

  public boolean publishStat(String queryId, Map<String, String> stats,
    Configuration conf) {
    if (info == null || info.getConnection() == null) {
      if(!initialize(conf)) {
        return false;
      }
    } else {
      try {
        if(stats == null || stats.isEmpty()) {
          return true;
        }
        Utilities.SQLCommand<Void> execUpdate = new Utilities.SQLCommand<Void>() {
          @Override
          public Void run(PreparedStatement stmt) throws SQLException {
            stmt.executeUpdate();
            return null;
          }
        };
        PreparedStatement insStmt = info.getInsert(stats);
        Utilities.executeWithRetry(execUpdate, insStmt, info.getWaitWindow(), info.getMaxRetries());
      } catch (Exception e) {
        LOG.error("ERROR during publishing profiling data. ", e);
        return false;
      }
    }

    return true;
  }
}
