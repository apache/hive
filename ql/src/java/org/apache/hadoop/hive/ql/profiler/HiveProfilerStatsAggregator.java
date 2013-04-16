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

import java.util.HashMap;
import java.util.Map;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;

public class HiveProfilerStatsAggregator {
  final private Log LOG = LogFactory.getLog(this.getClass().getName());
  private long totalTime;
  private HiveProfilePublisherInfo rawProfileConnInfo;

  private Map<String, HiveProfilerAggregateStat> stats =
    new HashMap<String, HiveProfilerAggregateStat>();

  public HiveProfilerStatsAggregator(HiveConf conf) {
    try {
    // initialize the raw data connection
    rawProfileConnInfo = new HiveProfilePublisherInfo(conf);
    populateAggregateStats(conf);
    } catch (Exception e) {

      LOG.error("Error during initialization", e);
    }
  }

  public long getTotalTime() {
    return totalTime;
  }

  public Map<String, HiveProfilerAggregateStat> getAggregateStats() {
    return stats;
  }


  private void populateAggregateStats(HiveConf conf) throws SQLException {
    int waitWindow = rawProfileConnInfo.getWaitWindow();
    int maxRetries = rawProfileConnInfo.getMaxRetries();

    String queryId = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYID);
    String profilerStatsTable = rawProfileConnInfo.getTableName();
    String getProfileStats =
      "SELECT * FROM " + profilerStatsTable + " WHERE queryId = ? ";
    Utilities.SQLCommand<ResultSet> execQuery = new Utilities.SQLCommand<ResultSet>() {
      @Override
      public ResultSet run(PreparedStatement stmt) throws SQLException {
        return stmt.executeQuery();
      }
    };

    try {
      PreparedStatement getProfileStatsStmt =
        Utilities.prepareWithRetry(rawProfileConnInfo.getConnection(),
          getProfileStats, waitWindow, maxRetries);
      getProfileStatsStmt.setString(1, queryId);
      ResultSet result = Utilities.executeWithRetry(execQuery, getProfileStatsStmt,
        waitWindow, maxRetries);

      populateAggregateStats(result);
      getProfileStatsStmt.close();
    } catch(Exception e) {
      LOG.error("executing error: ", e);
    } finally {
      HiveProfilerUtils.closeConnection(rawProfileConnInfo);
    }
  }

  private void populateAggregateStats(ResultSet result) {
    try {
      while(result.next()){
        // string denoting parent==>child
        // example:SEL_2==>GBY_1
        String levelAnnoName = result.getString(HiveProfilerStats.Columns.LEVEL_ANNO_NAME);
        // Microseconds
        Long curInclTime = result.getLong(HiveProfilerStats.Columns.INCL_TIME) / 1000;
        Long curCallCount = result.getLong(HiveProfilerStats.Columns.CALL_COUNT);
        totalTime += curInclTime;
        if(curInclTime != null && curCallCount != null) {
          HiveProfilerAggregateStat curStat;
          if (stats.containsKey(levelAnnoName)) {
            curStat = stats.get(levelAnnoName);
            curStat.update(curInclTime, curCallCount);
          } else {
            curStat = new HiveProfilerAggregateStat(curInclTime, curCallCount);
          }
          stats.put(levelAnnoName, curStat);
        }
      }
    } catch (Exception e) {
      LOG.error("Error Aggregating Stats", e);
    }
  }

}

