/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.ql.hooks.HiveProtoLoggingHook;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.executeStatementOnDriver;
import static org.junit.Assert.assertEquals;

public class TestMRCompactorOnTez extends CompactorOnTezTest {

  @Test
  public void testCompactorGatherStats() throws Exception{
    conf.setBoolVar(HiveConf.ConfVars.HIVE_WRITE_ACID_VERSION_FILE, true);
    conf.setVar(HiveConf.ConfVars.COMPACTOR_JOB_QUEUE, CUSTOM_COMPACTION_QUEUE);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_MR_COMPACTOR_GATHER_STATS, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER, false);
    conf.setVar(HiveConf.ConfVars.HIVE_PROTO_EVENTS_BASE_PATH, tmpFolder);

    String dbName = "default";
    String tableName = "stats_comp_test";
    List<String> colNames = Arrays.asList("a");

    executeStatementOnDriver("drop table if exists " + dbName + "." + tableName, driver);
    executeStatementOnDriver("create table " + dbName + "." + tableName +
        " (a INT) STORED AS ORC TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into " + dbName + "." + tableName + " values(1)", driver);

    // Make sure we do not have statistics for this table yet
    // Compaction generates stats only if there is any
    IMetaStoreClient msClient = new HiveMetaStoreClient(conf);
    executeStatementOnDriver("analyze table " + dbName + "." + tableName + " compute statistics for columns", driver);
    executeStatementOnDriver("insert into " + dbName + "." + tableName + " values(2)", driver);

    conf.setVar(HiveConf.ConfVars.PRE_EXEC_HOOKS, HiveProtoLoggingHook.class.getName());
    // Run major compaction and cleaner
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MAJOR, false);
    conf.setVar(HiveConf.ConfVars.PRE_EXEC_HOOKS, StringUtils.EMPTY);

    CompactorTestUtil.runCleaner(conf);
    verifySuccessfulCompaction(1);

    List<ColumnStatisticsObj> colStats = msClient.getTableColumnStatistics(dbName, tableName, colNames, Constants.HIVE_ENGINE);
    assertEquals("Stats should be there", 1, colStats.size());
    assertEquals("Value should contain new data", 2, colStats.get(0).getStatsData().getLongStats().getHighValue());
    assertEquals("Value should contain new data", 1, colStats.get(0).getStatsData().getLongStats().getLowValue());

    executeStatementOnDriver("insert into " + dbName + "." + tableName + " values(3)", driver);
    executeStatementOnDriver("alter table " + dbName + "." + tableName + " set tblproperties('compactor.mapred.job.queue.name'='" +
        CUSTOM_COMPACTION_QUEUE + "')", driver);

    conf.setVar(HiveConf.ConfVars.PRE_EXEC_HOOKS, HiveProtoLoggingHook.class.getName());
    // Run major compaction and cleaner
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MAJOR, false);
    conf.setVar(HiveConf.ConfVars.PRE_EXEC_HOOKS, StringUtils.EMPTY);

    CompactorTestUtil.runCleaner(conf);
    verifySuccessfulCompaction(2);

    colStats = msClient.getTableColumnStatistics(dbName, tableName, colNames, Constants.HIVE_ENGINE);
    assertEquals("Stats should be there", 1, colStats.size());
    assertEquals("Value should contain new data", 3, colStats.get(0).getStatsData().getLongStats().getHighValue());
    assertEquals("Value should contain new data", 1, colStats.get(0).getStatsData().getLongStats().getLowValue());

    executeStatementOnDriver("insert into " + dbName + "." + tableName + " values(4)", driver);
    conf.setVar(HiveConf.ConfVars.PRE_EXEC_HOOKS, HiveProtoLoggingHook.class.getName());
    CompactorTestUtil.runCompaction(conf, dbName, tableName, CompactionType.MAJOR, false,
        Collections.singletonMap("compactor.mapred.job.queue.name", CUSTOM_COMPACTION_QUEUE));
    conf.setVar(HiveConf.ConfVars.PRE_EXEC_HOOKS, StringUtils.EMPTY);

    CompactorTestUtil.runCleaner(conf);
    verifySuccessfulCompaction(3);

    colStats = msClient.getTableColumnStatistics(dbName, tableName, colNames, Constants.HIVE_ENGINE);
    assertEquals("Stats should be there", 1, colStats.size());
    assertEquals("Value should contain new data", 4, colStats.get(0).getStatsData().getLongStats().getHighValue());
    assertEquals("Value should contain new data", 1, colStats.get(0).getStatsData().getLongStats().getLowValue());
  }

}
