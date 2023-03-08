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
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.TxnCommandsBaseForTests;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.io.*;

import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactorBase.executeStatementOnDriver;

public class CompactionPoolOnTezTest extends CompactorOnTezTest {

  private static final String DEFAULT_TABLE_NAME = "compaction_test";
  private static final String NON_DEFAULT_DB_NAME = "test_db";

  private TestDataProvider provider;


  public void setup() throws Exception {
    super.setup();
    conf.setIntVar(HiveConf.ConfVars.HIVE_COMPACTOR_DELTA_NUM_THRESHOLD, 2);
    provider = new TestDataProvider();
  }

  private void checkCompactionRequest(String state, String pool) throws MetaException {
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    List<ShowCompactResponseElement> compacts = rsp.getCompacts();
    Assert.assertEquals("Expecting 1 rows and found " + compacts.size(), 1, compacts.size());
    Assert.assertEquals("Expecting compaction state '" + state + "' and found:" + compacts.get(0).getState(),
        state, compacts.get(0).getState());
    Assert.assertEquals("Expecting compaction pool '" + pool + "' and found:" + compacts.get(0).getPoolName(),
        pool, compacts.get(0).getPoolName());
  }

  @Test
  public void testAlterTableCompactCommandRespectsPoolName() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.HIVE_COMPACTOR_WORKER_POOL, "pool1");
    provider.createFullAcidTable(null, DEFAULT_TABLE_NAME, false, false, properties);
    provider.insertTestData(DEFAULT_TABLE_NAME, false);

    executeStatementOnDriver("ALTER TABLE " + DEFAULT_TABLE_NAME + " COMPACT 'major' POOL 'pool2'", driver);

    checkCompactionRequest("initiated", "pool2");
  }

  @Test
  public void testInitiatorRespectsDbLevelPoolName() throws Exception {
    provider.createDb(NON_DEFAULT_DB_NAME, "pool1");
    provider.createFullAcidTable(NON_DEFAULT_DB_NAME, DEFAULT_TABLE_NAME, false, false);
    provider.insertTestData(NON_DEFAULT_DB_NAME, DEFAULT_TABLE_NAME);

    TxnCommandsBaseForTests.runInitiator(conf);

    checkCompactionRequest("initiated", "pool1");
  }

  @Test
  public void testInitiatorRespectsTableLevelPoolName() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.HIVE_COMPACTOR_WORKER_POOL, "pool1");
    provider.createFullAcidTable(null, DEFAULT_TABLE_NAME, false, false, properties);
    provider.insertTestData(DEFAULT_TABLE_NAME, false);

    TxnCommandsBaseForTests.runInitiator(conf);

    checkCompactionRequest("initiated", "pool1");
  }

  @Test
  public void testInitiatorHandlesEmptyPoolName() throws Exception {
    provider.createFullAcidTable(null, DEFAULT_TABLE_NAME, false, false);
    provider.insertTestData(DEFAULT_TABLE_NAME, false);

    TxnCommandsBaseForTests.runInitiator(conf);

    checkCompactionRequest("initiated", "default");
  }

  @Test
  public void testInitiatorRespectsTableLevelPoolNameOverDbLevel() throws Exception {
    provider.createDb(NON_DEFAULT_DB_NAME, "db_pool");
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.HIVE_COMPACTOR_WORKER_POOL, "table_pool");
    provider.createFullAcidTable(NON_DEFAULT_DB_NAME, DEFAULT_TABLE_NAME, false, false, properties);
    provider.insertTestData(NON_DEFAULT_DB_NAME, DEFAULT_TABLE_NAME);

    TxnCommandsBaseForTests.runInitiator(conf);

    checkCompactionRequest("initiated", "table_pool");
  }

  @Test
  public void testShowCompactionsContainsPoolName() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.HIVE_COMPACTOR_WORKER_POOL, "pool1");
    provider.createFullAcidTable(null, DEFAULT_TABLE_NAME, false, false, properties);
    provider.insertTestData(DEFAULT_TABLE_NAME, false);
    provider.createFullAcidTable(null, "table2", false, false);
    provider.insertTestData("table2", false);

    TxnCommandsBaseForTests.runInitiator(conf);

    executeStatementOnDriver("SHOW COMPACTIONS", driver);
    List results = new ArrayList();
    driver.getResults(results);
    Assert.assertEquals(3, results.size());
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time\tDuration(ms)" +
       "\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\tHighest WriteId", results.get(0));
    Pattern p = Pattern.compile("(1|2)\tdefault\t(compaction_test|table2)\t --- \tMAJOR\tinitiated.*(pool1|default).*");
    for(int i = 1; i < results.size(); i++) {
      Assert.assertTrue(p.matcher(results.get(i).toString()).matches());
    }
  }


  @Test
  public void testShowCompactionsRespectPoolName() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(Constants.HIVE_COMPACTOR_WORKER_POOL, "pool1");
    provider.createFullAcidTable(null, DEFAULT_TABLE_NAME, false, false, properties);
    provider.insertTestData(DEFAULT_TABLE_NAME, false);
    properties.put(Constants.HIVE_COMPACTOR_WORKER_POOL, "pool2");
    provider.createFullAcidTable(null, "table2", false, false, properties);
    provider.insertTestData("table2", false);

    TxnCommandsBaseForTests.runInitiator(conf);

    executeStatementOnDriver("SHOW COMPACTIONS POOL 'pool1'", driver);
    List results = new ArrayList();
    driver.getResults(results);
    Assert.assertEquals(2, results.size());
    Assert.assertEquals("CompactionId\tDatabase\tTable\tPartition\tType\tState\tWorker host\tWorker\tEnqueue Time\tStart Time\tDuration(ms)" +
       "\tHadoopJobId\tError message\tInitiator host\tInitiator\tPool name\tTxnId\tNext TxnId\tCommit Time\tHighest WriteId",
        results.get(0));
    Pattern p = Pattern.compile("1|2\tdefault\tcompaction_test\t --- \tMAJOR\tinitiated.*pool1.*");
    Assert.assertTrue(p.matcher(results.get(1).toString()).matches());
  }

}
