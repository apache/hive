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

package org.apache.hive.jdbc.miniHS2;

import org.apache.hadoop.hive.UtilsForTest;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * End to end test with MiniHS2 with ACID enabled.
 */
public class TestHiveServer2Acid {

  private static MiniHS2 miniHS2 = null;
  private static Map<String, String> confOverlay = new HashMap<>();

  @BeforeClass
  public static void beforeTest() throws Exception {
    HiveConf conf = UtilsForTest.getHiveOnTezConfFromDir("../../data/conf/tez/");
    TestTxnDbUtil.setConfValues(conf);
    TestTxnDbUtil.prepDb(conf);
    miniHS2 = new MiniHS2(conf, MiniHS2.MiniClusterType.TEZ);
    confOverlay.put(ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "false");
    miniHS2.start(confOverlay);
  }

  @AfterClass
  public static void afterTest() throws Exception {
    miniHS2.stop();
  }

  public static class SleepMsUDF extends UDF {
    public Integer evaluate(final Integer value, final Integer ms) {
      try {
        Thread.sleep(ms);
      } catch (InterruptedException e) {
        // No-op
      }
      return value;
    }
  }

  @Test
  public void testCancelOperation() throws Exception {
    String tableName = "TestHiveServer2TestConnection";
    CLIServiceClient serviceClient = miniHS2.getServiceClient();
    SessionHandle sessHandle = serviceClient.openSession("foo", "bar");
    serviceClient.executeStatement(sessHandle, "DROP TABLE IF EXISTS " + tableName, confOverlay);
    serviceClient.executeStatement(sessHandle, "CREATE TABLE " + tableName + " (id INT)", confOverlay);
    serviceClient.executeStatement(sessHandle, "insert into " + tableName + " values(5)", confOverlay);

    serviceClient.executeStatement(sessHandle,
        "create temporary function sleepMsUDF as '" + TestHiveServer2Acid.SleepMsUDF.class.getName() + "'",
        confOverlay);
    OperationHandle opHandle = serviceClient
        .executeStatementAsync(sessHandle, "select sleepMsUDF(id, 1000), id from " + tableName, confOverlay);
    serviceClient.cancelOperation(opHandle);
    
    assertOperationWasCancelled(serviceClient, opHandle);
    Assert.assertEquals(0, TestTxnDbUtil.countQueryAgent(
        miniHS2.getHiveConf(), 
        "select count(*) from HIVE_LOCKS"));
    Assert.assertEquals(1, TestTxnDbUtil.countQueryAgent(
        miniHS2.getHiveConf(), 
        "select 1 from TXNS where TXN_ID = (select max(TXN_ID) from TXNS) and TXN_STATE='a'"));
    serviceClient.closeSession(sessHandle);
  }

  /**
   * Test overlapping async queries in one session.
   * Since TxnManager is shared in the session this can cause all kind of trouble.
   * @throws Exception ex
   */
  @Test
  public void testAsyncConcurrent() throws Exception {
    String tableName = "TestHiveServer2TestConnection";
    CLIServiceClient serviceClient = miniHS2.getServiceClient();
    SessionHandle sessHandle = serviceClient.openSession("foo", "bar");
    serviceClient.executeStatement(sessHandle, "DROP TABLE IF EXISTS " + tableName, confOverlay);
    serviceClient.executeStatement(sessHandle, "CREATE TABLE " + tableName + " (id INT)", confOverlay);
    serviceClient.executeStatement(sessHandle, "insert into " + tableName + " values(5)", confOverlay);
    OperationHandle opHandle =
        serviceClient.executeStatementAsync(sessHandle, "select * from " + tableName, confOverlay);
    assertOperationFinished(serviceClient, opHandle);
    RowSet rowSet = serviceClient.fetchResults(opHandle);
    serviceClient.executeStatement(sessHandle,
        "create temporary function sleepMsUDF as '" + TestHiveServer2Acid.SleepMsUDF.class.getName() + "'",
        confOverlay);
    // Start a second "slow" query
    OperationHandle opHandle2 = serviceClient
        .executeStatementAsync(sessHandle, "select sleepMsUDF(id, 1000), id from " + tableName, confOverlay);
    // Close the first operation (this will destroy the Driver and TxnManager)
    serviceClient.closeOperation(opHandle);
    assertEquals(1, rowSet.numRows());
    assertOperationFinished(serviceClient, opHandle2);
    rowSet = serviceClient.fetchResults(opHandle2);
    assertEquals(1, rowSet.numRows());
    serviceClient.executeStatement(sessHandle, "DROP TABLE IF EXISTS " + tableName, confOverlay);
    serviceClient.closeSession(sessHandle);
  }

  private void assertOperationFinished(CLIServiceClient serviceClient, OperationHandle opHandle) 
      throws HiveSQLException, InterruptedException {
    assertOperationStatus(serviceClient, opHandle, OperationState.FINISHED);
  }

  private void assertOperationWasCancelled(CLIServiceClient serviceClient, OperationHandle opHandle)
    throws HiveSQLException, InterruptedException {
    assertOperationStatus(serviceClient, opHandle, OperationState.CANCELED);
  }
  
  private void assertOperationStatus(CLIServiceClient serviceClient, OperationHandle opHandle, OperationState opState)
      throws InterruptedException, HiveSQLException {
    OperationState pStatus = OperationState.RUNNING;
    for (int i = 0; i < 10; i++) {
      Thread.sleep(100);
      pStatus = serviceClient.getOperationStatus(opHandle, false).getState();
      if (pStatus == opState) {
        break;
      }
    }
    assertEquals(opState, pStatus);
  }
}
